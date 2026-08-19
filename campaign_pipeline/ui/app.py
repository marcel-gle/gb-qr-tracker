from __future__ import annotations

import json
import sys
from pathlib import Path

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from campaign_pipeline.config import CampaignConfig, ScoreConfig
from campaign_pipeline.integrations import run_firestore_upload, run_pdf_generation
from campaign_pipeline.naming import review_decisions_path, review_issues_path, stage_path
from campaign_pipeline.pipeline import CampaignPipeline
from campaign_pipeline.ui.drive_helpers import (
    init_drive_session,
    list_campaign_dirs,
    maybe_push_active_campaign,
    pull_active_campaign,
    render_drive_connection_panel,
    use_google_drive,
)
from campaign_pipeline.ui.runner import run_cmd_streaming

DEFAULT_CAMPAIGN_BASE = str(Path.home() / "Desktop" / "Briefversand")


def _format_duration(seconds: float) -> str:
    if seconds < 0:
        return "—"
    total = int(seconds)
    if total < 60:
        return f"{total}s"
    minutes, secs = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m"


STEPS = [
    "1. Campaign setup",
    "2. Merge & dedupe raw",
    "3. Scoring",
    "4. Imprint scrape",
    "5. Address dedupe",
    "6. Template column",
    "7. Final review",
    "8. Firestore upload",
    "9. Generate PDFs",
]


def _prompts_path() -> Path:
    return REPO_ROOT / "scripts" / "business" / "prompts.json"


def load_prompt_names() -> list[str]:
    path = _prompts_path()
    if not path.exists():
        return ["handwerk_analysis"]
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return sorted(p["name"] for p in data.get("prompts", []) if p.get("name"))


def load_prompt_score_config(name: str) -> ScoreConfig:
    path = _prompts_path()
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    for p in data.get("prompts", []):
        if p.get("name") == name:
            return ScoreConfig.from_prompt_data(p)
    return ScoreConfig()


def load_prompt_metadata(name: str) -> dict:
    path = _prompts_path()
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    for p in data.get("prompts", []):
        if p.get("name") == name:
            return {
                "content_extraction": p.get("content_extraction") or {},
                "pass_rules": p.get("pass_rules") or {},
            }
    return {"content_extraction": {}, "pass_rules": {}}


def load_config_options() -> list[tuple[str, Path]]:
    config_dir = REPO_ROOT / "scripts" / "send_letter" / "configs"
    if not config_dir.exists():
        return []
    return sorted((p.name, p) for p in config_dir.glob("*.json"))


def init_session() -> None:
    defaults = {
        "campaign_base_parent": DEFAULT_CAMPAIGN_BASE,
        "active_campaign_dir": "",
        "active_base_name": "",
        "active_target_final_count": 0,
        "setup_campaign_dir": "",
        "setup_base_name": "",
        "setup_target_final_count": 0,
        "current_step": STEPS[0],
        "use_google_drive": False,
        "drive_root_id": "",
        "drive_root_name": "",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v
    init_drive_session()


def _set_campaign_context(campaign_dir: str, base_name: str) -> None:
    """Persist campaign paths in keys that are not tied to step-1-only widgets."""
    campaign_dir = campaign_dir.strip()
    base_name = base_name.strip()
    st.session_state["active_campaign_dir"] = campaign_dir
    st.session_state["active_base_name"] = base_name
    st.session_state["setup_campaign_dir"] = campaign_dir
    st.session_state["setup_base_name"] = base_name


def _sync_campaign_context_from_setup() -> bool:
    """Copy step-1 form values into persistent active_* keys."""
    campaign_dir = str(st.session_state.get("setup_campaign_dir", "") or "").strip()
    base_name = str(st.session_state.get("setup_base_name", "") or "").strip()
    if not campaign_dir or not base_name:
        return False
    target = int(st.session_state.get("setup_target_final_count") or 0)
    st.session_state["active_campaign_dir"] = campaign_dir
    st.session_state["active_base_name"] = base_name
    st.session_state["active_target_final_count"] = target
    return True


def _pipeline() -> CampaignPipeline | None:
    campaign_dir = str(st.session_state.get("active_campaign_dir", "") or "").strip()
    base_name = str(st.session_state.get("active_base_name", "") or "").strip()
    if not campaign_dir or not base_name:
        return None
    score_cfg = load_prompt_score_config(st.session_state.get("scoring_prompt", "handwerk_analysis"))
    if st.session_state.get("pass_threshold"):
        score_cfg.pass_threshold = float(st.session_state["pass_threshold"])
    target = int(st.session_state.get("active_target_final_count") or 0) or None
    cfg = CampaignConfig(
        campaign_dir=Path(campaign_dir),
        base_name=base_name,
        backend=st.session_state.get("backend", "local"),
        scoring_prompt_name=st.session_state.get("scoring_prompt", "handwerk_analysis"),
        score_config=score_cfg,
        max_workers_http=int(st.session_state.get("max_workers_http", 10)),
        max_workers_llm=int(st.session_state.get("max_workers_llm", 5)),
        target_final_count=target,
        enable_northdata_fallback=bool(st.session_state.get("imprint_enable_northdata", True)),
    )
    return CampaignPipeline(cfg)


def _render_active_campaign_banner() -> None:
    campaign_dir = str(st.session_state.get("active_campaign_dir", "") or "").strip()
    base_name = str(st.session_state.get("active_base_name", "") or "").strip()
    if campaign_dir and base_name:
        st.caption(f"Active campaign: **{base_name}** — `{campaign_dir}`")


def render_setup() -> None:
    st.subheader("Campaign setup")

    storage_mode = st.radio(
        "Storage",
        ["Google Drive", "Local folder"],
        index=0 if use_google_drive() else 1,
        horizontal=True,
        key="storage_mode_radio",
    )
    st.session_state["use_google_drive"] = storage_mode == "Google Drive"
    from campaign_pipeline.drive.settings import save_drive_settings

    save_drive_settings(use_google_drive=st.session_state["use_google_drive"])

    if use_google_drive():
        render_drive_connection_panel()
        if not st.session_state.get("drive_root_id"):
            st.info("Connect Google Drive and save your Briefversand folder ID to continue.")
            return
        base_parent = Path(str(st.session_state["campaign_base_parent"]))
    else:
        base_parent_str = st.text_input(
            "Base path",
            value=st.session_state.get("campaign_base_parent", DEFAULT_CAMPAIGN_BASE),
        )
        st.session_state["campaign_base_parent"] = base_parent_str.rstrip("/")
        base_parent = Path(st.session_state["campaign_base_parent"])

    st.markdown("#### Campaign folder")
    mode = st.radio("Mode", ["Create new", "Use existing"])
    existing = list_campaign_dirs()

    if mode == "Create new":
        name = st.text_input("Campaign folder name", placeholder="003-20260304-my-campaign")
        if st.button("Create campaign folder"):
            if base_parent and name:
                campaign_name = name.strip()
                if use_google_drive():
                    from campaign_pipeline.ui.drive_helpers import get_drive_sync

                    sync = get_drive_sync()
                    if not sync:
                        st.error("Connect Google Drive first.")
                    else:
                        campaign_path = sync.ensure_campaign_on_drive(campaign_name)
                        pipe = CampaignPipeline(
                            CampaignConfig(campaign_dir=campaign_path, base_name=campaign_name)
                        )
                        pipe.ensure_campaign_dirs()
                        _set_campaign_context(str(campaign_path), campaign_name)
                        maybe_push_active_campaign(show_message=True)
                        st.success(f"Created {campaign_path}")
                else:
                    campaign_path = base_parent / campaign_name
                    pipe = CampaignPipeline(
                        CampaignConfig(campaign_dir=campaign_path, base_name=campaign_name)
                    )
                    pipe.ensure_campaign_dirs()
                    _set_campaign_context(str(campaign_path), campaign_name)
                    st.success(f"Created {campaign_path}")
    else:
        if existing:
            selected = st.selectbox("Existing folder", existing)
            if st.button("Use folder"):
                if use_google_drive():
                    campaign_path = base_parent / selected
                    _set_campaign_context(str(campaign_path), selected)
                    pull_active_campaign(show_message=True)
                else:
                    _set_campaign_context(str(base_parent / selected), selected)
                st.success(f"Using {selected}")
        else:
            st.caption("No campaign folders yet. Create one or pull from Google Drive.")

    st.text_input("Campaign directory", key="setup_campaign_dir")
    st.text_input("Base name (file prefix)", key="setup_base_name")
    st.number_input(
        "Target final count (0 = no target)",
        min_value=0,
        step=1,
        key="setup_target_final_count",
    )

    if st.button("Save campaign settings"):
        if _sync_campaign_context_from_setup():
            if use_google_drive():
                pull_active_campaign(show_message=False)
            pipe = CampaignPipeline(
                CampaignConfig(
                    campaign_dir=Path(st.session_state["active_campaign_dir"]),
                    base_name=st.session_state["active_base_name"],
                    target_final_count=int(st.session_state.get("active_target_final_count") or 0) or None,
                )
            )
            pipe.ensure_campaign_dirs()
            maybe_push_active_campaign(show_message=use_google_drive())
            st.success("Campaign settings saved.")
        else:
            st.error("Set both campaign directory and base name.")

    pipe = _pipeline()
    if pipe:
        pipe.ensure_campaign_dirs()
        status = pipe.funnel_status()
        st.json(status)


def render_merge() -> None:
    st.subheader("Merge & dedupe raw")
    _render_active_campaign_banner()
    st.markdown(
        "Place source CSVs in `lists/incoming/`, select files, merge to `{base}_raw.csv`, "
        "then dedupe by domain — the merged file is renamed to `{base}_raw_deduped.csv`."
    )
    pipe = _pipeline()
    if not pipe:
        st.warning("Configure campaign in step 1.")
        return

    incoming = pipe.list_incoming_csvs()
    selected = st.multiselect("Incoming CSV files", [p.name for p in incoming], default=[p.name for p in incoming])
    append_new = st.checkbox("Append only new domains (top-up)", value=False)

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Merge raw lists", key="merge_raw_btn"):
            files = [pipe.config.incoming_dir() / n for n in selected]
            stats = pipe.merge_raw(files, append_only_new=append_new)
            maybe_push_active_campaign()
            st.success("Merged")
            st.json(stats)
    with col2:
        if st.button("Dedupe by domain", key="dedupe_domain_btn"):
            try:
                stats = pipe.dedupe_domain()
                output = Path(stats["output_path"])
                if output.is_file():
                    maybe_push_active_campaign()
                    st.success(f"Deduped → `{output}`")
                    st.json(stats)
                else:
                    raw_path = stage_path(pipe.config.campaign_dir, pipe.config.base_name, "raw")
                    st.error(
                        f"Dedupe did not create `{output.name}`. "
                        f"Still present: `{raw_path.name}`" if raw_path.is_file()
                        else f"Dedupe did not create `{output.name}` in `{output.parent}`."
                    )
                    st.caption(
                        "If this keeps happening, stop Streamlit (Ctrl+C) and restart with "
                        "`streamlit run campaign_pipeline/ui/app.py` to reload the pipeline code."
                    )
                    st.json(stats)
            except FileNotFoundError as exc:
                st.error(str(exc))
            except Exception as exc:
                st.error(f"Dedupe failed: {exc}")

    lists_dir = pipe.config.lists_dir()
    csv_files = sorted(p.name for p in lists_dir.glob("*.csv")) if lists_dir.is_dir() else []
    if csv_files:
        st.caption(f"CSV files in `lists/`: {', '.join(csv_files)}")


def _run_imprint_with_progress(
    pipe: CampaignPipeline,
    *,
    only_new: bool,
    skip_score_filter: bool,
    only_missing_fields: bool,
) -> dict:
    progress_bar = st.progress(0.0)
    status = st.empty()

    def on_imprint_progress(done: int, total: int, elapsed: float, domain: str) -> None:
        if total == 0:
            status.caption("No domains to scrape.")
            return
        frac = done / total
        progress_bar.progress(min(frac, 1.0))
        eta = (elapsed / done) * (total - done) if done else 0.0
        status.markdown(
            f"Scraped **{done}/{total}** ({frac * 100:.0f}%) · "
            f"elapsed **{_format_duration(elapsed)}** · "
            f"ETA **~{_format_duration(eta)}** · last `{domain}`"
        )

    stats = pipe.imprint(
        only_new=only_new,
        skip_score_filter=skip_score_filter,
        only_missing_fields=only_missing_fields,
        progress_callback=on_imprint_progress,
    )
    progress_bar.progress(1.0)
    if stats.get("scraped", 0) == 0:
        status.caption("No domains to scrape.")
    else:
        status.markdown(
            f"Finished imprint scrape for **{stats.get('scraped', 0)}** domains · "
            f"**{stats.get('output', 0)}** rows in output"
        )
    return stats


def render_scoring() -> None:
    st.subheader("Scoring")
    pipe = _pipeline()
    if not pipe:
        st.warning("Configure campaign in step 1.")
        return

    prompts = load_prompt_names()
    prompt = st.selectbox("Scoring prompt", prompts, key="scoring_prompt")
    score_cfg = load_prompt_score_config(prompt)
    prompt_meta = load_prompt_metadata(prompt)
    extraction_mode = prompt_meta.get("content_extraction", {}).get("mode", "text_only")
    browser_fallback = bool(prompt_meta.get("content_extraction", {}).get("browser_fallback", False))
    if extraction_mode == "text_and_technical":
        fallback_note = " (mit Browser-Fallback)" if browser_fallback else ""
        st.caption(f"Extraktion: sichtbarer Text + technische HTML-Signale{fallback_note}")
    else:
        st.caption("Extraktion: nur sichtbarer Webseiten-Text")
    require_boolean = prompt_meta.get("pass_rules", {}).get("require_boolean")
    if require_boolean:
        st.caption(
            f"Pass-Filter: score ≥ Schwellwert und {require_boolean}"
        )
    st.caption(f"Scale: **{score_cfg.scale}** | default threshold: **{score_cfg.pass_threshold}**")
    st.number_input("Pass threshold", value=float(score_cfg.pass_threshold), step=0.5, key="pass_threshold")
    st.selectbox("Backend", ["local", "openai"], key="backend")
    st.number_input("Max workers HTTP", min_value=1, max_value=50, value=10, key="max_workers_http")
    st.number_input("Max workers LLM", min_value=1, max_value=20, value=5, key="max_workers_llm")
    only_missing = st.checkbox(
        "Score only missing results",
        value=False,
        key="score_only_missing",
        help=(
            "Load the existing `_scored.csv`, score rows with empty `domain_analysis_raw` / "
            "`match_score` only, and keep rows that already have results."
        ),
    )
    only_new = st.checkbox(
        "Score only new domains",
        value=False,
        disabled=only_missing,
        help="Uses the pipeline registry (raw stage). Ignored when scoring only missing results.",
    )
    auto_imprint = st.checkbox(
        "Automatically continue to imprint scrape",
        value=False,
        key="score_auto_imprint",
        help=(
            "After scoring finishes, scrape Impressum pages for rows that passed the score filter. "
            "Uses the imprint-step options (skip score filter, North Data) if you already set them."
        ),
    )

    if st.button("Run scoring"):
        pipe = _pipeline()
        if pipe:
            progress_bar = st.progress(0.0)
            status = st.empty()

            def on_scoring_progress(done: int, total: int, elapsed: float, domain: str) -> None:
                if total == 0:
                    status.caption("No domains to score.")
                    return
                frac = done / total
                progress_bar.progress(min(frac, 1.0))
                eta = (elapsed / done) * (total - done) if done else 0.0
                status.markdown(
                    f"Scored **{done}/{total}** ({frac * 100:.0f}%) · "
                    f"elapsed **{_format_duration(elapsed)}** · "
                    f"ETA **~{_format_duration(eta)}** · last `{domain}`"
                )

            stats = pipe.score(
                only_new=only_new,
                only_missing=only_missing,
                progress_callback=on_scoring_progress,
            )
            progress_bar.progress(1.0)
            if stats.get("scored", 0) == 0:
                status.caption("No domains to score.")
            elif only_missing:
                status.markdown(
                    f"Scored **{stats.get('scored', 0)}** missing row(s) · "
                    f"missing **{stats.get('missing_before', 0)}** → **{stats.get('missing_after', 0)}** · "
                    f"**{stats.get('passed', 0)}** passed filter"
                )
            else:
                status.markdown(
                    f"Finished scoring **{stats.get('scored', 0)}** domains · "
                    f"**{stats.get('passed', 0)}** passed filter"
                )

            imprint_stats = None
            if auto_imprint:
                st.markdown("#### Imprint scrape")
                imprint_stats = _run_imprint_with_progress(
                    pipe,
                    only_new=only_new,
                    skip_score_filter=bool(
                        st.session_state.get("imprint_skip_score_filter", False)
                    ),
                    only_missing_fields=False,
                )

            maybe_push_active_campaign()
            st.success("Scoring and imprint scrape done" if auto_imprint else "Done")
            if imprint_stats is not None:
                st.json({"scoring": stats, "imprint": imprint_stats})
            else:
                st.json(stats)


def _resolve_imprint_input_path(config: CampaignConfig) -> Path:
    """Prefer scored; fall back to raw_deduped / raw when scoring was skipped."""
    scored = stage_path(config.campaign_dir, config.base_name, "scored")
    if scored.exists():
        return scored
    deduped = stage_path(config.campaign_dir, config.base_name, "raw_deduped")
    if deduped.exists():
        return deduped
    return stage_path(config.campaign_dir, config.base_name, "raw")


def render_imprint() -> None:
    st.subheader("Imprint scrape")
    pipe = _pipeline()
    if not pipe:
        st.warning("Configure campaign in step 1.")
        return

    input_path = _resolve_imprint_input_path(pipe.config)
    scored_path = stage_path(pipe.config.campaign_dir, pipe.config.base_name, "scored")
    using_scored = input_path.exists() and scored_path.exists() and (
        input_path.resolve() == scored_path.resolve()
    )
    if not input_path.exists():
        st.warning(
            f"No imprint input found yet (`{scored_path.name}`, "
            f"`{pipe.config.base_name}_raw_deduped.csv`, or `{pipe.config.base_name}_raw.csv`)."
        )
    elif using_scored:
        st.caption(f"Input: `{input_path.name}` (scored). Scoring pass-filter applies unless disabled below.")
    else:
        st.info(
            f"No `_scored.csv` — will use `{input_path.name}` and scrape all rows "
            "(scoring step can be skipped)."
        )

    only_new = st.checkbox("Scrape only new domains", value=False, key="imprint_only_new")
    skip_score_filter = st.checkbox(
        "Ignore score pass filter (scrape all rows)",
        value=not using_scored,
        key="imprint_skip_score_filter",
        help=(
            "When unchecked and `_scored.csv` exists, only rows with passed_score_filter=True are scraped. "
            "Enable this to scrape every row, or skip scoring entirely."
        ),
    )
    st.checkbox(
        "North Data fallback for missing directors",
        value=True,
        key="imprint_enable_northdata",
        help=(
            "When the imprint has an address but no managing director, look up the "
            "current legal representative(s) on North Data."
        ),
    )
    imprint_path = stage_path(pipe.config.campaign_dir, pipe.config.base_name, "imprint")
    only_missing_fields = st.checkbox(
        "Re-scrape only rows missing street/name",
        value=False,
        key="imprint_only_missing_fields",
        disabled=not imprint_path.exists(),
        help=(
            "Re-runs only the rows in the existing `_imprint.csv` that still lack a "
            "street or a managing-director name. Forces a fresh fetch (ignores the "
            "cache) so improved scraping/North Data can fill the gaps. Other rows "
            "are left untouched."
        ),
    )
    if only_missing_fields and imprint_path.exists():
        try:
            from campaign_pipeline.io.readers import load_rows_as_business
            from campaign_pipeline.steps.imprint_scrape import row_is_missing_core_fields

            _existing = load_rows_as_business(imprint_path, max_directors=pipe.config.max_directors)
            _missing = sum(1 for r in _existing if row_is_missing_core_fields(r))
            st.caption(f"{_missing} of {len(_existing)} rows are missing street/name.")
        except Exception:
            pass
    if st.button("Run imprint scrape"):
        stats = _run_imprint_with_progress(
            pipe,
            only_new=only_new,
            skip_score_filter=skip_score_filter,
            only_missing_fields=only_missing_fields,
        )
        maybe_push_active_campaign()
        st.success("Done")
        st.json(stats)


def render_address_dedupe() -> None:
    st.subheader("Address dedupe")
    pipe = _pipeline()
    if not pipe:
        st.warning("Configure campaign in step 1.")
        return
    if st.button("Dedupe by address"):
        stats = pipe.dedupe_address()
        maybe_push_active_campaign()
        st.success("Done")
        st.json(stats)


def render_template() -> None:
    st.subheader("Template column")
    pipe = _pipeline()
    if not pipe:
        st.warning("Configure campaign in step 1.")
        return

    tpl_dir = pipe.config.templates_dir()
    st.text_input("Templates directory", value=str(tpl_dir), disabled=True)
    pdfs = sorted(p.name for p in tpl_dir.glob("*.pdf")) if tpl_dir.exists() else []
    mode = st.radio("Assignment", ["Single template", "Split by count"])
    single = None
    split_specs: list[tuple[str, int | None]] = []
    if mode == "Single template" and pdfs:
        single = st.selectbox("Template", pdfs)
    elif pdfs:
        n = st.number_input("Segments", min_value=1, max_value=10, value=1)
        for i in range(int(n)):
            t = st.selectbox(f"Template {i+1}", pdfs, key=f"split_t_{i}")
            rest = st.checkbox("Rest", value=(i == int(n) - 1), key=f"split_rest_{i}")
            count = None if rest else st.number_input(f"Rows seg {i+1}", min_value=1, value=100, key=f"split_n_{i}")
            split_specs.append((t, count))

    if st.button("Add template column"):
        stats = pipe.add_template(
            tpl_dir,
            single_template=single if mode == "Single template" else None,
            split_specs=split_specs if mode == "Split by count" else None,
        )
        maybe_push_active_campaign()
        st.success("Done")
        st.json(stats)


def render_final() -> None:
    st.subheader("Final review")
    pipe = _pipeline()
    if not pipe:
        st.warning("Configure campaign in step 1.")
        return

    default_min = float(pipe.config.score_config.pass_threshold)
    min_score = st.number_input("Min score", value=default_min, step=0.5)
    skip_score_filter = st.checkbox(
        "Skip min-score filter",
        value=False,
        key="final_skip_score_filter",
        help="Keep rows even when match_score is missing or below the threshold (e.g. after skipping scoring).",
    )
    issues_path = review_issues_path(pipe.config.campaign_dir, pipe.config.base_name)
    decisions_path = review_decisions_path(pipe.config.campaign_dir, pipe.config.base_name)

    st.markdown("#### 1. LLM quality check")
    st.caption(
        "Flags suspicious rows (non-German address, invalid address, odd data). "
        f"Issues file: `{issues_path.name}`"
    )
    if st.button("Run LLM quality check", key="run_llm_quality_check"):
        progress_bar = st.progress(0.0)
        status = st.empty()

        def on_llm_progress(done: int, total: int, _elapsed: float) -> None:
            if total == 0:
                status.caption("No rows to check.")
                return
            frac = done / total
            progress_bar.progress(min(frac, 1.0))
            status.markdown(f"LLM review chunk **{done}/{total}** ({frac * 100:.0f}%)")

        stats = pipe.run_llm_quality_check(progress_callback=on_llm_progress)
        progress_bar.progress(1.0)
        status.markdown(
            f"Flagged **{stats.get('flagged_rows', 0)}** of **{stats.get('records_total', 0)}** rows for manual review."
        )
        maybe_push_active_campaign()
        st.success("LLM quality check complete")
        st.json(stats)

    issues = pipe.get_review_issues()
    decisions = pipe.get_review_decisions()

    st.markdown("#### 2. Manual review of flagged rows")
    if not issues:
        st.info("No flagged rows yet. Run the LLM quality check first.")
    else:
        st.caption(f"{len(issues)} flagged row(s). Default action is **Discard** unless you choose Keep.")
        new_decisions: dict[int, str] = dict(decisions)
        for issue in issues:
            raw_idx = (issue.get("row_index") or "").strip()
            if not raw_idx.isdigit():
                continue
            idx = int(raw_idx)
            default_choice = decisions.get(idx, "discard")
            with st.expander(
                f"Row {idx}: {issue.get('domain') or issue.get('company_name') or '?'} — "
                f"{issue.get('issue') or 'flagged'} ({issue.get('severity') or '?'})",
                expanded=(default_choice == "discard"),
            ):
                st.write(f"**Address:** {issue.get('address') or '—'}")
                if issue.get("details"):
                    st.write(f"**Details:** {issue.get('details')}")
                choice = st.radio(
                    "Decision",
                    ["Keep", "Discard"],
                    index=0 if default_choice == "keep" else 1,
                    key=f"review_decision_{idx}",
                    horizontal=True,
                )
                new_decisions[idx] = "keep" if choice == "Keep" else "discard"

        if st.button("Save review decisions", key="save_review_decisions"):
            pipe.save_review_decisions(new_decisions)
            maybe_push_active_campaign()
            st.success(f"Saved decisions to `{decisions_path}`")

    st.markdown("#### 3. Build final CSV")
    st.caption(
        "Applies min score (unless skipped), required fields, PLZ validation, and removes flagged rows you marked Discard."
    )
    if st.button("Build final CSV", key="build_final_csv"):
        stats = pipe.final_review(min_score=min_score, skip_score_filter=skip_score_filter)
        maybe_push_active_campaign()
        st.success("Done")
        st.json(stats)
        final_path = stage_path(pipe.config.campaign_dir, pipe.config.base_name, "final")
        st.info(f"Output: `{final_path}`")


def render_upload() -> None:
    st.subheader("Firestore upload")
    pipe = _pipeline()
    if not pipe:
        st.warning("Configure campaign in step 1.")
        return

    final_path = stage_path(pipe.config.campaign_dir, pipe.config.base_name, "final")
    env = st.selectbox("Environment", ["dev", "prod"])
    owner_id = st.text_input("Owner ID")
    campaign_code = st.text_input("Campaign code")
    campaign_name = st.text_input("Campaign name")
    campaign_id = st.text_input("Campaign ID", value=pipe.config.base_name)
    destination = st.text_input("Destination URL")
    do_upload = st.checkbox("Upload to GCS", value=False)

    if st.button("Prepare / upload"):
        if not (campaign_name or "").strip():
            st.error("Campaign name is required.")
            return
        if not all([(owner_id or "").strip(), (campaign_code or "").strip(), (destination or "").strip()]):
            st.error("Owner ID, campaign code, and destination URL are required.")
            return
        code, out, err = run_firestore_upload(
            env=env,
            owner_id=owner_id.strip(),
            campaign_code=campaign_code.strip(),
            input_csv=final_path,
            templates_dir=pipe.config.templates_dir(),
            destination=destination.strip(),
            campaign_name=campaign_name.strip(),
            campaign_id=campaign_id,
            upload=do_upload,
        )
        st.code(out + err)
        if code != 0:
            st.error("Failed")
        else:
            st.success("Done")
    st.info("Next: download with_links CSV manually, then step 9 for PDFs.")


def render_pdf() -> None:
    st.subheader("Generate PDFs (no API upload)")
    pipe = _pipeline()
    if not pipe:
        st.warning("Configure campaign in step 1.")
        return

    contacts = st.text_input("Contacts CSV (with_links)")
    configs = load_config_options()
    config_name = st.selectbox("Letter config", [c[0] for c in configs] if configs else ["(none)"])
    config_path = next((p for n, p in configs if n == config_name), None)
    save_dir = st.text_input("Save PDFs directory", value=str(pipe.config.pdf_output_dir()))
    limit = st.number_input("Limit (0 = all)", min_value=0, value=0)

    if st.button("Generate PDFs"):
        if not contacts or not config_path:
            st.error("Set contacts CSV and config.")
        else:
            code, out, err = run_pdf_generation(
                contacts_csv=Path(contacts),
                templates_dir=pipe.config.templates_dir(),
                config_path=config_path,
                save_pdfs_dir=Path(save_dir),
                campaign_id=pipe.config.base_name,
                limit=int(limit) if limit else None,
            )
            st.code(out + err)
            st.success("Done" if code == 0 else "Failed")


def render_funnel_panel(pipe: CampaignPipeline) -> None:
    st.sidebar.markdown("---")
    st.sidebar.subheader("Funnel status")
    status = pipe.funnel_status()
    counts = status.get("counts", {})
    st.sidebar.write(f"raw: {counts.get('raw', 0)}")
    st.sidebar.write(f"scored: {counts.get('scored', 0)}")
    st.sidebar.write(f"imprint: {counts.get('imprint', 0)}")
    st.sidebar.write(f"final: {counts.get('final', 0)}")
    target = pipe.config.target_final_count
    if target:
        final_n = counts.get("final", 0)
        if final_n < target:
            st.sidebar.warning(f"Below target ({final_n}/{target})")
        else:
            st.sidebar.success(f"Target met ({final_n}/{target})")
    if st.sidebar.button("Continue pipeline (new domains)"):
        with st.spinner("Running..."):
            results = pipe.continue_pipeline()
        maybe_push_active_campaign()
        st.sidebar.json(results)


def render_drive_sidebar() -> None:
    if not use_google_drive():
        return
    st.sidebar.markdown("---")
    st.sidebar.subheader("Google Drive")
    if st.session_state.get("drive_root_name"):
        st.sidebar.caption(f"Root: **{st.session_state['drive_root_name']}**")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("Pull", key="sidebar_drive_pull"):
            pull_active_campaign(show_message=True)
    with col2:
        if st.button("Push", key="sidebar_drive_push"):
            maybe_push_active_campaign(show_message=True)


def main() -> None:
    st.set_page_config(page_title="Campaign Pipeline", layout="wide")
    init_session()
    st.sidebar.title("Campaign Pipeline")
    st.sidebar.warning("Dev mode: GCS upload off by default. PDF step never calls onlinebrief24 API.")

    step = st.sidebar.radio("Step", STEPS, key="current_step")
    pipe = _pipeline()
    if pipe:
        st.sidebar.caption(f"**{pipe.config.base_name}**")
        render_funnel_panel(pipe)
    render_drive_sidebar()

    if step == STEPS[0]:
        render_setup()
    elif step == STEPS[1]:
        render_merge()
    elif step == STEPS[2]:
        render_scoring()
    elif step == STEPS[3]:
        render_imprint()
    elif step == STEPS[4]:
        render_address_dedupe()
    elif step == STEPS[5]:
        render_template()
    elif step == STEPS[6]:
        render_final()
    elif step == STEPS[7]:
        render_upload()
    else:
        render_pdf()


if __name__ == "__main__":
    main()
