"""
Streamlit UI for the campaign workflow: list enrichment, filtering,
template column, Firestore upload, and PDF generation/sending.

Run from repo root: streamlit run scripts/streamlit_app.py
"""

from __future__ import annotations

import csv
import io
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Tuple

import streamlit as st

# Encodings to try when reading CSV files (e.g. German exports often use cp1252/latin-1)
CSV_READ_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")

# Repo root (parent of scripts/)
REPO_ROOT = Path(__file__).resolve().parent.parent

# Default parent path where campaign folders live (editable in Step 1)
DEFAULT_CAMPAIGN_BASE_PARENT = "/Users/marcelgleich/Desktop/Briefversand/"

# Subfolders created inside each campaign folder
CAMPAIGN_SUBFOLDERS = ("lists", "templates", "pdf_output")

STEPS = [
    "Step 1: Campaign folder",
    "Step 2: List enrichment",
    "Step 3.1: Filter output",
    "Step 3.2: Template column",
    "Step 4: Firestore upload",
    "Step 5: Download with_links",
    "Step 6: PDF / send letters",
]


def _prompts_path() -> Path:
    return REPO_ROOT / "scripts" / "business" / "prompts.json"


def _configs_dir() -> Path:
    return REPO_ROOT / "scripts" / "send_letter" / "configs"


def _presets_path() -> Path:
    return REPO_ROOT / "scripts" / "streamlit_presets.json"


def load_prompt_names() -> list[str]:
    path = _prompts_path()
    if not path.exists():
        return ["handwerk_analysis"]
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        prompts = data.get("prompts", [])
        return sorted([p["name"] for p in prompts if isinstance(p.get("name"), str)])
    except Exception:
        return ["handwerk_analysis"]


def load_config_options() -> list[tuple[str, Path]]:
    config_dir = _configs_dir()
    if not config_dir.exists():
        return []
    out = []
    for p in sorted(config_dir.glob("*.json")):
        out.append((p.name, p))
    return out


def run_cmd(cmd: list[str], cwd: Path) -> tuple[int, str, str]:
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout or "", result.stderr or ""


# Height in pixels for scrollable pipeline output
PIPELINE_OUTPUT_HEIGHT = 400


def run_cmd_streaming(
    cmd: list[str],
    cwd: Path,
    stream_placeholder: st.delta_generator.DeltaGenerator | None = None,
    output_height: int = PIPELINE_OUTPUT_HEIGHT,
) -> tuple[int, str, str]:
    """
    Run command and optionally stream stdout/stderr to a Streamlit placeholder in real time.
    Uses a fixed-height scrollable text area when stream_placeholder is set.
    Returns (returncode, full_stdout, full_stderr). When streaming, stderr is merged into stdout.
    """
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    process = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )
    accumulated: List[str] = []
    if process.stdout is None:
        process.wait()
        return process.returncode or 0, "", ""

    for line in iter(process.stdout.readline, ""):
        accumulated.append(line)
        if stream_placeholder is not None:
            stream_placeholder.text_area(
                "Pipeline output (live)",
                value="".join(accumulated),
                height=output_height,
                disabled=True,
                label_visibility="collapsed",
            )

    process.wait()
    full = "".join(accumulated)
    return process.returncode or 0, full, ""


def init_session_state() -> None:
    defaults = {
        "campaign_base_path": "",
        "campaign_base_parent": DEFAULT_CAMPAIGN_BASE_PARENT,
        "path_to_gesamt_csv": "",
        "step5_output_csv": "",
        "step51_output_csv": "",
        "step52_output_csv": "",
        "presets": {},
        "selected_preset_name": "",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def load_presets() -> dict:
    path = _presets_path()
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_presets(presets: dict) -> None:
    path = _presets_path()
    with path.open("w", encoding="utf-8") as f:
        json.dump(presets, f, indent=2)


def _detect_delimiter(sample: str) -> str:
    """Detect CSV delimiter from first non-empty line."""
    for line in sample.splitlines():
        if line.strip():
            return ";" if line.count(";") >= line.count(",") else ","
    return ";"


def _read_csv_file_content(file_path: Path) -> str:
    """Read file content trying common encodings (UTF-8, cp1252, latin-1). Raises ValueError if all fail."""
    last_error: Exception | None = None
    for enc in CSV_READ_ENCODINGS:
        try:
            return file_path.read_text(encoding=enc)
        except (UnicodeDecodeError, LookupError) as e:
            last_error = e
            continue
    raise ValueError(f"Could not decode {file_path.name} (tried {', '.join(CSV_READ_ENCODINGS)}): {last_error}")


def _create_limited_csv(source_path: Path, max_rows: int) -> Path:
    """
    Create a temporary CSV with header + first max_rows data rows from source_path.
    Uses encoding-safe read. Caller must unlink the returned path when done.
    """
    content = _read_csv_file_content(source_path)
    delim = _detect_delimiter(content)
    reader = csv.DictReader(io.StringIO(content), delimiter=delim, restkey="_extra", restval="")
    fieldnames = list(reader.fieldnames or [])
    rows: List[dict] = []
    for i, row in enumerate(reader):
        if i >= max_rows:
            break
        row.pop("_extra", None)
        rows.append(row)
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8", newline="")
    try:
        writer = csv.DictWriter(tmp, fieldnames=fieldnames, delimiter=delim, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    finally:
        tmp.close()
    return Path(tmp.name)


def combine_lists_to_gesamt(
    lists_dir: Path,
    output_path: Path,
    exclude_gesamt: bool = True,
    selected_filenames: List[str] | None = None,
) -> Tuple[int, str]:
    """
    Combine CSV files in lists_dir into one -gesamt CSV.
    If selected_filenames is given, only those files (by name) are combined;
    otherwise all *.csv are used, excluding the output and -gesamt files when exclude_gesamt=True.
    Headers are unified: first file defines order, then any extra columns from
    other files are appended. Header row is written once; no duplicates.
    Returns (total_rows, message).
    """
    if selected_filenames is not None:
        csv_files = [lists_dir / n for n in selected_filenames if (lists_dir / n).exists() and (lists_dir / n).suffix.lower() == ".csv"]
        csv_files = sorted(csv_files, key=lambda p: p.name)
    else:
        csv_files = sorted(lists_dir.glob("*.csv"))
        if exclude_gesamt:
            output_name = output_path.name.lower()
            csv_files = [p for p in csv_files if p.name.lower() != output_name and "-gesamt" not in p.name.lower()]
    if not csv_files:
        raise ValueError(f"No CSV files to combine in {lists_dir}.")

    all_rows: List[dict] = []
    header_order: List[str] = []
    seen_headers: set[str] = set()

    for fp in csv_files:
        content = _read_csv_file_content(fp)
        delim = _detect_delimiter(content)
        reader = csv.DictReader(io.StringIO(content), delimiter=delim, restkey="_extra", restval="")
        fieldnames = list(reader.fieldnames or [])
        for h in fieldnames:
            if h != "_extra" and h not in seen_headers:
                header_order.append(h)
                seen_headers.add(h)
        for row in reader:
            row.pop("_extra", None)
            all_rows.append(row)

    if not header_order:
        raise ValueError("No headers found in any CSV")

    # Ensure every row has every key (fill missing with "")
    for row in all_rows:
        for h in header_order:
            row.setdefault(h, "")

    # Replace column "Name" with "company_name" (first column often named "Name" in source lists)
    for row in all_rows:
        for k in list(row.keys()):
            if k.strip().lower() == "name":
                row["company_name"] = row.get(k, row.get("company_name", ""))
                del row[k]
                break
    header_order = ["company_name" if h.strip().lower() == "name" else h for h in header_order]
    # Dedupe: keep first "company_name" only
    seen_cn = False
    new_order: List[str] = []
    for h in header_order:
        if h == "company_name":
            if not seen_cn:
                new_order.append(h)
                seen_cn = True
        else:
            new_order.append(h)
    header_order = new_order
    for row in all_rows:
        for h in header_order:
            row.setdefault(h, "")

    delim_out = ";" if output_path.suffix.lower() == ".csv" else ","
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header_order, delimiter=delim_out, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    return len(all_rows), f"Combined {len(csv_files)} file(s) into {output_path} ({len(all_rows)} rows)."


def render_step1() -> None:
    st.subheader("Step 1: Campaign folder")
    st.markdown(
        "Define the base path where campaign folders live, then either **create a new** campaign folder or **select an existing** one to continue with."
    )

    base_parent = st.text_input(
        "Base path (where campaign folders are stored)",
        value=st.session_state.get("campaign_base_parent", DEFAULT_CAMPAIGN_BASE_PARENT),
        key="step1_base_parent",
        placeholder=DEFAULT_CAMPAIGN_BASE_PARENT,
    )
    if base_parent:
        st.session_state["campaign_base_parent"] = base_parent.rstrip("/")

    mode = st.radio(
        "Campaign folder",
        ["Create new campaign folder", "Use existing campaign folder"],
        key="step1_mode",
    )

    parent_path = Path(base_parent.rstrip("/")) if base_parent else None
    existing_folders: List[str] = []
    if parent_path and parent_path.exists() and parent_path.is_dir():
        existing_folders = sorted([p.name for p in parent_path.iterdir() if p.is_dir() and not p.name.startswith(".")])

    if mode == "Create new campaign folder":
        campaign_folder_name = st.text_input(
            "Campaign folder name",
            value="",
            key="step1_campaign_name",
            placeholder="e.g. 003-20260304-management-forum-stress",
        )
        if st.button("Create campaign folder", key="step1_create"):
            if not base_parent or not campaign_folder_name:
                st.error("Please set base path and campaign folder name.")
            else:
                campaign_path = parent_path / campaign_folder_name.strip()
                try:
                    campaign_path.mkdir(parents=True, exist_ok=True)
                    for sub in CAMPAIGN_SUBFOLDERS:
                        (campaign_path / sub).mkdir(exist_ok=True)
                    st.session_state["campaign_base_path"] = str(campaign_path)
                    suggested_gesamt = campaign_path / "lists" / f"{campaign_folder_name.strip()}-gesamt.csv"
                    st.session_state["path_to_gesamt_csv"] = str(suggested_gesamt)
                    st.success(f"Created: **{campaign_path}** with subfolders: **lists**, **templates**, **pdf_output**.")
                except OSError as e:
                    st.error(f"Could not create folder: {e}")
    else:
        if not existing_folders:
            st.caption("No subfolders found under the base path. Create a new campaign folder first or check the base path.")
        else:
            selected = st.selectbox(
                "Existing campaign folder",
                options=existing_folders,
                key="step1_existing_folder",
            )
            if st.button("Use this campaign folder", key="step1_use_existing"):
                campaign_path = parent_path / selected
                st.session_state["campaign_base_path"] = str(campaign_path)
                st.session_state["path_to_gesamt_csv"] = str(campaign_path / "lists" / f"{selected}-gesamt.csv")
                st.success(f"Using campaign folder: **{campaign_path}**")

    if st.session_state.get("campaign_base_path"):
        st.markdown("---")
        st.markdown("**Next:** Put your list CSV files in the **lists** subfolder, then combine them into one **-gesamt** file (button below). Ensure the first column is **company_name** (not `name`). Templates go in **templates**; generated PDFs will go in **pdf_output**.")
        path_gesamt = st.text_input(
            "Path to -gesamt CSV (used as input for Step 2)",
            value=st.session_state.get("path_to_gesamt_csv", ""),
            key="step1_path_gesamt",
            placeholder=f"{st.session_state.get('campaign_base_path', '')}/lists/campaign-name-gesamt.csv",
        )
        if "step1_path_gesamt" in st.session_state:
            st.session_state["path_to_gesamt_csv"] = st.session_state["step1_path_gesamt"]

        lists_dir = Path(st.session_state["campaign_base_path"]) / "lists"
        output_gesamt = st.session_state.get("path_to_gesamt_csv", "").strip() or str(lists_dir / f"{Path(st.session_state['campaign_base_path']).name}-gesamt.csv")
        output_basename = Path(output_gesamt).name

        # List CSV files in lists folder (excluding the -gesamt output file)
        available_csvs: List[str] = []
        if lists_dir.exists():
            available_csvs = sorted(
                p.name for p in lists_dir.glob("*.csv")
                if p.name != output_basename and "-gesamt" not in p.name.lower()
            )

        if available_csvs:
            st.markdown("**Lists in folder** (select which to include in the combined file):")
            selected_lists = st.multiselect(
                "Select lists to combine",
                options=available_csvs,
                default=available_csvs,
                key="step1_selected_lists",
                label_visibility="collapsed",
            )
            st.caption(f"{len(available_csvs)} file(s) in **lists** — {len(selected_lists)} selected.")
        else:
            selected_lists = []
            if lists_dir.exists():
                st.caption("No CSV files found in **lists** (excluding -gesamt). Add CSV files and run combine.")
            else:
                st.caption("**lists** folder does not exist yet.")

        if st.button("Combine lists to -gesamt", key="step1_combine"):
            if not lists_dir.exists():
                st.error(f"Lists folder does not exist: {lists_dir}")
            elif not selected_lists:
                st.error("Select at least one list to combine.")
            else:
                try:
                    total, msg = combine_lists_to_gesamt(
                        lists_dir, Path(output_gesamt),
                        exclude_gesamt=True,
                        selected_filenames=selected_lists,
                    )
                    st.session_state["path_to_gesamt_csv"] = output_gesamt
                    st.success(msg)
                except ValueError as e:
                    st.error(str(e))
                except OSError as e:
                    st.error(f"Error writing file: {e}")

        st.info("Then run **Step 2: List enrichment** with the -gesamt CSV as input.")


def render_step5() -> None:
    st.subheader("Step 5: List enrichment")
    st.markdown("Run the list_processing pipeline to enrich and score the list.")
    base = st.session_state.get("campaign_base_path", "")
    default_in = st.session_state.get("path_to_gesamt_csv", "") or (f"{base}/lists/campaign-name-gesamt.csv".replace("//", "/") if base else "")
    default_out = ""
    if default_in:
        p = Path(default_in)
        default_out = str(p.parent / f"{p.stem}-out.csv")

    col1, col2 = st.columns(2)
    with col1:
        input_csv = st.text_input("Input CSV", value=default_in, key="step5_input", placeholder="/path/to/campaign-name-gesamt.csv")
    with col2:
        output_csv = st.text_input("Output CSV", value=default_out, key="step5_output", placeholder="/path/to/campaign-name-gesamt-out.csv")

    prompt_names = load_prompt_names()
    scoring_prompt = st.selectbox("Scoring prompt", prompt_names, key="step5_prompt")
    backend = st.selectbox("Backend", ["local", "openai"], key="step5_backend")
    limit_rows = st.number_input(
        "Limit rows (0 = no limit)",
        min_value=0,
        value=0,
        step=1,
        key="step5_limit_rows",
        help="Run on only the first N rows (e.g. 10 for a quick test).",
    )

    with st.expander("Advanced options"):
        no_enrichment = st.checkbox("Skip enrichment", value=False, key="step5_no_enrichment")
        no_salutation = st.checkbox("Skip salutation", value=False, key="step5_no_salutation")
        no_scoring = st.checkbox("Skip scoring", value=False, key="step5_no_scoring")
        no_resume = st.checkbox("No resume (start fresh)", value=False, key="step5_no_resume")
        max_workers_http = st.number_input("Max workers HTTP", min_value=1, max_value=50, value=10, key="step5_mw_http")
        max_workers_llm = st.number_input("Max workers LLM", min_value=1, max_value=20, value=5, key="step5_mw_llm")

    if st.button("Run list enrichment", key="step5_run"):
        if not input_csv or not output_csv:
            st.error("Please set Input CSV and Output CSV.")
        else:
            input_path = Path(input_csv)
            if not input_path.exists():
                st.error(f"Input file not found: {input_csv}")
            else:
                run_input = input_csv
                tmp_limited: Path | None = None
                if limit_rows and limit_rows > 0:
                    try:
                        tmp_limited = _create_limited_csv(input_path, limit_rows)
                        run_input = str(tmp_limited)
                        st.caption(f"Running on first **{limit_rows}** rows (temp file).")
                    except Exception as e:
                        st.error(f"Could not create limited CSV: {e}")
                        run_input = ""
                if run_input:
                    try:
                        cmd = [
                            sys.executable, "-m", "list_processing.main",
                            run_input, output_csv,
                            "--scoring-prompt", scoring_prompt,
                            "--backend", backend,
                        ]
                        if no_enrichment:
                            cmd.append("--no-enrichment")
                        if no_salutation:
                            cmd.append("--no-salutation")
                        if no_scoring:
                            cmd.append("--no-scoring")
                        if no_resume:
                            cmd.append("--no-resume")
                        cmd.extend(["--max-workers-http", str(max_workers_http)])
                        cmd.extend(["--max-workers-llm", str(max_workers_llm)])

                        st.markdown("**Pipeline output (live):**")
                        progress_placeholder = st.empty()
                        code, out, err = run_cmd_streaming(
                            cmd, REPO_ROOT,
                            stream_placeholder=progress_placeholder,
                            output_height=PIPELINE_OUTPUT_HEIGHT,
                        )
                        progress_placeholder.text_area(
                            "Pipeline output",
                            value=out or err or "(no output)",
                            height=PIPELINE_OUTPUT_HEIGHT,
                            disabled=True,
                            label_visibility="collapsed",
                        )

                        if code != 0:
                            st.error("Command failed")
                        else:
                            st.success("Done")
                            st.session_state["step5_output_csv"] = output_csv
                        st.caption("Command: " + " ".join(cmd))
                    finally:
                        if tmp_limited is not None and tmp_limited.exists():
                            tmp_limited.unlink()

    if st.session_state.get("step5_output_csv"):
        st.info(f"Output from this step: **{st.session_state['step5_output_csv']}** — use as input in Step 5.1.")


def render_step51() -> None:
    st.subheader("Step 5.1: Filter final output")
    st.markdown("Filter the pipeline output by score, required fields, and optional branchencode.")
    default_in = st.session_state.get("step5_output_csv", "") or (st.session_state.get("path_to_gesamt_csv", "").replace("-gesamt.csv", "-gesamt-out.csv") if st.session_state.get("path_to_gesamt_csv") else "")
    default_out = ""
    if default_in:
        p = Path(default_in)
        default_out = str(p.parent / f"{p.stem}-final.csv")

    input_csv = st.text_input("Input CSV (pipeline output)", value=default_in, key="step51_input", placeholder="/path/to/campaign-name-gesamt-out.csv")
    output_csv = st.text_input("Output CSV (optional; default: input_stem__filtered.csv)", value=default_out, key="step51_output", placeholder="/path/to/campaign-name-gesamt-out-final.csv")
    min_score = st.number_input("Min score", min_value=0.0, max_value=5.0, value=4.0, step=0.5, key="step51_min_score")
    issues_csv = st.text_input("Issues CSV (optional)", value="", key="step51_issues", placeholder="default: <input_stem>.final_review_issues.csv")
    branchencodes = st.text_input("Branchencode(s), comma-separated (optional)", value="", key="step51_branchencodes", placeholder="e.g. 61, 62")

    if st.button("Run filter", key="step51_run"):
        if not input_csv:
            st.error("Please set Input CSV.")
        else:
            cmd = [sys.executable, str(REPO_ROOT / "scripts" / "list_processing" / "filter_final_output.py"), input_csv, "--min-score", str(min_score)]
            if output_csv:
                cmd.extend(["--output-csv", output_csv])
            if issues_csv:
                cmd.extend(["--issues-csv", issues_csv])
            for bc in [x.strip() for x in branchencodes.split(",") if x.strip()]:
                cmd.extend(["--branchencode", bc])

            with st.spinner("Filtering..."):
                code, out, err = run_cmd(cmd, REPO_ROOT)
            if code != 0:
                st.error("Command failed")
                st.code(err or out, language="text")
            else:
                st.success("Done")
                out_path = output_csv or str(Path(input_csv).with_name(Path(input_csv).stem + "__filtered.csv"))
                st.session_state["step51_output_csv"] = out_path
                st.code(out + (("\n" + err) if err else ""), language="text")
            st.caption("Command: " + " ".join(cmd))

    if st.session_state.get("step51_output_csv"):
        st.info(f"Output: **{st.session_state['step51_output_csv']}** — add Template column (Step 5.2) then upload (Step 6).")


def render_step52() -> None:
    st.subheader("Step 5.2: Template column")
    st.markdown("Add or set the **Template** column so every row has a PDF filename from your templates directory (required for upload). You can use one template for all rows or split rows across multiple templates by count.")
    default_in = st.session_state.get("step51_output_csv", "")
    base = st.session_state.get("campaign_base_path", "")
    default_tpl_dir = f"{base}/templates".replace("//", "/") if base else ""
    default_out = ""
    if default_in:
        p = Path(default_in)
        default_out = str(p.parent / f"{p.stem}-with-template.csv")

    input_csv = st.text_input("Input CSV", value=default_in, key="step52_input", placeholder="/path/to/filtered.csv")
    templates_dir = st.text_input("Templates directory (PDFs)", value=default_tpl_dir, key="step52_templates", placeholder="/path/to/templates")
    output_csv = st.text_input("Output CSV", value=default_out, key="step52_output", placeholder="/path/to/output-with-template.csv")

    template_options: list[str] = []
    if templates_dir and Path(templates_dir).exists():
        template_options = sorted([p.name for p in Path(templates_dir).glob("*.pdf")])

    mode = st.radio(
        "Assignment",
        ["Single template for all rows", "Split by row count across multiple templates"],
        key="step52_mode",
    )

    single_template: str | None = None
    split_entries: list[tuple[str, int | None]] = []  # (template_name, count or None for rest)

    if mode == "Single template for all rows":
        single_template = st.selectbox("Template", [""] + template_options, key="step52_single_template") if template_options else None
        if single_template == "":
            single_template = None
    else:
        if "step52_split_count" not in st.session_state:
            st.session_state["step52_split_count"] = 1
        n_split = st.number_input("Number of template segments", min_value=1, max_value=20, value=st.session_state["step52_split_count"], key="step52_n_split")
        st.session_state["step52_split_count"] = int(n_split)
        for i in range(int(n_split)):
            selected_tpl = st.session_state.get(f"step52_split_t_{i}", "") or "(select template)"
            st.markdown(f"**Segment {i + 1}:** {selected_tpl}")
            col_t, col_n = st.columns([2, 1])
            with col_t:
                st.selectbox("Template", template_options, key=f"step52_split_t_{i}", label_visibility="collapsed") if template_options else None
            with col_n:
                st.checkbox("Rest (remaining rows)", value=(i == int(n_split) - 1), key=f"step52_split_rest_{i}")
                st.number_input("Rows", min_value=1, value=100, key=f"step52_split_n_{i}")

    if st.button("Run add template column", key="step52_run"):
        if not input_csv or not templates_dir or not output_csv:
            st.error("Please set Input CSV, Templates directory, and Output CSV.")
        elif not template_options:
            st.error("Templates directory has no PDFs or path invalid.")
        elif mode == "Single template for all rows" and not single_template:
            st.error("Select a template.")
        elif mode == "Split by row count across multiple templates":
            n_split = int(st.session_state.get("step52_n_split", 1))
            split_entries = []
            for i in range(n_split):
                t = st.session_state.get(f"step52_split_t_{i}", "")
                use_rest = st.session_state.get(f"step52_split_rest_{i}", False)
                n_rows = int(st.session_state.get(f"step52_split_n_{i}", 100) or 100)
                if t:
                    split_entries.append((t, None if use_rest else n_rows))
            if not split_entries:
                st.error("Add at least one template segment and select a template for each.")
            else:
                script = REPO_ROOT / "scripts" / "list_processing" / "add_template_column.py"
                if not script.exists():
                    st.error("add_template_column.py not found. Create it first.")
                else:
                    cmd = [sys.executable, str(script), "--input-csv", input_csv, "--templates-dir", templates_dir, "--output-csv", output_csv]
                    for tpl_name, count in split_entries:
                        if count is None:
                            cmd.extend(["--split", tpl_name])
                        else:
                            cmd.extend(["--split", f"{tpl_name}:{count}"])
                    with st.spinner("Adding template column..."):
                        code, out, err = run_cmd(cmd, REPO_ROOT)
                    if code != 0:
                        st.error("Command failed")
                        st.code(err or out, language="text")
                    else:
                        st.success("Done")
                        st.session_state["step52_output_csv"] = output_csv
                        st.code(out + (("\n" + err) if err else ""), language="text")
                    st.caption("Command: " + " ".join(cmd))
        else:
            script = REPO_ROOT / "scripts" / "list_processing" / "add_template_column.py"
            if not script.exists():
                st.error("add_template_column.py not found. Create it first.")
            else:
                cmd = [sys.executable, str(script), "--input-csv", input_csv, "--templates-dir", templates_dir, "--output-csv", output_csv]
                if mode == "Single template for all rows" and single_template:
                    cmd.extend(["--template", single_template])
                with st.spinner("Adding template column..."):
                    code, out, err = run_cmd(cmd, REPO_ROOT)
                if code != 0:
                    st.error("Command failed")
                    st.code(err or out, language="text")
                else:
                    st.success("Done")
                    st.session_state["step52_output_csv"] = output_csv
                    st.code(out + (("\n" + err) if err else ""), language="text")
                st.caption("Command: " + " ".join(cmd))

    if st.session_state.get("step52_output_csv"):
        st.info(f"Output: **{st.session_state['step52_output_csv']}** — use in Step 6 (Firestore upload).")


def render_step6() -> None:
    st.subheader("Step 6: Firestore upload")
    st.markdown("Validate and optionally upload the final CSV (with Template column) to GCS for the upload_processor.")
    st.caption("Choose **dev** or **prod** environment. Upload to GCS is off by default — no Firestore writes unless you check \"Upload to GCS\".")
    default_in = st.session_state.get("step52_output_csv", "")
    base = st.session_state.get("campaign_base_path", "")
    default_tpl = f"{base}/templates".replace("//", "/") if base else ""

    env = st.selectbox("Environment", ["dev", "prod"], key="step6_env")
    owner_id = st.text_input("Owner ID", value=st.session_state.get("step6_owner", ""), key="step6_owner", placeholder="xLRk37rnV7T4CbOXzW5N3saxVfy1")
    campaign_code = st.text_input("Campaign code", value=st.session_state.get("step6_code", ""), key="step6_code", placeholder="RLIT")
    campaign_name = st.text_input("Campaign name (optional)", value=st.session_state.get("step6_name", ""), key="step6_name", placeholder="Rocket Letter - IT 2")
    campaign_id = st.text_input("Campaign ID (optional; UUID if empty)", value=st.session_state.get("campaign_base_path", "").split("/")[-1] if st.session_state.get("campaign_base_path") else "", key="step6_campaign_id", placeholder="003-20260304-management-forum-stress")
    input_csv = st.text_input("Input CSV", value=default_in, key="step6_input", placeholder="/path/to/final-with-template.csv")
    templates_dir = st.text_input("Templates directory", value=default_tpl, key="step6_templates", placeholder="/path/to/templates")
    destination = st.text_input("Destination URL", value=st.session_state.get("step6_destination", ""), key="step6_destination", placeholder="https://www.rocket-letter.de/erstgespraech")
    do_upload = st.checkbox("Upload to GCS", value=False, key="step6_upload")

    if st.button("Run prepare/upload", key="step6_run"):
        if not all([owner_id, campaign_code, input_csv, templates_dir, destination]):
            st.error("Please set Owner ID, Campaign code, Input CSV, Templates dir, and Destination URL.")
        else:
            cmd = [
                sys.executable, str(REPO_ROOT / "scripts" / "business" / "local_process_upload.py"),
                "--env", env, "--owner-id", owner_id, "--campaign-code", campaign_code,
                "--input-csv", input_csv, "--templates-dir", templates_dir, "--destination", destination,
            ]
            if campaign_name:
                cmd.extend(["--campaign-name", campaign_name])
            if campaign_id:
                cmd.extend(["--campaign-id", campaign_id])
            if do_upload:
                cmd.append("--upload")

            with st.spinner("Running..."):
                code, out, err = run_cmd(cmd, REPO_ROOT)
            if code != 0:
                st.error("Command failed")
                st.code(err or out, language="text")
            else:
                st.success("Done")
                st.code(out + (("\n" + err) if err else ""), language="text")
            st.caption("Command: " + " ".join(cmd))

    st.info("Next: Download the list with_links (Step 7), then generate/send PDFs (Step 8).")


def render_step7() -> None:
    st.subheader("Step 7: Download with_links")
    st.markdown("After upload, download the list with links from your existing process (e.g. frontend or export). Use that file as the contacts CSV in Step 8.")


def render_step8() -> None:
    st.subheader("Step 8: PDF generation and sending")
    st.markdown("Generate PDFs and optionally send them via onlinebrief24.de.")
    st.caption("Dev mode: **Actually send to onlinebrief24** is off by default (PDF-only). When enabled, the letter API is always called with **test** mode — no live letters are sent.")
    base = st.session_state.get("campaign_base_path", "")
    default_tpl = f"{base}/templates".replace("//", "/") if base else ""
    default_pdf = f"{base}/pdf_output".replace("//", "/") if base else ""
    config_options = load_config_options()
    config_choices = [name for name, _ in config_options]
    config_paths = {name: path for name, path in config_options}

    contacts_csv = st.text_input("Contacts CSV (list with links)", value="", key="step8_contacts", placeholder="/path/to/with_links.csv")
    templates_dir = st.text_input("Templates directory", value=default_tpl, key="step8_templates", placeholder="/path/to/templates")
    config_name = st.selectbox("Config file", config_choices or ["(none)"], key="step8_config") if config_choices else "(none)"
    config_path = str(config_paths[config_name]) if config_name and config_name in config_paths else ""
    campaign_id = st.text_input("Campaign ID", value=st.session_state.get("campaign_base_path", "").split("/")[-1] if st.session_state.get("campaign_base_path") else "", key="step8_campaign_id", placeholder="003-20260304-management-forum-stress")
    save_pdfs_dir = st.text_input("Save PDFs directory (optional, for debugging)", value=default_pdf, key="step8_save_pdfs", placeholder="/path/to/pdf_output")
    limit = st.number_input("Limit (optional, e.g. 10 for test)", min_value=0, value=0, key="step8_limit")
    do_upload = st.checkbox("Actually send to onlinebrief24", value=False, key="step8_upload")

    with st.expander("Before sending – checklist"):
        st.markdown("""
        - [ ] Config file created and configured
        - [ ] QR Code positions correct
        - [ ] Link 1 (tracking text) correct
        - [ ] Datum (date) position correct
        - [ ] Anrede (salutation) correct
        - [ ] Link 2 correct
        - [ ] If using multiple templates: all of them checked
        - [ ] Test run with limit (e.g. 10) and Save PDFs dir first
        """)

    if st.button("Run PDF generation / send", key="step8_run"):
        if not contacts_csv or not templates_dir or not config_path:
            st.error("Please set Contacts CSV, Templates directory, and Config file.")
        else:
            cmd = [
                sys.executable, str(REPO_ROOT / "scripts" / "send_letter" / "send_letters_onlinebrief24.py"),
                contacts_csv, "--templates-dir", templates_dir, "--config", config_path,
                "--mode", "test",  # always test mode from this UI — no live letter API writes
            ]
            if campaign_id:
                cmd.extend(["--campaign-id", campaign_id])
            if save_pdfs_dir:
                cmd.extend(["--save-pdfs-dir", save_pdfs_dir])
            if limit and limit > 0:
                cmd.extend(["--limit", str(limit)])
            if do_upload:
                cmd.append("--upload")

            with st.spinner("Running..."):
                code, out, err = run_cmd(cmd, REPO_ROOT)
            if code != 0:
                st.error("Command failed")
                st.code(err or out, language="text")
            else:
                st.success("Done")
                st.code(out + (("\n" + err) if err else ""), language="text")
            st.caption("Command: " + " ".join(cmd))


def main() -> None:
    st.set_page_config(page_title="Campaign workflow", layout="wide")
    init_session_state()

    st.sidebar.title("Campaign workflow")
    st.sidebar.warning(
        "**Dev mode:** No Firestore/GCS upload and no live letter API by default. "
        "Step 6 upload and Step 8 send are off unless you enable them; Step 8 uses **test** API mode when send is on."
    )
    step = st.sidebar.radio("Step", STEPS, key="step_radio")

    # Presets in sidebar
    st.sidebar.markdown("---")
    presets = load_presets()
    preset_names = list(presets.keys())
    if preset_names:
        selected = st.sidebar.selectbox("Load preset", [""] + preset_names, key="preset_load")
        if selected and st.sidebar.button("Apply preset"):
            p = presets[selected]
            for k, v in (p or {}).items():
                st.session_state[k] = v if isinstance(v, str) else str(v)
            st.sidebar.success(f"Applied preset: {selected}")
        save_name = st.sidebar.text_input("Save current as preset name", key="preset_save_name")
        if save_name and st.sidebar.button("Save preset"):
            presets[save_name] = {
                "campaign_base_parent": st.session_state.get("campaign_base_parent", DEFAULT_CAMPAIGN_BASE_PARENT),
                "campaign_base_path": st.session_state.get("campaign_base_path", ""),
                "path_to_gesamt_csv": st.session_state.get("path_to_gesamt_csv", ""),
                "step6_owner": st.session_state.get("step6_owner", ""),
                "step6_code": st.session_state.get("step6_code", ""),
                "step6_name": st.session_state.get("step6_name", ""),
                "step6_destination": st.session_state.get("step6_destination", ""),
            }
            save_presets(presets)
            st.sidebar.success(f"Saved preset: {save_name}")

    if step == STEPS[0]:
        render_step1()
    elif step == STEPS[1]:
        render_step5()
    elif step == STEPS[2]:
        render_step51()
    elif step == STEPS[3]:
        render_step52()
    elif step == STEPS[4]:
        render_step6()
    elif step == STEPS[5]:
        render_step7()
    else:
        render_step8()


if __name__ == "__main__":
    main()
