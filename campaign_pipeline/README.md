# Campaign Pipeline

Staged list processing for outbound campaigns: merge raw lists, score domains, scrape imprints, dedupe, template assignment, final QA, Firestore upload, and local PDF generation.

## Run the UI

```bash
pip install -r requirements-dev.txt
streamlit run campaign_pipeline/ui/app.py
```

## Google Drive storage (optional)

In **Step 1**, choose **Google Drive** instead of a local folder. The app keeps a local cache under `~/.cache/campaign-pipeline/drive/{folder_id}/` and syncs changes back to your shared Briefversand folder.

1. Create a **Desktop OAuth client** in [Google Cloud Console](https://console.cloud.google.com/apis/credentials) and enable the Google Drive API.
2. Save the downloaded JSON as `~/.config/campaign-pipeline/client_secret.json` (or set `CAMPAIGN_DRIVE_CLIENT_SECRET` to its path).
3. In Step 1, click **Connect Google Drive** and authorize in the browser.
4. Paste your Briefversand folder ID from the Drive URL (`drive.google.com/drive/folders/FOLDER_ID`) and click **Save Drive folder**.
5. Create or select a campaign folder as usual. After each pipeline step, changes are pushed to Drive automatically; use **Pull** / **Push** in the sidebar to sync manually.

OAuth token and settings are stored in `~/.config/campaign-pipeline/` (not committed to git).

## Campaign folder layout

```
my-campaign/
  lists/
    incoming/          # drop source CSVs here
    {base}_raw.csv
    {base}_scored.csv
    {base}_imprint.csv
    {base}_final.csv
  templates/           # PDF letter templates
  pdf_output/
  .pipeline/
    state.json         # per-domain progress (funnel top-up)
    manifest.json
```

## LLM backends (scoring / imprint extract)

In the Streamlit **Scoring** step (or CLI `--backend`), choose:

| Backend | Env | Notes |
|---------|-----|--------|
| `local` | `ML_STUDIO_BASE_URL`, `LOCAL_MODEL` | LM Studio / OpenAI-compatible local server |
| `openai` | `OPENAI_API_KEY`, optional `OPENAI_MODEL` | Default model `gpt-4.1-mini` |
| `claude` | `ANTHROPIC_API_KEY`, optional `ANTHROPIC_MODEL`, `ANTHROPIC_EFFORT` | Default model `claude-sonnet-4-5`; effort `low` \| `medium` \| `high`; optional `ANTHROPIC_WORKSPACE_ID` |

Keys can live in the repo `.env` (loaded automatically). Install cloud SDKs via `pip install -r requirements-dev.txt` (`openai`, `anthropic`).

Example `.env` for Sonnet 5.5 with low effort:

```bash
ANTHROPIC_API_KEY=sk-ant-...
ANTHROPIC_MODEL=claude-sonnet-5-5
ANTHROPIC_EFFORT=low
```

```bash
python -m campaign_pipeline.main ./my-campaign my-campaign score --backend claude --scoring-prompt handwerk_analysis
```

## CLI (secondary)

```bash
python -m campaign_pipeline.main ./my-campaign my-campaign merge --incoming lists/incoming/*.csv
python -m campaign_pipeline.main ./my-campaign my-campaign dedupe-domain
python -m campaign_pipeline.main ./my-campaign my-campaign score --scoring-prompt handwerk_analysis
python -m campaign_pipeline.main ./my-campaign my-campaign imprint
python -m campaign_pipeline.main ./my-campaign my-campaign dedupe-address
python -m campaign_pipeline.main ./my-campaign my-campaign final
python -m campaign_pipeline.main ./my-campaign my-campaign status
```

## Pipeline order

1. Merge raw CSVs (domain required)
2. Dedupe by domain
3. Scoring (optional — before imprint to save cost)
4. Imprint scrape
5. Dedupe by company_name + address
6. Template column
7. Final review → `_final.csv`
8. Firestore upload (`local_process_upload.py`)
9. PDFs from manual `with_links` CSV (no onlinebrief24 API)

## Imprint scraping

The imprint step fetches each domain's Impressum/Kontakt page and extracts the
company address, phone, email, and managing directors. Robustness features:

- **Charset repair** — pages served as `text/html` without a charset (which
  `requests` otherwise decodes as ISO-8859-1) are re-decoded from the
  `<meta charset>` tag, fixing mojibake like `GeschÃ¤ftsfÃ¼hrer`.
- **Deep link crawl** — when the homepage has no Impressum link and the common
  fallback paths miss, one level of internal links is followed. This handles
  HTTrack mirror index pages and menus that live on an inner landing page.
- **North Data fallback** — when the imprint has an address but no managing
  director (common for logistics/holding companies), the current legal
  representative(s) are looked up on North Data via its public JSON-LD data.
  Disable with `--no-northdata` on the CLI, or the checkbox in the UI.
- **Phone provenance** — stage CSVs keep sources separate:
  - `input_phone` — number from the incoming list (e.g. ocean.io)
  - `imprint_phone` — company number from the Impressum scrape
  - `phone` — lettershop field: input wins; imprint fills only if empty
  - `apollo_phone_{1..N}` — optional verified personal numbers from Apollo.io
- **Apollo phones (optional)** — with `APOLLO_API_KEY` set and the UI checkbox
  **Apollo-Telefonnummern abfragen** (or CLI `--apollo-phones`), each named
  managing director is enriched via Apollo People Enrichment in two stages:
  1. **Match** (no phone reveal) with name, domain, company name and LinkedIn URL.
  2. **Verify**: accepted only if `match_confidence` is `high`/`medium`, the
     person's *current* employer domain equals the row domain, and first/last
     name match (umlauts, case and double first names normalized). Otherwise
     `apollo_status_i = rejected` with `apollo_reject_reason_i`
     (`no_match`, `low_confidence`, `domain_mismatch`, `not_current`,
     `name_mismatch`) — no reveal, no phone credits.
  3. **Reveal** by Apollo person id (`reveal_phone_number` + `poll_only`) and
     **poll** `/webhook_result/{request_id}`. Only `valid_number` entries for
     that person id are kept; best number (high confidence, mobile first) in
     `apollo_phone_i`, all numbers in `apollo_phones_all_i`.

  Stage CSVs also carry `apollo_status_i`, `apollo_match_confidence_i`,
  `apollo_person_id_i` and `apollo_request_id_i`. Apollo never overwrites
  `phone`. Decisions (including rejections) are cached under
  `.pipeline/apollo_phones.cache.pkl`; reveals still outstanding after the
  5-minute poll timeout stay `pending` and are polled (not re-billed) on the
  next Apollo-only or imprint run.

  **Match details (no extra credits):** the match response that is already
  paid for is stored too — only for verified matches, never for rejected ones.
  Per director: `apollo_full_name_i`, `apollo_title_i`, `apollo_headline_i`,
  `apollo_seniority_i`, `apollo_departments_i`, `apollo_position_start_i`,
  `apollo_email_i`, `apollo_email_status_i`, `apollo_linkedin_url_i`,
  `apollo_twitter_url_i`, `apollo_facebook_url_i`, `apollo_city_i`,
  `apollo_state_i`, `apollo_country_i`. Per row (company): `apollo_org_name`,
  `apollo_org_domain`, `apollo_org_phone`, `apollo_org_linkedin_url`,
  `apollo_org_industry`, `apollo_org_keywords`, `apollo_org_employees`,
  `apollo_org_annual_revenue`, `apollo_org_founded_year`, `apollo_org_street`,
  `apollo_org_postcode`, `apollo_org_city`, `apollo_org_country`,
  `apollo_org_description`, `apollo_org_technologies`, `apollo_org_id`.
  Apollo values never overwrite `email`, `phone` or `linkedin_profile_url_i`.

  `{base}_final.csv` contains `phone`, `input_phone`, `imprint_phone`,
  `apollo_org_phone` and per director `apollo_phone_i`, `apollo_phone_type_i`,
  `apollo_phones_all_i`, `apollo_title_i`, `apollo_email_i`,
  `apollo_linkedin_url_i` (before the LinkedIn and `Template` columns).

  Credit safeguards: each person is matched and revealed at most once (cache by
  domain+name and by Apollo person id, saved after every row); rows that the
  later address dedupe would drop are not queried; waterfall and personal-email
  reveals are explicitly disabled; polling is free and transient poll errors
  stay `pending`; on HTTP 401/402/403 (invalid key, no permission, no credits)
  all further Apollo requests of the run are skipped.

The key is read from the shell or from the repo-root `.env`
(`APOLLO_API_KEY=...`, git-ignored); shell values take precedence. Both the
Streamlit UI and the CLI load `.env` on start.

```bash
python -m campaign_pipeline.main ./my-campaign my-campaign imprint --apollo-phones
# Or Apollo only on an existing CSV (same cache, no re-scrape):
python -m campaign_pipeline.main ./my-campaign my-campaign apollo-phones
python -m campaign_pipeline.main ./my-campaign my-campaign apollo-phones \
  --input lists/other.csv --output lists/other_with_apollo.csv
```

In the Streamlit UI use **Nur Apollo laufen lassen** on the imprint step and set
the CSV path (defaults to `_imprint.csv`; `_scored.csv` works too when it has
`Imprint: Managing director` — names are split into `first_name_*` /
`last_name_*`). After Apollo, re-run address dedupe → template → final so
`apollo_*` columns land in `_final.csv`.

### Re-scraping only incomplete rows

After a full imprint run you can re-process just the rows that are still missing
a street or a managing-director name (e.g. to pick up the charset/crawl/North
Data improvements) without redoing the whole list. This reads the existing
`{base}_imprint.csv`, forces a fresh fetch (ignoring the per-domain cache) for
those rows only, and leaves complete rows untouched.

```bash
python -m campaign_pipeline.main ./my-campaign my-campaign imprint --only-missing-fields
```

In the UI, tick **Re-scrape only rows missing street/name** on the Imprint step.

### Skipping scoring

Imprint prefers `{base}_scored.csv`, but if it is missing it falls back to
`{base}_raw_deduped.csv` then `{base}_raw.csv`, and automatically disables the
score pass-filter. In the UI you can also check **Ignore score pass filter**.

When building the final CSV after skipping scoring, enable **Skip min-score filter**
(or pass `--skip-score-filter` on the CLI) so rows without `match_score` are kept.

```bash
python -m campaign_pipeline.main ./my-campaign my-campaign imprint --skip-score-filter
python -m campaign_pipeline.main ./my-campaign my-campaign final --skip-score-filter
```

## Funnel top-up

Use **Append only new domains** when merging additional source files, then **Continue pipeline** in the sidebar to score/scrape only domains not yet at that stage.

## Prompt scoring scales

Configure per prompt in `scripts/business/prompts.json`:

```json
"score_config": {
  "field": "match_score",
  "scale": "0-5",
  "pass_threshold": 4
}
```

Scales: `0-5`, `0-10`, `binary` (also supports `verkauft`/`installiert` boolean fields).
