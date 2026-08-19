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

Legacy `list_processing` and `scripts/streamlit_app.py` remain unchanged.
