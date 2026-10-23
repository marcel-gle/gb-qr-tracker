# Personalized letter paragraphs from agency websites

Crawlt pro Empfänger die Website (Startseite + bis zu 3 relevante Unterseiten), säubert den Text und lässt daraus den individuellen Absatz schreiben – über die Claude API oder lokal (LM Studio / Ollama).

## Setup

```bash
cd agentur-absatz
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export ANTHROPIC_API_KEY=sk-ant-...      # only needed for Claude profiles
```

## Recipient list

CSV (comma, semicolon or tab) or XLSX. Required: a URL column. Recommended: agency and first name. Column names are matched via `columns` in `config.yaml`. All other columns are passed through to the data-merge CSV.

## Usage

```bash
# 1. Crawl and cache websites
python paragraphs.py crawl recipients.csv

# 2. Inspect what the model receives for one recipient
python paragraphs.py prompt recipients.csv --row 3

# 3. Test run: 10 random recipients, three models side by side
python paragraphs.py run recipients.csv --sample 10 --profile sonnet --profile opus --profile qwen-ollama --name model-test

# 4. Production: all recipients, Sonnet via Batch API (50% cheaper, async)
python paragraphs.py run recipients.csv --profile sonnet --batch --name mailing-nov
```

Filters: `--limit 5`, `--rows 2,7,9`, `--sample 10 --seed 1`. `--refresh` re-crawls instead of using the cache.
Prompt variants: copy `prompt.md` and run with `--prompt prompt_v2.md`.

## Output (`runs/<timestamp>_<name>/`)

| File | Content |
|---|---|
| `comparison.csv` | One row per recipient, paragraphs of all profiles side by side (only with >1 profile) |
| `results.csv` | Full detail: industries, angle, evidence, flags, tokens, cost, seconds |
| `datamerge_<profile>.csv` | Original columns + `AgencyName`, `Paragraph`, `Review` – for InDesign data merge |
| `summary.json` | Cost and duration per profile |
| `raw_responses.jsonl` | Raw model responses – first place to look when debugging |

Review CSVs use `;` (German Excel), the data-merge CSV uses `,`. All UTF-8.

## Flags

`too_long_61_words`, `4_sentences`, `char_!`, `phrase_…`, `name_missing`, `low_confidence`, `no_paragraph` (model declined, e.g. not a recruiting agency), `no_json`, `crawl_error` / `crawl_thin` (unreachable or almost empty, often JS-rendered sites). Rules live under `validation` in `config.yaml`. Unreachable sites are never sent to a model.

## Local models

- **LM Studio:** load model, start server under "Developer", set the exact model name in profile `qwen-lmstudio`.
- **Ollama:** `ollama pull <model>`, use the name from `ollama list` in `qwen-ollama` / `gemma-ollama`.

Note: the JSON keys in the model response (`absatz`, `agenturname`, `winkel` …) stay German because they are defined in the German prompt. They are mapped to English column names in `cmd_run`.
