# Agenturmarkt.de Scraper

Crawls all ~40,480 agencies from agenturmarkt.de into SQLite, resumable, with CSV/XLSX export.

## Setup

```bash
pip install requests beautifulsoup4 lxml openpyxl
```

## Recommended workflow

```bash
# 1. Sample run first — validates the parser against the live site
python agenturmarkt_scraper.py scrape --limit 50

# 2. Check the output
python agenturmarkt_scraper.py export
open agenturmarkt_export.xlsx

# 3. If the sample looks good, run the full crawl (resumable — Ctrl-C anytime, rerun to continue)
python agenturmarkt_scraper.py scrape

# Progress check anytime:
python agenturmarkt_scraper.py status
```

Full crawl at the default 1 req/sec: ~4,050 listing pages + ~40,480 detail pages ≈ **12.5 h**. Run overnight or in chunks; it resumes where it stopped.

## Options

| Flag | Purpose |
|---|---|
| `--limit N` | stop after N agencies (sampling) |
| `--category seo-agentur` | only one category |
| `--city berlin` | only one city |
| `--delay 0.5` | faster (don't go below ~0.5s) |
| `--no-details` | slugs + listing data only |
| `--db path.db` | alternate database file |

## Collected fields

name, slogan, score, review_count, city, state, categories, services (Dienstleistungen), industries (Branchen), website, email (from listing cards), founded, employees, address, verified, description.

**Phone:** not collected — the site still serves placeholders (`+49 000 00000`) to anonymous visitors. **E-mail:** listing cards contain the real address (HTML-masked); detail pages do not.

## Notes

- Parser is heading/structure-based (not CSS classes) plus JSON-LD fallback, so minor site redesigns won't break it. If fields come back empty on the sample run, the markup changed — send me a saved page and I'll adjust.
- Failed pages are marked `error` in the DB; reset them with `UPDATE agencies SET detail_status='pending' WHERE detail_status='error';` and rerun.
- Legal: agenturmarkt.de's AGB, EU database rights (§87a UrhG) and GDPR may constrain bulk use of this data — check before commercial redistribution. (Not legal advice.)
