# Letter PDFs for Rocket Letter mailings

Generates print-ready letters (A4, DIN 5008 Form B for DL window envelopes) from the recipient CSV:
letterhead, return line, address window, personal QR code + link, general copy and the individual paragraph.

## Setup

```bash
pip install -r requirements.txt
```

## Files

| File | Purpose |
|---|---|
| `generate_letters.py` | the generator |
| `config.yaml` | sender, footer, signatures, QR, fonts, layout, CSV column mapping |
| `letter_template.md` | the letter copy (edit freely; placeholders in `{braces}`) |
| `assets/` | put logo and signature PNGs here and reference them in `config.yaml` |

## Usage

```bash
# Proof print: first 5 letters, plus one PDF per letter
python generate_letters.py ../013-rl-dmc_datamerge_final.csv --limit 5 --single

# Full run
python generate_letters.py ../013-rl-dmc_datamerge_final.csv
```

Input is the data-merge CSV from `merge_paragraphs.py` (comma or semicolon both work).

Output in `output/`:
- `letters_all.pdf`: everything in one file, in list order. With `duplex_padding: true`, every letter is padded to an even page count, so duplex printing never puts letter B on the back of letter A.
- `single/`: one PDF per letter (only with `--single`).
- `report.csv`: status per recipient (ok / skipped + reason), pages, font size used, QR slug.
- `qr_links.csv`: slug → URL. **Create these links in Rocket Letter before printing**, otherwise the QR codes lead nowhere.

## Behaviour

- **Fit to one page:** starts at `font_size` (10 pt) and shrinks in 0.25-pt steps down to `font_size_min` (9 pt). If it still doesn't fit, the letter gets 2 pages, as reported in `report.csv`.
- **QR slug:** taken from column `qr_slug` if present. Otherwise the last name is used. When a last name appears more than once, the slug becomes `lastname-firstname` (e.g. `weber-lucas`, `weber-mario`). Slugs are computed on the full list, so `--limit`/`--rows` don't change them.
- **Address window:** company (legal name if ≤ 42 chars, else the brand name), salutation + name, c/o line (detected from `full_address`), street, postcode + city. Long lines shrink to fit 80 mm.
- **No salutation** (e.g. unknown gender): the address shows the name without Herr/Frau; the letter still starts with "Hallo {first_name},".
- **No contact person:** the greeting falls back to `greeting_without_name` ("Hallo zusammen,").
- **Skipped:** recipients without a paragraph (unless `missing_paragraph: fallback`), with an incomplete address, or with a non-German postcode.

## Brand design

Colours and fonts come from rocket-letter.de and live in `config.yaml` under `brand`:
navy `#152842` text, cyan `#00B0CA` accents, dark cyan `#008198` for the QR link, grey `#62657C` secondary text.
Headline, letterhead, QR box and footer use **Sora**, the body text uses **Source Serif 4**
(`body_font: sans` switches the body to Sora). Both fonts are bundled in `assets/fonts/` (SIL Open Font License).

## Assets to add

| File | Used for |
|---|---|
| `assets/logo.svg` (or `.png` / `.jpg`, then change `sender.logo`) | letterhead, top left, 11 mm high |
| `assets/signature_benjamin.png`, `assets/signature_marcel.png` | signatures above "Benjamin & Marcel Gleich" (transparent PNG, ~600 px wide) |

Missing files are fine: the letterhead falls back to "Rocket Letter" as text, and the signature space stays empty.

## Before the first print

1. Add logo and signatures (see above).
2. Print one page at 100 % (no "fit to page") and hold it behind a DL window envelope to check the address position.
