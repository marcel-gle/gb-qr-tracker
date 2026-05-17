"""
Normalize raw business CSV files for scripts/business/local_process_upload.py.

This script:
- keeps only headers expected by local_process_upload (legacy format),
- maps common source header aliases to target headers,
- formats first/last names and address fields,
- infers salutation (Herr/Frau) where possible,
- reports missing required source fields and missing required output values.

Usage:
  python scripts/business/normalize_for_local_process_upload.py \
    --input-csv raw.csv \
    --output-csv normalized.csv \
    --default-template standard.pdf
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency at runtime
    OpenAI = None  # type: ignore[assignment]

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - optional dependency at runtime
    tqdm = None  # type: ignore[assignment]


TARGET_HEADERS = [
    "Anrede",
    "Namenszeile",
    "Namenszeile 1",
    "Namenszeile 2",
    "Namenszeile 3",
    "PLZ",
    "Ort",
    "Ortsteil",
    "Straße",
    "Hausnummer",
    "Branchencode WZ",
    "Branchenname WZ",
    "Dachmarkt WZ",
    "Bundesland",
    "Vorwahl Telefon",
    "Telefonnummer",
    "E-Mail-Adresse",
    "Entscheider 1 Anrede",
    "Entscheider 1 Titel",
    "Entscheider 1 Vorname",
    "Entscheider 1 Nachname",
    "Entscheider 1 Funktionsnummer",
    "Entscheider 1 Funktionsname",
    "Template",
]

# Target fields that should contain values for a useful upload.
REQUIRED_OUTPUT_VALUES = [
    "PLZ",
    "Ort",
    "Straße",
    "Hausnummer",
    "E-Mail-Adresse",
    "Template",
]

SOURCE_ALIASES: Dict[str, List[str]] = {
    "company_name": ["company_name", "firma", "unternehmen", "company", "name"],
    "salutation": ["anrede", "salutation", "gender", "geschlecht"],
    "title": ["titel", "title"],
    "first_name": ["vorname", "first_name", "firstname", "ansprechpartner_vorname"],
    "last_name": ["nachname", "last_name", "lastname", "ansprechpartner_nachname"],
    "full_name": ["name_kontakt", "kontaktname", "full_name", "contact_name"],
    "zip": ["plz", "postleitzahl", "zip", "postcode"],
    "city": ["ort", "stadt", "city"],
    "district": ["ortsteil", "district", "stadtteil"],
    "street": ["straße", "strasse", "str", "str.", "street"],
    "house_number": ["hausnummer", "hnr", "hnr.", "nr"],
    "address": ["adresse", "address", "anschrift", "street_address"],
    "state": ["bundesland", "state"],
    "phone_prefix": ["vorwahl telefon", "vorwahl", "telefon vorwahl", "phone_prefix"],
    "phone": ["telefonnummer", "telefon", "phone", "mobil", "handy"],
    "email": ["e-mail", "e-mail-adresse", "e-mail adresse", "email", "mail"],
    "function_number": [
        "entscheider 1 funktionsnummer",
        "funktionsnummer",
        "position_nummer",
    ],
    "function_name": ["entscheider 1 funktionsname", "funktionsname", "funktion", "position"],
    "template": ["template", "vorlage"],
    "wz_code": ["branchencode wz", "wz", "wz_code", "nace"],
    "wz_name": ["branchenname wz", "wz_name", "branche", "industry"],
    "roof_market": ["dachmarkt wz", "dachmarkt"],
}

FEMALE_FIRST_NAMES = {
    "anna",
    "maria",
    "julia",
    "sarah",
    "laura",
    "sophie",
    "eva",
    "lea",
}

MALE_FIRST_NAMES = {
    "max",
    "maximilian",
    "thomas",
    "sebastian",
    "jonas",
    "alexander",
    "daniel",
    "michael",
}


def _normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower()).strip()


def _detect_delimiter(sample: str) -> str:
    if not sample:
        return ","
    first_line = ""
    for line in sample.splitlines():
        if line.strip():
            first_line = line
            break
    if not first_line:
        return ","
    best = ","
    best_count = 1
    for delim in (";", "\t", ",", "|"):
        count = len(first_line.split(delim))
        if count > best_count:
            best = delim
            best_count = count
    return best


def _read_rows(path: Path) -> Tuple[List[Dict[str, str]], List[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(4096)
        handle.seek(0)
        delimiter = _detect_delimiter(sample)
        reader = csv.DictReader(handle, delimiter=delimiter, restval="")
        rows = [{(k or "").strip(): (v or "").strip() for k, v in row.items()} for row in reader]
        return rows, list(reader.fieldnames or [])


def _write_rows(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        iterable = _progress_iter(
            rows,
            desc="Writing normalized rows",
            total=len(rows),
        )
        for row in iterable:
            writer.writerow(row)


def _source_lookup(headers: List[str]) -> Dict[str, str]:
    by_normalized = {_normalize_header(h): h for h in headers}
    resolved: Dict[str, str] = {}
    for logical_name, aliases in SOURCE_ALIASES.items():
        for alias in aliases:
            found = by_normalized.get(_normalize_header(alias))
            if found:
                resolved[logical_name] = found
                break
    return resolved


def _title_case_name(value: str) -> str:
    value = re.sub(r"\s+", " ", value.strip())
    if not value:
        return ""
    parts = re.split(r"([ -])", value.lower())
    return "".join(p.capitalize() if p not in {" ", "-"} else p for p in parts)


def _split_full_name(value: str) -> Tuple[str, str]:
    value = re.sub(r"\s+", " ", value.strip())
    if not value:
        return "", ""
    parts = value.split(" ")
    if len(parts) == 1:
        return _title_case_name(parts[0]), ""
    return _title_case_name(parts[0]), _title_case_name(" ".join(parts[1:]))


def _normalize_salutation(raw: str, first_name: str) -> str:
    value = (raw or "").strip().lower()
    if any(token in value for token in ("frau", "ms", "mrs", "weiblich", "female")):
        return "Frau"
    if any(token in value for token in ("herr", "mr", "männlich", "male")):
        return "Herr"

    first = first_name.lower().strip()
    if first in FEMALE_FIRST_NAMES:
        return "Frau"
    if first in MALE_FIRST_NAMES:
        return "Herr"
    return ""


def _split_street_and_number(street: str, house_number: str, full_address: str) -> Tuple[str, str]:
    if street and house_number:
        return street.strip(), house_number.strip()
    if not street and full_address:
        street = full_address.strip()

    match = re.match(r"^(.*?)[,\s]+(\d+[a-zA-Z]?(?:[-/]\d+[a-zA-Z]?)?)$", street.strip())
    if match:
        return match.group(1).strip(), house_number.strip() or match.group(2).strip()
    return street.strip(), house_number.strip()


def _value(row: Dict[str, str], mapping: Dict[str, str], logical_name: str) -> str:
    header = mapping.get(logical_name)
    if not header:
        return ""
    return (row.get(header) or "").strip()


def _progress_iter(items: List[Dict[str, str]], desc: str, total: int):
    if tqdm is not None:
        return tqdm(items, total=total, desc=desc, unit="row")
    return items


class LMStudioFormatter:
    def __init__(self, enabled: bool, base_url: str, model: str) -> None:
        self.enabled = enabled and OpenAI is not None
        self.model = model
        self.errors = 0
        self.calls = 0
        if self.enabled:
            self.client = OpenAI(base_url=base_url, api_key="not-needed")
        else:
            self.client = None

    def _extract_json(self, text: str) -> Dict[str, str] | None:
        raw = (text or "").strip()
        if not raw:
            return None
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return {str(k): str(v) for k, v in parsed.items() if v is not None}
        except json.JSONDecodeError:
            pass
        match = re.search(r"\{.*\}", raw, flags=re.S)
        if not match:
            return None
        try:
            parsed = json.loads(match.group(0))
            if isinstance(parsed, dict):
                return {str(k): str(v) for k, v in parsed.items() if v is not None}
        except json.JSONDecodeError:
            return None
        return None

    def format_contact(self, payload: Dict[str, str]) -> Dict[str, str] | None:
        if not self.enabled or self.client is None:
            return None

        system_prompt = (
            "Du normalisierst deutsche Kontaktdaten fuer Briefversand. "
            "Antworte nur mit einem JSON-Objekt ohne Erklaerung."
        )
        user_prompt = (
            "Bereinige Name, Adresse und leite die Anrede ab.\n"
            "Regeln:\n"
            "- anrede muss genau 'Herr', 'Frau' oder '' sein.\n"
            "- vorname/nachname/titel in korrekter Schreibweise.\n"
            "- strasse ohne hausnummer, hausnummer separat.\n"
            "- ort/ortsteil/bundesland sauber formatiert.\n"
            "- Wenn unklar, Feld leer lassen.\n"
            "Gib exakt dieses JSON-Schema zurueck:\n"
            '{"anrede":"","titel":"","vorname":"","nachname":"","strasse":"","hausnummer":"","ort":"","ortsteil":"","bundesland":""}\n'
            f"Eingabe:\n{json.dumps(payload, ensure_ascii=False)}"
        )

        try:
            self.calls += 1
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0,
                max_tokens=220,
            )
            content = resp.choices[0].message.content or ""
            return self._extract_json(content)
        except Exception:
            self.errors += 1
            return None


def normalize_rows(
    rows: List[Dict[str, str]],
    source_map: Dict[str, str],
    default_template: str,
    llm_formatter: LMStudioFormatter | None = None,
) -> Tuple[List[Dict[str, str]], Dict[str, int], int]:
    normalized_rows: List[Dict[str, str]] = []
    missing_output_value_counts = {key: 0 for key in REQUIRED_OUTPUT_VALUES}
    llm_applied_rows = 0

    for row in _progress_iter(rows, desc="Normalizing rows", total=len(rows)):
        first_name = _title_case_name(_value(row, source_map, "first_name"))
        last_name = _title_case_name(_value(row, source_map, "last_name"))

        if not first_name and not last_name:
            split_first, split_last = _split_full_name(_value(row, source_map, "full_name"))
            first_name = split_first
            last_name = split_last

        title = _title_case_name(_value(row, source_map, "title"))
        salutation = _normalize_salutation(_value(row, source_map, "salutation"), first_name)
        company_name = _value(row, source_map, "company_name")

        street, house_number = _split_street_and_number(
            _value(row, source_map, "street"),
            _value(row, source_map, "house_number"),
            _value(row, source_map, "address"),
        )

        phone_prefix = _value(row, source_map, "phone_prefix")
        phone = _value(row, source_map, "phone")
        if phone and not phone_prefix:
            compact = re.sub(r"[^\d+]", "", phone)
            if compact.startswith("+49"):
                phone_prefix = "+49"
                phone = compact[3:].strip()

        email = _value(row, source_map, "email").lower()
        template = _value(row, source_map, "template") or default_template

        if llm_formatter is not None:
            llm_input = {
                "anrede": _value(row, source_map, "salutation"),
                "titel": title,
                "vorname": first_name,
                "nachname": last_name,
                "full_name": _value(row, source_map, "full_name"),
                "strasse": street,
                "hausnummer": house_number,
                "adresse": _value(row, source_map, "address"),
                "ort": _value(row, source_map, "city"),
                "ortsteil": _value(row, source_map, "district"),
                "bundesland": _value(row, source_map, "state"),
            }
            llm_out = llm_formatter.format_contact(llm_input)
            if llm_out:
                llm_applied_rows += 1
                title = _title_case_name(llm_out.get("titel", title) or title)
                first_name = _title_case_name(llm_out.get("vorname", first_name) or first_name)
                last_name = _title_case_name(llm_out.get("nachname", last_name) or last_name)
                salutation = _normalize_salutation(
                    llm_out.get("anrede", salutation) or salutation,
                    first_name,
                )
                street = _title_case_name(llm_out.get("strasse", street) or street)
                house_number = (llm_out.get("hausnummer", house_number) or house_number).strip()

                llm_city = _title_case_name(llm_out.get("ort", "") or "")
                llm_district = _title_case_name(llm_out.get("ortsteil", "") or "")
                llm_state = _title_case_name(llm_out.get("bundesland", "") or "")
            else:
                llm_city = ""
                llm_district = ""
                llm_state = ""
        else:
            llm_city = ""
            llm_district = ""
            llm_state = ""

        decision_line = " ".join(p for p in [salutation, title, first_name, last_name] if p).strip()

        out = {
            "Anrede": salutation,
            "Namenszeile": decision_line or company_name,
            "Namenszeile 1": company_name,
            "Namenszeile 2": decision_line,
            "Namenszeile 3": "",
            "PLZ": _value(row, source_map, "zip"),
            "Ort": llm_city or _title_case_name(_value(row, source_map, "city")),
            "Ortsteil": llm_district or _title_case_name(_value(row, source_map, "district")),
            "Straße": _title_case_name(street),
            "Hausnummer": house_number,
            "Branchencode WZ": _value(row, source_map, "wz_code"),
            "Branchenname WZ": _value(row, source_map, "wz_name"),
            "Dachmarkt WZ": _value(row, source_map, "roof_market"),
            "Bundesland": llm_state or _title_case_name(_value(row, source_map, "state")),
            "Vorwahl Telefon": phone_prefix,
            "Telefonnummer": phone,
            "E-Mail-Adresse": email,
            "Entscheider 1 Anrede": salutation,
            "Entscheider 1 Titel": title,
            "Entscheider 1 Vorname": first_name,
            "Entscheider 1 Nachname": last_name,
            "Entscheider 1 Funktionsnummer": _value(row, source_map, "function_number"),
            "Entscheider 1 Funktionsname": _value(row, source_map, "function_name"),
            "Template": template,
        }

        for required in REQUIRED_OUTPUT_VALUES:
            if not (out.get(required) or "").strip():
                missing_output_value_counts[required] += 1

        normalized_rows.append(out)

    return normalized_rows, missing_output_value_counts, llm_applied_rows


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Normalize a raw CSV to local_process_upload-compatible headers.",
    )
    parser.add_argument("--input-csv", required=True, type=Path, help="Path to raw input CSV.")
    parser.add_argument("--output-csv", required=True, type=Path, help="Path for normalized CSV.")
    parser.add_argument(
        "--default-template",
        default="",
        help="Fallback Template value when source has no template column/value.",
    )
    parser.add_argument(
        "--no-lm-studio",
        action="store_true",
        help="Disable LM Studio formatting and use rules only.",
    )
    parser.add_argument(
        "--ml-studio-base-url",
        default=os.environ.get("ML_STUDIO_BASE_URL", "http://localhost:1234/v1"),
        help="LM Studio OpenAI-compatible base URL.",
    )
    parser.add_argument(
        "--local-model",
        default=os.environ.get("LOCAL_MODEL", "openai/gpt-oss-20b"),
        help="Model name served by local LM Studio.",
    )
    args = parser.parse_args()

    print("[1/5] Reading input CSV...")
    rows, headers = _read_rows(args.input_csv)
    if not rows:
        raise SystemExit(f"Input has no rows: {args.input_csv}")
    print(f"      Loaded {len(rows)} rows and {len(headers)} headers")

    print("[2/5] Mapping source headers...")
    source_map = _source_lookup(headers)
    print(f"      Resolved {len(source_map)} logical source fields")

    required_source_fields = {
        "zip": "PLZ/Postleitzahl",
        "city": "Ort/Stadt",
        "email": "E-Mail/Email",
        "template": "Template/Vorlage (or provide --default-template)",
    }
    missing_source_fields: List[str] = []
    for logical_key, description in required_source_fields.items():
        if logical_key == "template":
            if logical_key not in source_map and not args.default_template:
                missing_source_fields.append(description)
            continue
        if logical_key not in source_map:
            missing_source_fields.append(description)
    if "street" not in source_map and "address" not in source_map:
        missing_source_fields.append("Straße+Hausnummer or Adresse/Address")

    llm_formatter = LMStudioFormatter(
        enabled=not args.no_lm_studio,
        base_url=args.ml_studio_base_url,
        model=args.local_model,
    )
    if not args.no_lm_studio:
        print(f"[3/5] Using LM Studio model '{args.local_model}' at {args.ml_studio_base_url}")
    else:
        print("[3/5] LM Studio disabled, using rules only")

    print("[4/5] Normalizing rows...")
    normalized_rows, missing_value_counts, llm_applied_rows = normalize_rows(
        rows=rows,
        source_map=source_map,
        default_template=args.default_template.strip(),
        llm_formatter=llm_formatter,
    )
    print("[5/5] Writing output CSV...")
    _write_rows(args.output_csv, normalized_rows, TARGET_HEADERS)
    print("      Output write complete")

    print("=" * 80)
    print("NORMALIZATION SUMMARY")
    print("=" * 80)
    print(f"Input rows:                {len(rows)}")
    print(f"Source headers detected:   {len(headers)}")
    print(f"Output headers written:    {len(TARGET_HEADERS)}")
    print(f"Output file:               {args.output_csv}")
    print(f"LM Studio enabled:         {not args.no_lm_studio}")
    print(f"LM Studio model:           {args.local_model}")
    print(f"Rows formatted by LLM:     {llm_applied_rows}")
    print(f"LLM call errors:           {llm_formatter.errors}")
    print("-" * 80)
    print("Mapped source headers:")
    for logical_name in sorted(source_map):
        print(f"  - {logical_name:14s} -> {source_map[logical_name]}")

    if missing_source_fields:
        print("-" * 80)
        print("MISSING REQUIRED SOURCE FIELDS:")
        for item in missing_source_fields:
            print(f"  - {item}")
    else:
        print("-" * 80)
        print("All required source fields were detected.")

    print("-" * 80)
    print("MISSING REQUIRED OUTPUT VALUES (row-level):")
    for key in REQUIRED_OUTPUT_VALUES:
        print(f"  - {key:14s}: {missing_value_counts[key]} rows")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
