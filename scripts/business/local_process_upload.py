"""
Helper script to prepare a CSV/XLSX for the `upload_processor` Cloud Function.

Features:
- Choose environment (dev/prod) – affects suggested GCS path.
- Specify the owner/user ID and campaign metadata.
- Validate that required headers are present (according to upload_processor logic).
- Ensure a `Template` column exists and, if requested, populate it from a templates directory.

Usage (example):

    python scripts/prepare_upload_file.py \
        --env dev \
        --owner-id SOME_USER_ID \
        --campaign-id test-campaign-1 \
        --campaign-code TEST1 \
        --campaign-name "Test Campaign 1" \
        --input-csv path/to/input.csv \
        --output-csv path/to/input_prepared.csv \
        --templates-dir path/to/templates
"""

import argparse
import csv
import json
import mimetypes
import os
import uuid
from pathlib import Path
from typing import Dict, List, Tuple

from google.cloud import storage


def _detect_delimiter(sample: str) -> str:
    """
    Detect CSV delimiter by choosing the character that yields the most
    columns on the first non-empty line. This is robust for semicolon-
    separated exports where fields may contain many commas (e.g. JSON).
    """
    if not sample:
        return ","

    first_line = ""
    for line in sample.splitlines():
        if line.strip():
            first_line = line
            break

    if not first_line:
        return ","

    best_delimiter = ","
    best_count = 1
    for delim in (";", "\t", ",", "|"):
        count = len(first_line.split(delim))
        if count > best_count:
            best_count = count
            best_delimiter = delim

    return best_delimiter


def _read_rows(path: Path) -> Tuple[List[Dict[str, str]], List[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        delimiter = _detect_delimiter(sample)
        reader = csv.DictReader(f, delimiter=delimiter, restkey="_extra", restval="")
        rows: List[Dict[str, str]] = []
        for r in reader:
            r = {(k if isinstance(k, str) else str(k)): v for k, v in r.items()}
            r.pop("_extra", None)
            rows.append(r)
        headers = list(reader.fieldnames or [])
    return rows, headers


def _write_rows(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def _find_header(headers: List[str], *candidates: str) -> str | None:
    lower_map = {h.lower(): h for h in headers if isinstance(h, str)}
    for cand in candidates:
        h = lower_map.get(cand.lower())
        if h is not None:
            return h
    return None


def _validate_headers(headers: List[str]) -> None:
    """
    Validate that the CSV has all required logical headers, allowing the same
    variants that `upload_processor` accepts via `get_ci` / `get_ci_key`.

    Uses case-insensitive matching and alias sets instead of requiring a single
    exact header name.
    """
    # Detect new list format via presence of `company_name` header
    is_new_format = _find_header(headers, "company_name") is not None

    if is_new_format:
        # Relaxed validation for new list format while still ensuring
        # core fields needed by upload_processor are present.
        required_groups = [
            ("company_name", ["company_name"]),
            ("Straße", ["Straße", "Strasse", "Str", "Str.", "street", "Street"]),
            ("PLZ", ["PLZ", "Postleitzahl", "postcode", "Postcode"]),
            ("Ort", ["Ort", "Stadt", "City"]),
            ("E-Mail", ["E-Mail", "E-Mail-Adresse", "Email", "Mail", "e-mail-adresse"]),
            ("Template", ["Template", "template"]),
        ]

        missing_logical: List[str] = []
        for logical_name, variants in required_groups:
            # If any variant is present (case-insensitive), we're good
            if _find_header(headers, *variants) is None:
                missing_logical.append(
                    f"{logical_name} (expected one of: {', '.join(variants)})"
                )

        if missing_logical:
            msg = (
                "❌ Input CSV (new list format) is missing required headers (by logical field):\n  - "
                + "\n  - ".join(missing_logical)
                + "\n\nPresent headers:\n  - "
                + "\n  - ".join(headers)
            )
            raise SystemExit(msg)

        # Optional analytics/business fields – warn if missing but do not fail.
        optional_new_cols = [
            "Gegenstand",
            "Umsatz EUR",
            "Branche (NACE)",
        ]
        missing_optional = [
            col for col in optional_new_cols if _find_header(headers, col) is None
        ]
        if missing_optional:
            print(
                "WARNING: Optional columns missing for new list format (no hard error): "
                + ", ".join(missing_optional)
            )

        return

    # Legacy format validation (existing behavior)
    # Each tuple: (logical name for error messages, list of accepted header variants)
    required_groups = [
        ("Anrede", ["Anrede"]),
        ("Namenszeile", ["Namenszeile"]),
        ("Namenszeile 1", ["Namenszeile 1"]),
        ("Namenszeile 2", ["Namenszeile 2"]),
        ("Namenszeile 3", ["Namenszeile 3"]),
        ("PLZ", ["PLZ", "Postleitzahl"]),
        ("Ort", ["Ort", "Stadt", "City"]),
        ("Ortsteil", ["Ortsteil"]),
        ("Straße", ["Straße", "Strasse", "Str", "Str."]),
        ("Hausnummer", ["Hausnummer", "HNr", "Hnr", "Nr"]),
        ("Branchencode WZ", ["Branchencode WZ"]),
        ("Branchenname WZ", ["Branchenname WZ"]),
        ("Dachmarkt WZ", ["Dachmarkt WZ"]),
        ("Bundesland", ["Bundesland"]),
        # Phone / email groups mirror get_ci usage in upload_processor
        ("Vorwahl Telefon", ["Vorwahl Telefon", "Vorwahl", "Telefon Vorwahl", "vorwahl_telefon"]),
        ("Telefonnummer", ["Telefonnummer", "Telefon", "Phone", "telefonnummer"]),
        ("E-Mail-Adresse", ["E-Mail-Adresse", "Email", "E-Mail", "Mail", "e-mail-adresse"]),
        ("Entscheider 1 Anrede", ["Entscheider 1 Anrede", "Salutation"]),
        ("Entscheider 1 Titel", ["Entscheider 1 Titel"]),
        ("Entscheider 1 Vorname", ["Entscheider 1 Vorname", "Vorname", "Anrede Vorname"]),
        ("Entscheider 1 Nachname", ["Entscheider 1 Nachname", "Nachname"]),
        ("Entscheider 1 Funktionsnummer", ["Entscheider 1 Funktionsnummer"]),
        ("Entscheider 1 Funktionsname", ["Entscheider 1 Funktionsname"]),
        # Template column (case-insensitive)
        ("Template", ["Template", "template"]),
    ]

    missing_logical: List[str] = []
    for logical_name, variants in required_groups:
        # If any variant is present (case-insensitive), we're good
        if _find_header(headers, *variants) is None:
            missing_logical.append(
                f"{logical_name} (expected one of: {', '.join(variants)})"
            )

    if missing_logical:
        msg = (
            "❌ Input CSV is missing required headers (by logical field):\n  - "
            + "\n  - ".join(missing_logical)
            + "\n\nPresent headers:\n  - "
            + "\n  - ".join(headers)
        )
        raise SystemExit(msg)


def _collect_templates(templates_dir: Path) -> List[str]:
    """
    Collect template filenames from the given directory.
    Currently loads all *.pdf files, sorted alphabetically.
    """
    if not templates_dir.exists():
        raise SystemExit(f"❌ Templates directory does not exist: {templates_dir}")
    if not templates_dir.is_dir():
        raise SystemExit(f"❌ Templates path is not a directory: {templates_dir}")

    pdfs = sorted(p.name for p in templates_dir.glob("*.pdf"))
    return pdfs


def _upload_to_gcs(
    bucket_name: str,
    object_name: str,
    local_path: Path,
    manifest: Dict[str, object],
) -> Tuple[str, str]:
    """
    Upload the CSV file and a sibling manifest.json to the given GCS bucket.
    Returns (csv_uri, manifest_uri).
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Upload CSV/XLSX
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(local_path))

    # Upload manifest.json next to the object
    prefix_dir = os.path.dirname(object_name)
    manifest_blob_name = f"{prefix_dir}/manifest.json"
    manifest_blob = bucket.blob(manifest_blob_name)
    manifest_blob.upload_from_string(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        content_type="application/json",
    )

    csv_uri = f"gs://{bucket_name}/{object_name}"
    manifest_uri = f"gs://{bucket_name}/{manifest_blob_name}"
    print(f"[DEBUG _upload_to_gcs] csv_uri: {csv_uri}")
    print(f"[DEBUG _upload_to_gcs] manifest_uri: {manifest_uri}")
    return csv_uri, manifest_uri


def _upload_templates_to_gcs(
    bucket_name: str,
    campaign_root_prefix: str,
    templates_dir: Path,
) -> List[str]:
    """
    Upload all *.pdf templates to a sibling /templates/ folder next to /source/.

    Example:
      campaign_root_prefix = "uploads/dev/<uid>/<campaignId>"
      templates will go under ".../templates/<filename>.pdf".
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    uploaded_uris: List[str] = []

    for pdf in sorted(templates_dir.glob("*.pdf")):
        dest_name = f"{campaign_root_prefix}/templates/{pdf.name}"
        blob = bucket.blob(dest_name)
        blob.upload_from_filename(str(pdf))
        uploaded_uris.append(f"gs://{bucket_name}/{dest_name}")
        print(f"[DEBUG _upload_templates_to_gcs] uploaded_uris: {uploaded_uris}")
    return uploaded_uris


def prepare_file(
    input_csv: Path,
    output_csv: Path | None,
    templates_dir: Path | None,
) -> Dict[str, object]:
    rows, headers = _read_rows(input_csv)

    if not rows:
        raise SystemExit(f"❌ Input file has no data rows: {input_csv}")

    # Validate headers strictly (raises on error)
    _validate_headers(headers)

    # Template header must exist (exact name, but allow case-insensitive alias)
    template_header = _find_header(headers, "template", "Template")
    if template_header is None:
        raise SystemExit("❌ Required header 'Template' is missing.")

    # Templates directory is required so we can validate against the folder
    if templates_dir is None:
        raise SystemExit("❌ --templates-dir is required so templates can be validated against the folder.")

    template_files: List[str] = _collect_templates(templates_dir)
    if not template_files:
        raise SystemExit(f"❌ No *.pdf templates found in {templates_dir}")

    # Determine existing template values in the CSV
    existing_templates = [str((row.get(template_header) or "")).strip() for row in rows]
    unique_existing = sorted({t for t in existing_templates if t})

    # Enforce that every row already has a template value in the input
    empty_row_indices = [i for i, t in enumerate(existing_templates, start=1) if t == ""]
    if empty_row_indices:
        max_show = 50
        if len(empty_row_indices) <= max_show:
            rows_msg = ", ".join(str(i) for i in empty_row_indices)
        else:
            rows_msg = ", ".join(str(i) for i in empty_row_indices[:max_show]) + f" ... and {len(empty_row_indices) - max_show} more"
        raise SystemExit(
            f"❌ Template column contains empty values; every row must have a template before upload.\n"
            f"   Rows missing a template (data row number): {rows_msg}\n"
            f"   Total: {len(empty_row_indices)} of {len(rows)} rows."
        )

    # Validate that all template values in the CSV exist as PDFs in the templates folder
    folder_set = set(template_files)
    csv_set = set(unique_existing)

    # 1) Every template used in the CSV must have a corresponding PDF file
    missing_in_folder = sorted(csv_set - folder_set)
    if missing_in_folder:
        raise SystemExit(
            "❌ Template values in CSV do not match templates in the folder.\n"
            "   Missing in folder (no matching .pdf):\n  - "
            + "\n  - ".join(missing_in_folder)
        )

    # 2) Every template PDF in the folder must be referenced at least once in the CSV.
    #    This ensures that the set of templates in the CSV matches exactly the set
    #    of templates that will be uploaded.
    unused_in_csv = sorted(folder_set - csv_set)
    if unused_in_csv:
        raise SystemExit(
            "❌ Some template PDF files in the templates directory are not referenced in the CSV.\n"
            "   Unused template files:\n  - "
            + "\n  - ".join(unused_in_csv)
        )

    # After assignment / validation, enforce non-empty templates
    for i, row in enumerate(rows, start=1):
        val = str((row.get(template_header) or "")).strip()
        if not val:
            raise SystemExit(f"❌ Row {i} has an empty Template value after processing.")

    # Optionally write an output file; if no output path is provided,
    # we only validate in-place and leave the original file untouched.
    if output_csv is not None:
        _write_rows(output_csv, rows, headers)

    return {
        "input": str(input_csv),
        "output": str(output_csv) if output_csv is not None else str(input_csv),
        "rows": len(rows),
        "headers": headers,
        "detected_headers": headers,
        "template_header": template_header,
        "templates_dir": str(templates_dir) if templates_dir is not None else None,
        "template_files": template_files,
        "template_keys": unique_existing,
        "warnings": [],
    }


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prepare a CSV for the upload_processor Cloud Function.",
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Target environment (affects suggested GCS path). Default: dev",
    )
    parser.add_argument(
        "--owner-id",
        required=True,
        help="Owner/user ID that will be used in manifest/metadata (same as front-end ownerId).",
    )
    parser.add_argument(
        "--campaign-id",
        required=False,
        help=(
            "Optional campaign ID for naming paths/metadata. "
            "If omitted, a random UUID will be generated (similar to the frontend)."
        ),
    )
    parser.add_argument(
        "--campaign-code",
        required=True,
        help="Campaign code (will be used as tracking prefix in upload_processor).",
    )
    parser.add_argument(
        "--campaign-name",
        default=None,
        help="Optional human-readable campaign name.",
    )
    parser.add_argument(
        "--input-csv",
        required=True,
        type=Path,
        help="Path to input CSV file.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        help="Optional path to write a prepared CSV file. If omitted, the script only validates the input file without creating a new one.",
    )
    parser.add_argument(
        "--templates-dir",
        type=Path,
        required=True,
        help="Directory containing PDF templates to assign to rows (required; used for validation).",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help=(
            "Optional GCS bucket name to upload to when using --upload. "
            "If omitted, the bucket is chosen automatically based on --env: "
            "dev -> gb-qr-tracker-dev.firebasestorage.app, "
            "prod -> gb-qr-tracker.firebasestorage.app."
        ),
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="If set, upload the CSV and manifest.json to GCS after validation.",
    )
    parser.add_argument(
        "--destination",
        required=True,
        help="Destination URL to store in manifest (used by upload_processor).",
    )
    parser.add_argument(
        "--search-group-id",
        required=False,
        help=(
            "Optional search_group document ID under customers/{ownerId}/search_groups "
            "that should be linked to the created campaign (its searches will also be linked)."
        ),
    )

    args = parser.parse_args(argv)

    input_csv: Path = args.input_csv
    if not input_csv.exists():
        raise SystemExit(f"❌ Input CSV does not exist: {input_csv}")

    # If no explicit output file is given, we run in validation-only mode
    # and do not create a new CSV. The original file is left unchanged.
    output_csv: Path | None = args.output_csv

    result = prepare_file(
        input_csv=input_csv,
        output_csv=output_csv,
        templates_dir=args.templates_dir,
    )

    env = args.env
    owner_id = args.owner_id
    search_group_id: str | None = args.search_group_id
    # If no campaign_id is provided, generate a UUID v4 similar to frontend crypto.randomUUID()
    campaign_id = args.campaign_id or str(uuid.uuid4())
    campaign_root_prefix = f"uploads/{env}/{owner_id}/{campaign_id}"
    campaign_code = args.campaign_code
    campaign_name = args.campaign_name or "(none)"
    # Auto-select bucket by environment unless explicitly overridden
    if args.bucket:
        bucket_name = args.bucket
    else:
        if env == "dev":
            bucket_name = "gb-qr-tracker-dev.firebasestorage.app"
        else:
            bucket_name = "gb-qr-tracker.firebasestorage.app"
    upload_flag = args.upload
    destination = args.destination

    detected_headers: List[str] = result.get("detected_headers", [])
    template_keys: List[str] = result.get("template_keys", [])

    # Always use the effective campaign_id in the suggested prefix
    suggested_prefix = f"{campaign_root_prefix}/source"
    upload_path = output_csv or input_csv
    file_name_for_upload = upload_path.name
    suggested_object = f"{suggested_prefix}/{file_name_for_upload}"

    # If requested, upload the file and manifest.json to GCS now
    csv_uri = None
    manifest_uri = None
    if upload_flag:
        object_name = f"{campaign_root_prefix}/source/{file_name_for_upload}"

        # Derive content type similar to frontend guessMime
        mime_type, _ = mimetypes.guess_type(file_name_for_upload)
        content_type = mime_type or "text/csv"

        # Base URL depends on environment (prod/dev)
        if env == "prod":
            base_url = "https://europe-west3-gb-qr-tracker.cloudfunctions.net/redirector"
        else:
            base_url = "https://europe-west3-gb-qr-tracker-dev.cloudfunctions.net/redirector"

        file_size = upload_path.stat().st_size

        manifest: Dict[str, object] = {
            "env": env,
            "ownerId": owner_id,
            "campaignId": campaign_id,
            "base_url": base_url,
            "destination": destination,
            "campaign_code": campaign_code,
            "campaign_name": campaign_name,
            "campaign_code_from_business": True,
            "list": {
                "path": object_name,
                "name": file_name_for_upload,
                "contentType": content_type,
                "size": file_size,
            },
            "templateColumn": "Template",
            "templateKeys": template_keys,
            "detectedHeaders": detected_headers,
            # Reasonable defaults matching upload_processor expectations
            "limit": 0,
            "skip_existing": True,
            "geocode": False,
        }

        # Optionally pass through search group metadata so upload_processor
        # can link the search_group + its searches to this campaign.
        if search_group_id:
            manifest["search_group"] = {
                "id": search_group_id,
            }

        csv_uri, manifest_uri = _upload_to_gcs(
            bucket_name=bucket_name,
            object_name=object_name,
            local_path=upload_path,
            manifest=manifest,
        )
        # Upload templates into a sibling /templates/ folder
        template_uris = _upload_templates_to_gcs(
            bucket_name=bucket_name,
            campaign_root_prefix=campaign_root_prefix,
            templates_dir=args.templates_dir,
        )

    print("=" * 80)
    print("UPLOAD PREPARATION SUMMARY")
    print("=" * 80)
    print(f"Environment:       {env}")
    print(f"Owner ID:          {owner_id}")
    print(f"Campaign ID:       {campaign_id}")
    print(f"Campaign code:     {campaign_code}")
    print(f"Campaign name:     {campaign_name}")
    print("-" * 80)
    print(f"Input CSV:         {result['input']}")
    print(f"Prepared CSV:      {result['output']}")
    print(f"Row count:         {result['rows']}")
    print(f"Headers:           {', '.join(result['headers'])}")
    print(f"Template header:   {result['template_header']}")

    if result["templates_dir"]:
        print(f"Templates dir:     {result['templates_dir']}")
        print(f"Templates found:   {len(result['template_files'])}")
    else:
        print("Templates dir:     (none)")

    if upload_flag and csv_uri and manifest_uri:
        print("-" * 80)
        print("UPLOAD TO GCS COMPLETED")
        print(f"CSV object:        {csv_uri}")
        print(f"Manifest object:   {manifest_uri}")
        if template_uris:
            print("Template objects:")
            for uri in template_uris:
                print(f"  - {uri}")

    if result["warnings"]:
        print("-" * 80)
        print("WARNINGS:")
        for w in result["warnings"]:
            print(f"- {w}")

    print("-" * 80)
    print("Next steps (example):")
    if upload_flag:
        print("1) File and manifest have been uploaded; monitor upload_processor logs and output.")
        print("2) You can inspect the objects in the bucket at the URIs shown above.")
    else:
        print("1) Upload the prepared file to your bucket, e.g.:")
        print(f"   gsutil cp {upload_path} gs://{bucket_name}/{suggested_object}")
        print("2) Ensure a matching manifest.json exists next to the file or set metadata:")
        print("   - ownerId       ->", owner_id)
        print("   - campaignId    ->", campaign_id)
        print("   - campaign_code ->", campaign_code)
        print("   - campaign_name ->", campaign_name)
        print("3) Trigger upload_processor (via GCS finalize event).")
    print("=" * 80)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

