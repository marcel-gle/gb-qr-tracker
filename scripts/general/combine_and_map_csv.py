#!/usr/bin/env python3
"""
Combine multiple CSV files from a folder into one large file and map columns.

This script:
- Reads all CSV files from a specified folder
- Combines them into a single output CSV file
- Maps input columns to output columns according to predefined mapping
- Handles header variations (case-insensitive, whitespace-tolerant)
- Reports statistics about records and missing data
- Warns about unmapped headers

Usage:
    python scripts/combine_and_map_csv.py <input_folder> <output_file.csv>
"""

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set


# Column mapping: input header -> output header
# Multiple input headers can map to the same output header
# Note: Some input columns may need to populate multiple output columns
COLUMN_MAPPING: Dict[str, str] = {
    "Firma": "Namenszeile",
    "Postleitzahl": "PLZ",
    "Stadt": "Ort",
    "Straße": "Straße",
    "Branche": "Branchenname WZ",
    "Bundesland/Kanton": "Bundesland",
    "Telefonnummer": "Telefonnummer",
    "E-Mail (AP 1)": "E-Mail-Adresse",
    "E-Mail": "E-Mail-Adresse",
    "Anrede": "Anrede",
    "Anrede (AP 1)": "Entscheider 1 Anrede",  # Also populates "Anrede" if empty
    "Vorname (AP 1)": "Entscheider 1 Vorname",
    "Nachname (AP 1)": "Entscheider 1 Nachname",
}

# Output columns in the desired order
OUTPUT_COLUMNS = [
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


def normalize_header(header: str) -> str:
    """Normalize header for comparison: lowercase, strip whitespace."""
    return header.strip().lower()


def find_matching_header(input_header: str, mapping_keys: List[str]) -> Optional[str]:
    """
    Find matching header from mapping keys.
    Uses case-insensitive, whitespace-normalized comparison.
    """
    normalized_input = normalize_header(input_header)
    for mapping_key in mapping_keys:
        if normalize_header(mapping_key) == normalized_input:
            return mapping_key
    return None


def detect_delimiter(file_path: Path) -> str:
    """Detect CSV delimiter by reading first line."""
    with file_path.open("r", encoding="utf-8-sig", newline="") as f:
        first_line = f.readline()
        comma_count = first_line.count(",")
        semicolon_count = first_line.count(";")
        return ";" if semicolon_count >= comma_count else ","


def extract_hausnummer(street: str) -> tuple[str, str]:
    """
    Attempt to extract house number from street address.
    Returns: (street_without_number, hausnummer)
    """
    if not street:
        return "", ""
    
    street = street.strip()
    # Try to match pattern like "Musterstraße 123" or "Musterstraße 123a"
    match = re.search(r'\s+(\d+[a-zA-Z]?)$', street)
    if match:
        hausnummer = match.group(1)
        street_without_number = street[:match.start()].strip()
        return street_without_number, hausnummer
    
    return street, ""


def extract_vorwahl(telefon: str) -> tuple[str, str]:
    """
    Attempt to extract area code (Vorwahl) from phone number.
    Returns: (vorwahl, telefonnummer)
    """
    if not telefon:
        return "", ""
    
    telefon = telefon.strip()
    # Remove common separators
    telefon_clean = re.sub(r'[\s\-\(\)]', '', telefon)
    
    # German phone numbers: country code +49, then area code (2-5 digits), then number
    # Try to match patterns like +49 30 12345678 or 030 12345678 or 030/12345678
    match = re.match(r'^(\+?49\s*)?(0?\d{2,5})[/\s]?(\d+)$', telefon_clean)
    if match:
        vorwahl = match.group(2)
        nummer = match.group(3)
        return vorwahl, nummer
    
    # If no clear pattern, return as is
    return "", telefon


def process_csv_file(
    file_path: Path,
    all_unmapped_headers: Set[str],
) -> tuple[List[Dict[str, str]], Dict[str, int]]:
    """
    Process a single CSV file and return rows and statistics.
    
    Returns:
        (rows, stats) where rows is list of dicts and stats contains counts
    """
    delimiter = detect_delimiter(file_path)
    rows = []
    stats = {"rows_read": 0}
    
    with file_path.open("r", encoding="utf-8-sig", newline="") as infile:
        reader = csv.DictReader(infile, delimiter=delimiter)
        input_headers = reader.fieldnames or []
        
        # Build mapping from input headers to output headers
        input_to_output: Dict[str, str] = {}
        for input_header in input_headers:
            matching_key = find_matching_header(input_header, list(COLUMN_MAPPING.keys()))
            if matching_key:
                input_to_output[input_header] = COLUMN_MAPPING[matching_key]
            else:
                # Track unmapped headers
                all_unmapped_headers.add(input_header)
        
        for row in reader:
            stats["rows_read"] += 1
            
            # Create output row with all output columns initialized to empty
            output_row: Dict[str, str] = {col: "" for col in OUTPUT_COLUMNS}
            
            # Map input columns to output columns
            # If multiple input columns map to same output, use first non-empty value
            for input_header, input_value in row.items():
                if input_header in input_to_output:
                    output_header = input_to_output[input_header]
                    value = (input_value or "").strip()
                    
                    # Skip if output already has a non-empty value
                    if output_row.get(output_header, "").strip():
                        continue
                    
                    # Skip if current value is empty
                    if not value:
                        continue
                    
                    # Handle special cases
                    if output_header == "Straße":
                        street, hausnummer = extract_hausnummer(value)
                        output_row["Straße"] = street
                        if hausnummer:
                            output_row["Hausnummer"] = hausnummer
                    elif output_header == "Telefonnummer":
                        vorwahl, nummer = extract_vorwahl(value)
                        output_row["Telefonnummer"] = nummer if nummer else value
                        if vorwahl:
                            output_row["Vorwahl Telefon"] = vorwahl
                    elif output_header == "Entscheider 1 Anrede":
                        # "Anrede (AP 1)" maps to "Entscheider 1 Anrede"
                        # Also populate "Anrede" if it's empty
                        output_row["Entscheider 1 Anrede"] = value
                        if not output_row.get("Anrede", "").strip():
                            output_row["Anrede"] = value
                    else:
                        output_row[output_header] = value
            
            rows.append(output_row)
    
    return rows, stats


def combine_csv_files(
    input_folder: Path,
    output_file: Path,
) -> Dict:
    """
    Combine all CSV files from input folder and write to output file.
    
    Returns:
        Dictionary with statistics
    """
    # Find all CSV files
    csv_files = sorted(input_folder.glob("*.csv"))
    
    if not csv_files:
        raise ValueError(f"No CSV files found in folder: {input_folder}")
    
    print(f"Found {len(csv_files)} CSV file(s) to process")
    print()
    
    all_rows: List[Dict[str, str]] = []
    all_unmapped_headers: Set[str] = set()
    file_stats: List[Dict] = []
    
    # Process each CSV file
    for i, csv_file in enumerate(csv_files, 1):
        print(f"[{i}/{len(csv_files)}] Processing: {csv_file.name}")
        try:
            rows, stats = process_csv_file(csv_file, all_unmapped_headers)
            all_rows.extend(rows)
            stats["filename"] = csv_file.name
            file_stats.append(stats)
            print(f"  ✓ Read {stats['rows_read']} records")
        except Exception as e:
            print(f"  ✗ Error processing {csv_file.name}: {e}", file=sys.stderr)
            continue
    
    print()
    
    # Write combined output
    print(f"Writing combined output to: {output_file}")
    with output_file.open("w", encoding="utf-8-sig", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=OUTPUT_COLUMNS, delimiter=";")
        writer.writeheader()
        writer.writerows(all_rows)
    
    # Calculate statistics
    total_records = len(all_rows)
    column_stats: Dict[str, Dict[str, int]] = {}
    
    for col in OUTPUT_COLUMNS:
        non_empty = sum(1 for row in all_rows if row.get(col, "").strip())
        empty = total_records - non_empty
        completeness = (non_empty / total_records * 100) if total_records > 0 else 0
        
        column_stats[col] = {
            "non_empty": non_empty,
            "empty": empty,
            "completeness": completeness,
        }
    
    return {
        "total_files": len(csv_files),
        "total_records": total_records,
        "file_stats": file_stats,
        "column_stats": column_stats,
        "unmapped_headers": sorted(all_unmapped_headers),
    }


def print_statistics(stats: Dict) -> None:
    """Print descriptive statistics about the processing."""
    print("\n" + "=" * 80)
    print("PROCESSING STATISTICS")
    print("=" * 80)
    
    print(f"\nFiles Processed: {stats['total_files']}")
    print(f"Total Records: {stats['total_records']}")
    
    print("\n" + "-" * 80)
    print("Records per File:")
    print("-" * 80)
    for file_stat in stats["file_stats"]:
        print(f"  {file_stat['filename']}: {file_stat['rows_read']} records")
    
    print("\n" + "-" * 80)
    print("Column Completeness:")
    print("-" * 80)
    print(f"{'Column':<35} {'With Data':<12} {'Missing':<12} {'Completeness':<12}")
    print("-" * 80)
    
    for col, col_stats in stats["column_stats"].items():
        print(
            f"{col:<35} "
            f"{col_stats['non_empty']:<12} "
            f"{col_stats['empty']:<12} "
            f"{col_stats['completeness']:>6.1f}%"
        )
    
    if stats["unmapped_headers"]:
        print("\n" + "-" * 80)
        print("⚠️  Unmapped Headers (not in mapping dictionary):")
        print("-" * 80)
        for header in stats["unmapped_headers"]:
            print(f"  - {header}")
        print("\n  These headers were found in input files but are not defined in the mapping.")
        print("  Consider adding them to COLUMN_MAPPING if they should be mapped.")
    else:
        print("\n" + "-" * 80)
        print("✓ All headers were successfully mapped")
        print("-" * 80)
    
    print("\n" + "=" * 80)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Combine CSV files from a folder and map columns",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "input_folder",
        type=str,
        help="Path to folder containing CSV files",
    )
    parser.add_argument(
        "output_file",
        type=str,
        help="Path to output CSV file",
    )
    
    args = parser.parse_args()
    
    input_folder = Path(args.input_folder)
    output_file = Path(args.output_file)
    
    if not input_folder.exists():
        print(f"Error: Input folder does not exist: {input_folder}", file=sys.stderr)
        return 1
    
    if not input_folder.is_dir():
        print(f"Error: Input path is not a directory: {input_folder}", file=sys.stderr)
        return 1
    
    try:
        stats = combine_csv_files(input_folder, output_file)
        print_statistics(stats)
        print(f"\n✓ Done. Combined CSV written to: {output_file}")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

