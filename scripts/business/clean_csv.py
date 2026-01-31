"""
Clean and normalize CSV file:
- Remove duplicate entries in "Namenszeile" column (keep first occurrence)
- Fix "PLZ" column: ensure exactly 5 digits (add leading 0 if 4 digits, remove letters)
- Handle both comma (,) and semicolon (;) separators

Usage:
    python scripts/clean_csv.py input.csv output.csv
"""

import csv
import re
import sys
from pathlib import Path


def detect_delimiter(file_path: Path) -> str:
    """Detect CSV delimiter by reading first line."""
    with file_path.open("r", encoding="utf-8-sig", newline="") as f:
        first_line = f.readline()
        # Count occurrences of comma and semicolon
        comma_count = first_line.count(",")
        semicolon_count = first_line.count(";")
        # Return the delimiter with more occurrences, default to semicolon
        return ";" if semicolon_count >= comma_count else ","


def normalize_plz(plz_value: str) -> tuple[str, bool]:
    """
    Normalize PLZ to exactly 5 digits.
    - Remove all non-digit characters
    - If 4 digits, add leading 0
    - If not 4 or 5 digits, pad with zeros or truncate to 5
    
    Returns: (normalized_plz, was_changed)
    """
    if not plz_value:
        return "", False
    
    original = str(plz_value).strip()
    
    # Remove all non-digit characters
    digits_only = re.sub(r"[^\d]", "", original)
    
    if not digits_only:
        return "", original != ""
    
    # Normalize to 5 digits
    if len(digits_only) == 4:
        normalized = "0" + digits_only
        return normalized, True
    elif len(digits_only) == 5:
        normalized = digits_only
        return normalized, original != normalized
    elif len(digits_only) < 4:
        # Pad with leading zeros
        normalized = digits_only.zfill(5)
        return normalized, True
    else:
        # More than 5 digits, take first 5
        normalized = digits_only[:5]
        return normalized, True


def process_csv(input_path: Path, output_path: Path) -> dict:
    """
    Process CSV file and return statistics.
    """
    delimiter = detect_delimiter(input_path)
    
    stats = {
        "total_rows": 0,
        # Total duplicates removed (for backwards compatibility / pipeline summary)
        "duplicates_removed": 0,
        # More detailed breakdown
        "duplicates_removed_exact_name": 0,
        "duplicates_removed_address_name_prefix": 0,
        "plz_fixed": 0,
        "plz_removed_letters": 0,
        "plz_added_leading_zero": 0,
        "rows_written": 0,
    }
    
    # Track seen names globally (backwards-compatible behavior)
    seen_nameszeile = set()
    # Track names per composite address key for simple fuzzy-ish dedup
    # address_key -> set of normalized names seen at this address
    seen_by_address: dict[str, set[str]] = {}
    rows_to_write = []
    
    with input_path.open("r", encoding="utf-8-sig", newline="") as infile:
        reader = csv.DictReader(infile, delimiter=delimiter)
        fieldnames = reader.fieldnames or []
        
        # Try to find the column name (handle both "Namenszeile" and "Nameszeile")
        nameszeile_col = None
        for col in ["Namenszeile", "Nameszeile"]:
            if col in fieldnames:
                nameszeile_col = col
                break
        
        if nameszeile_col is None:
            raise ValueError("Column 'Namenszeile' not found in CSV header.")
        if "PLZ" not in fieldnames:
            raise ValueError("Column 'PLZ' not found in CSV header.")
        if "Straße" not in fieldnames and "Strasse" not in fieldnames:
            raise ValueError("Column 'Straße' (or 'Strasse') not found in CSV header.")
        if "Hausnummer" not in fieldnames:
            raise ValueError("Column 'Hausnummer' not found in CSV header.")
        
        for row in reader:
            stats["total_rows"] += 1
            
            # Normalize name for deduplication
            raw_name = (row.get(nameszeile_col) or "").strip()
            normalized_name = raw_name.lower()
            
            # Global exact duplicate check by name (previous behavior)
            if raw_name in seen_nameszeile:
                stats["duplicates_removed"] += 1
                stats["duplicates_removed_exact_name"] += 1
                continue  # Skip duplicate
            
            # Build simple composite address key for fast, local deduplication
            plz_raw = row.get("PLZ") or ""
            street_raw = row.get("Straße") or row.get("Strasse") or ""
            hausnummer_raw = row.get("Hausnummer") or ""

            # Use normalized PLZ in the key so "01234" and "1234" match
            normalized_plz_for_key, _ = normalize_plz(plz_raw)
            street_for_key = re.sub(r"\s+", " ", street_raw.strip()).upper()
            hausnummer_for_key = hausnummer_raw.strip().replace(" ", "").upper()

            address_key = f"{normalized_plz_for_key}|{street_for_key}|{hausnummer_for_key}"

            # Simple, fast "fuzzy" dedup within the same address:
            # treat as duplicate if the shorter name is a prefix of the longer one
            is_address_duplicate = False
            address_names = seen_by_address.get(address_key)
            if address_names:
                for existing_name in address_names:
                    if existing_name.startswith(normalized_name) or normalized_name.startswith(existing_name):
                        is_address_duplicate = True
                        break
            
            if is_address_duplicate:
                stats["duplicates_removed"] += 1
                stats["duplicates_removed_address_name_prefix"] += 1
                continue  # Skip duplicate at same address

            # Mark this name as seen globally and for this specific address
            seen_nameszeile.add(raw_name)
            if address_key not in seen_by_address:
                seen_by_address[address_key] = set()
            seen_by_address[address_key].add(normalized_name)
            
            # Fix PLZ
            plz_value = row.get("PLZ") or ""
            original_plz = str(plz_value).strip()
            normalized_plz, was_changed = normalize_plz(plz_value)
            
            if was_changed:
                stats["plz_fixed"] += 1
                # Check what type of change was made
                original_digits = re.sub(r"[^\d]", "", original_plz)
                if len(original_digits) == 4:
                    stats["plz_added_leading_zero"] += 1
                if re.search(r"[a-zA-Z]", original_plz):
                    stats["plz_removed_letters"] += 1
            
            row["PLZ"] = normalized_plz
            rows_to_write.append(row)
    
    # Write cleaned data
    with output_path.open("w", encoding="utf-8-sig", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=delimiter)
        writer.writeheader()
        writer.writerows(rows_to_write)
        stats["rows_written"] = len(rows_to_write)
    
    return stats


def print_statistics(stats: dict) -> None:
    """Print processing statistics."""
    print("\n" + "=" * 60)
    print("PROCESSING STATISTICS")
    print("=" * 60)
    print(f"Total rows read:                    {stats['total_rows']}")
    print(f"Duplicate rows removed (total):     {stats['duplicates_removed']}")
    # Only print detailed breakdown if keys are present (for forwards compatibility)
    exact = stats.get("duplicates_removed_exact_name", 0)
    addr_prefix = stats.get("duplicates_removed_address_name_prefix", 0)
    if exact or addr_prefix:
        print("  Breakdown:")
        print(f"    Exact name duplicates:          {exact}")
        print(f"    Same-address name-prefix dupes: {addr_prefix}")
    print(f"Rows written:                        {stats['rows_written']}")
    print()
    print("PLZ Column Fixes:")
    print(f"  Total PLZ values fixed:           {stats['plz_fixed']}")
    print(f"  Leading zeros added:              {stats['plz_added_leading_zero']}")
    print(f"  Letters removed:                  {stats['plz_removed_letters']}")
    print("=" * 60)


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("Usage: python scripts/clean_csv.py input.csv output.csv")
        return 1
    
    input_path = Path(argv[1])
    output_path = Path(argv[2])
    
    if not input_path.exists():
        print(f"Error: Input file does not exist: {input_path}")
        return 1
    
    try:
        stats = process_csv(input_path, output_path)
        print_statistics(stats)
        print(f"\nDone. Cleaned CSV written to: {output_path}")
        return 0
    except Exception as e:
        print(f"Error processing CSV: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

