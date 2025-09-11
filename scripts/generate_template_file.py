# assign_templates.py
import pandas as pd
import re

# --- edit these file names or pass them in however you like ---
FILE_A = "/Users/marcelgleich/Downloads/branchenliste_wzcodes_groessig (1).csv"   # simple list of WZ codes (first column)
FILE_B_XLSX = "/Users/marcelgleich/Downloads/Gleich___Brother_GmbH_b2bselfservice_UY915052_20250820_131101.xlsx"  # Excel file with columns "Branchencode WZ" and "Template"
B_SHEET = "Export-20.08.2025-Einmal"  # or a sheet name like "Contacts"
OUTPUT_XLSX = "data/file_b_with_templates.xlsx"
TEMPLATE_PREFIX = "template_"   # change to "temple_" if that's what you want
TEMPLATE_SUFFIX = ".pdf"
DEFAULT_TEMPLATE = f"{TEMPLATE_PREFIX}standart{TEMPLATE_SUFFIX}"  # fallback if no match
# --------------------------------------------------------------

# Allowed templates (only these will be assigned)
ALLOWED_CODES = [
    "4791", "6622", "781", "731", "813", "9313", "6619",
    "862", "855", "4511", "561", "551", "6831"
]


def normalize(s: str) -> str:
    # Keep only digits to handle things like "55.10" -> "5510"
    return re.sub(r"\D", "", str(s)) if pd.notna(s) else ""

# Sort allowed codes by length (desc) so the most specific prefix wins
BASE_CODES = sorted(ALLOWED_CODES, key=len, reverse=True)

# Read file B (Excel) as strings so codes like 55101 don't turn into numbers
b = pd.read_excel(FILE_B_XLSX, sheet_name=B_SHEET, dtype=str, engine="openpyxl")

if "Branchencode WZ" not in b.columns:
    raise KeyError('Column "Branchencode WZ" not found in file B.')

# Ensure Template column exists
if "Template" not in b.columns:
    b["Template"] = ""

def pick_template(branch_code):
    s = normalize(branch_code)
    for base in BASE_CODES:
        if s.startswith(base):
            return f"{TEMPLATE_PREFIX}{base}{TEMPLATE_SUFFIX}"
    return DEFAULT_TEMPLATE  # hard fallback if no allowed prefix matches

# Fill Template using only allowed codes
b["Template"] = b["Branchencode WZ"].apply(pick_template)

# Save to Excel
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    b.to_excel(writer, index=False, sheet_name="Sheet1")

print(f"Done. Wrote: {OUTPUT_XLSX}")