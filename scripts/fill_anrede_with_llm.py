"""
Fill missing 'Anrede' values in a CSV using a local LM Studio model.

- If 'Anrede' is empty/missing in a row, the script uses the local model
  (configured as in 'scrape_oceanio_ai.py') to decide whether
  'Entscheider 1 Vorname' is male or female.
- It then fills 'Anrede' with 'Herr' (male) or 'Frau' (female).

Usage:
    python scripts/fill_anrede_with_llm.py input.csv output.csv
"""

import csv
import os
import sys
from pathlib import Path

from openai import OpenAI
from tqdm import tqdm


# ---------------- Local ML Studio client ----------------

# Keep this consistent with 'scrape_oceanio_ai.py'
ML_STUDIO_BASE_URL = os.environ.get("ML_STUDIO_BASE_URL", "http://localhost:1234/v1")
LOCAL_MODEL = "openai/gpt-oss-20b"  # ML Studio model name

# Initialize local client (no real API key needed)
local_client = OpenAI(
    base_url=ML_STUDIO_BASE_URL,
    api_key="not-needed",
)


def infer_anrede_from_first_name(first_name: str) -> str:
    """
    Use the local LLM to infer gender from a first name and return 'Herr' or 'Frau'.
    Falls back to 'Herr' if the answer is unclear, invalid, or the name is missing.
    """
    first_name = (first_name or "").strip()
    if not first_name:
        return "Herr"

    prompt = (
        "Du klassifizierst deutsche Vornamen nach Anrede.\n"
        f"Vorname: '{first_name}'\n"
        "Antwortformat:\n"
        "- Antworte mit genau einem Wort: 'Herr' oder 'Frau'.\n"
        "- Keine Erklärungen, keine Satzzeichen, keine weiteren Wörter."
    )

    try:
        resp = local_client.chat.completions.create(
            model=LOCAL_MODEL,
            messages=[
                {"role": "system", "content": "You classify first names by gender."},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
            max_tokens=100,
        )
        raw_answer = resp.choices[0].message.content
        print(f"First name: {first_name}, Raw answer: {repr(raw_answer)}")
        answer = (raw_answer or "").strip().lower()
    except Exception:
        # If the local model is not reachable or any error occurs, fall back
        return "Herr"

    # Prefer direct German honorifics if the model followed the instructions
    if answer == "frau":
        return "Frau"
    if answer == "herr":
        return "Herr"

    # Fallback: also support English/gender words if the model ignores instructions
    if "female" in answer or "weiblich" in answer:
        return "Frau"
    if "male" in answer or "männlich" in answer:
        return "Herr"

    # If we still can't interpret the answer, fall back to 'Herr'
    return "Herr"


def process_csv(input_path: Path, output_path: Path) -> None:
    with input_path.open("r", encoding="utf-8-sig", newline="") as infile, \
         output_path.open("w", encoding="utf-8-sig", newline="") as outfile:

        # Your CSV uses ';' as the delimiter (per the provided header),
        # so configure DictReader/DictWriter accordingly.
        reader = csv.DictReader(infile, delimiter=";")
        fieldnames = reader.fieldnames or []

        required_columns = ["Anrede", "Entscheider 1 Vorname"]
        for col in required_columns:
            if col not in fieldnames:
                raise ValueError(f"Required column '{col}' not found in CSV header.")

        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()

        # Load all rows first so we can show a proper progress bar with total count
        rows = list(reader)

        for row in tqdm(rows, desc="Processing rows"):
            anrede = (row.get("Anrede") or "").strip()
            if not anrede:
                first_name = row.get("Entscheider 1 Vorname") or ""
                inferred = infer_anrede_from_first_name(first_name)
                # Always overwrite when Anrede is empty; infer_anrede_from_first_name
                # already contains safe fallbacks.
                row["Anrede"] = inferred

            writer.writerow(row)


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("Usage: python scripts/fill_anrede_with_llm.py input.csv output.csv")
        return 1

    input_path = Path(argv[1])
    output_path = Path(argv[2])

    if not input_path.exists():
        print(f"Input file does not exist: {input_path}")
        return 1

    process_csv(input_path, output_path)
    print(f"Done. Wrote updated CSV to: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))


