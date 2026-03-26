#!/usr/bin/env python3
"""
Export Firestore docs from `properties_de` where `advertising_objection == True`
to a CSV containing: street, house_number, postcode, city.

All address fields are assumed to be top-level in the document.
"""

import argparse
import csv
import re
from typing import Any, Dict

from google.cloud import firestore
from google.oauth2 import service_account


def get_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


# German address heuristic:
# - Typical street name format: "<street> <house_number>".
# - House numbers can have an optional letter suffix (e.g. 17a, 20b).
# - We only use this when `house_number` is missing/empty.
_HOUSE_RE = re.compile(r"^(?P<street>.+?)\s+(?P<house>\d+[a-zA-Z]?)\s*$")


def parse_house_number_from_street(street: str) -> tuple[str, str]:
    """
    Returns (street_without_number, house_number).
    If parsing fails, returns (street, "").
    """
    s = street.strip()
    if not s:
        return street, ""
    m = _HOUSE_RE.match(s)
    if not m:
        return street, ""
    return m.group("street").strip(), m.group("house").strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-id", required=True, help="GCP project id for the Firestore instance")
    parser.add_argument("--credentials", required=True, help="Path to service account JSON key")
    parser.add_argument("--output", default="advertising_objection_properties_de.csv", help="Output CSV path")
    parser.add_argument("--limit", type=int, default=0, help="Optional limit (0 = no limit)")
    args = parser.parse_args()

    creds = service_account.Credentials.from_service_account_file(args.credentials)
    db = firestore.Client(project=args.project_id, credentials=creds)

    query = db.collection("properties_de").where("advertising_objection", "==", True)
    if args.limit and args.limit > 0:
        query = query.limit(args.limit)

    rows = []
    for doc in query.stream():
        data: Dict[str, Any] = doc.to_dict() or {}

        street = get_str(data.get("street"))
        house_number = get_str(data.get("house_number"))
        if not house_number.strip() and street.strip():
            # If the address was stored as a single string, split it.
            street, house_number = parse_house_number_from_street(street)

        rows.append(
            [
                street,
                house_number,
                get_str(data.get("postcode")),
                get_str(data.get("city")),
            ]
        )

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["street", "house_number", "postcode", "city"])
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {args.output}")


if __name__ == "__main__":
    main()

