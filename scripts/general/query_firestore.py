#!/usr/bin/env python3
"""
Query a Firestore collection with optional filters on DocumentReference fields
and simple equality on strings, integers, or booleans. Prints matching documents
as JSON and reports the total count.

Uses Application Default Credentials unless --credentials is set (same pattern
as delete_hits_by_business_and_campaign.py).

Examples:

  # Hits for one business (path may start with / or not)
  python query_firestore.py --env dev --collection hits \\
      --ref-eq business_ref businesses/css-informationstechnik-gmbh-63920

  python query_firestore.py --env dev --collection hits \\
      --ref-eq business_ref /businesses/css-informationstechnik-gmbh-63920

  # Multiple reference filters (AND)
  python query_firestore.py --env dev --collection hits \\
      --ref-eq business_ref businesses/foo \\
      --ref-eq campaign_ref campaigns/bar-uuid

  # Equality on plain fields
  python query_firestore.py --env dev --collection links \\
      --eq-int hit_count 0

  # Count only (no document bodies)
  python query_firestore.py --env dev --collection hits \\
      --ref-eq business_ref businesses/foo --count-only

  # Cap printed documents (still counts all unless --limit is set)
  python query_firestore.py --env dev --collection hits \\
      --ref-eq business_ref businesses/foo --print-limit 50

  # Explicit project / named database (e.g. non-default Firestore DB)
  python query_firestore.py --project gb-qr-tracker-dev --database test-2 \\
      --collection hits --ref-eq business_ref businesses/foo
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List, Optional, Tuple, Union

from google.cloud import firestore
from google.cloud.firestore_v1 import DocumentReference


DEFAULT_PROJECT_DEV = "gb-qr-tracker-dev"
DEFAULT_PROJECT_PROD = "gb-qr-tracker"
DEFAULT_DATABASE_ID = "(default)"


def get_project_for_env(env: str) -> str:
    if env == "prod":
        return DEFAULT_PROJECT_PROD
    if env == "dev":
        return DEFAULT_PROJECT_DEV
    raise ValueError(f"Unknown env: {env!r}")


def normalize_document_path(path: str) -> str:
    """Turn '/businesses/foo' or 'businesses/foo' into a Firestore document path."""
    return path.strip().strip("/")


def serialize_value(value: Any) -> Any:
    if isinstance(value, DocumentReference):
        return f"DocumentReference({value.path})"
    type_name = type(value).__name__
    if type_name in ("DatetimeWithNanoseconds", "Timestamp"):
        try:
            return value.isoformat()
        except (AttributeError, TypeError):
            return str(value)
    if hasattr(value, "isoformat") and callable(getattr(value, "isoformat", None)):
        if not isinstance(value, (str, int, float, bool)):
            try:
                return value.isoformat()
            except (AttributeError, TypeError):
                pass
    if isinstance(value, dict):
        return {k: serialize_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [serialize_value(v) for v in value]
    return value


def serialize_document(doc: Any) -> Dict[str, Any]:
    data = doc.to_dict()
    out: Dict[str, Any] = {"id": doc.id}
    if data is None:
        return out
    for key, val in data.items():
        out[key] = serialize_value(val)
    return out


def parse_bool(s: str) -> bool:
    low = s.lower()
    if low in ("true", "1", "yes"):
        return True
    if low in ("false", "0", "no"):
        return False
    raise argparse.ArgumentTypeError(f"Not a boolean: {s!r}")


def build_query(
    db: firestore.Client,
    collection: str,
    ref_equalities: List[Tuple[str, str]],
    string_equalities: List[Tuple[str, str]],
    int_equalities: List[Tuple[str, int]],
    bool_equalities: List[Tuple[str, bool]],
    query_limit: Optional[int],
):
    q: Union[firestore.CollectionReference, firestore.Query] = db.collection(collection)
    for field, doc_path in ref_equalities:
        ref = db.document(normalize_document_path(doc_path))
        q = q.where(field, "==", ref)
    for field, val in string_equalities:
        q = q.where(field, "==", val)
    for field, val in int_equalities:
        q = q.where(field, "==", val)
    for field, val in bool_equalities:
        q = q.where(field, "==", val)
    if query_limit is not None:
        q = q.limit(query_limit)
    return q


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Query Firestore by collection with reference and equality filters.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Project preset when --project is omitted. Default: dev",
    )
    parser.add_argument(
        "--project",
        default=None,
        help="GCP project ID (overrides --env).",
    )
    parser.add_argument(
        "--database",
        default=DEFAULT_DATABASE_ID,
        help=f"Firestore database ID (default: {DEFAULT_DATABASE_ID!r}).",
    )
    parser.add_argument(
        "--credentials",
        default=None,
        help="Optional path to a service account JSON key file.",
    )
    parser.add_argument(
        "--collection",
        required=True,
        help="Top-level collection id (e.g. hits, links).",
    )
    parser.add_argument(
        "--ref-eq",
        dest="ref_eq",
        nargs=2,
        metavar=("FIELD", "DOC_PATH"),
        action="append",
        default=[],
        help="Field must equal this document path (DocumentReference). Repeatable.",
    )
    parser.add_argument(
        "--eq",
        dest="eq_str",
        nargs=2,
        metavar=("FIELD", "VALUE"),
        action="append",
        default=[],
        help="Field == string value. Repeatable.",
    )
    parser.add_argument(
        "--eq-int",
        dest="eq_int",
        nargs=2,
        metavar=("FIELD", "INT"),
        action="append",
        default=[],
        help="Field == int value. Repeatable.",
    )
    parser.add_argument(
        "--eq-bool",
        dest="eq_bool",
        nargs=2,
        metavar=("FIELD", "BOOL"),
        action="append",
        default=[],
        help="Field == bool (true/false). Repeatable.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Firestore query limit (max documents returned from the server).",
    )
    parser.add_argument(
        "--print-limit",
        type=int,
        default=None,
        help="Max documents to print (default: all). Count includes all streamed unless --limit is set.",
    )
    parser.add_argument(
        "--count-only",
        action="store_true",
        help="Only print the total count, not document JSON.",
    )
    parser.add_argument(
        "--allow-full-scan",
        action="store_true",
        help="Allow query with no filters (reads entire collection).",
    )

    args = parser.parse_args()

    has_filter = bool(
        args.ref_eq or args.eq_str or args.eq_int or args.eq_bool
    )
    if not has_filter and not args.allow_full_scan:
        print(
            "Error: specify at least one of --ref-eq, --eq, --eq-int, --eq-bool, "
            "or pass --allow-full-scan to read the whole collection.",
            file=sys.stderr,
        )
        return 1

    int_equalities: List[Tuple[str, int]] = []
    for field, s in args.eq_int:
        try:
            int_equalities.append((field, int(s, 10)))
        except ValueError:
            print(f"Error: --eq-int {field!r} value {s!r} is not an integer.", file=sys.stderr)
            return 1

    bool_equalities = [(field, parse_bool(s)) for field, s in args.eq_bool]

    project_id = args.project or get_project_for_env(args.env)
    client_kwargs: Dict[str, Any] = {"project": project_id, "database": args.database}
    if args.credentials:
        from google.oauth2 import service_account

        creds = service_account.Credentials.from_service_account_file(args.credentials)
        client_kwargs["credentials"] = creds

    print("=" * 60)
    print("Firestore query")
    print("=" * 60)
    print(f"Project:    {project_id}")
    print(f"Database:   {args.database}")
    print(f"Collection: {args.collection}")
    for field, path in args.ref_eq:
        print(f"  {field} == DocumentReference({normalize_document_path(path)!r})")
    for field, val in args.eq_str:
        print(f"  {field} == {val!r} (string)")
    for field, val in int_equalities:
        print(f"  {field} == {val} (int)")
    for field, val in bool_equalities:
        print(f"  {field} == {val} (bool)")
    if args.limit is not None:
        print(f"Query limit: {args.limit}")
    print()

    try:
        db = firestore.Client(**client_kwargs)
    except Exception as e:
        print(f"Error initializing Firestore client: {e}", file=sys.stderr)
        return 1

    query = build_query(
        db,
        args.collection,
        list(args.ref_eq),
        list(args.eq_str),
        int_equalities,
        bool_equalities,
        args.limit,
    )

    total = 0
    printed = 0
    try:
        for doc in query.stream():
            total += 1
            if args.count_only:
                continue
            if args.print_limit is not None and printed >= args.print_limit:
                continue
            printed += 1
            print(f"--- document {printed} ---")
            print(json.dumps(serialize_document(doc), indent=2, default=str))
            print()
    except Exception as e:
        print(f"Error running query: {e}", file=sys.stderr)
        return 1

    print("=" * 60)
    print(f"Total matching documents: {total}")
    if (
        not args.count_only
        and args.print_limit is not None
        and printed >= args.print_limit
        and total > printed
    ):
        print(
            f"(Printed {printed} document(s); use --print-limit or remove it to print more.)"
        )
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
