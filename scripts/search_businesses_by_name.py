import argparse
from typing import List, Dict, Any

import firebase_admin
from firebase_admin import credentials, firestore


# Hard-coded prod service account path (same as other scripts)
SERVICE_ACCOUNT_PATH = "/Users/marcelgleich/Desktop/Software/Firebase_Service//gb-qr-tracker-firebase-adminsdk-fbsvc-e89462f043.json"


def _init_firebase() -> None:
    """Initialize Firebase Admin SDK once using the prod service account JSON."""
    if firebase_admin._apps:
        return
    cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
    firebase_admin.initialize_app(cred)


def _format_result(doc: firestore.DocumentSnapshot, source: str) -> Dict[str, Any]:
    data = doc.to_dict() or {}
    return {
        "source": source,
        "path": doc.reference.path,
        "id": doc.id,
        "name": data.get("name"),
        "email": data.get("email"),
        "raw": data,
    }


def search_businesses_by_name(term: str) -> List[Dict[str, Any]]:
    """Search top-level `businesses` and `customers/*/businesses` for name/email containing term.

    The match is **case-insensitive** and performed client-side so that
    partial matches (substrings) are supported.
    """
    _init_firebase()
    db = firestore.client()

    term_lower = term.lower()
    results: List[Dict[str, Any]] = []

    # 1) Top-level `businesses` collection
    for doc in db.collection("businesses").stream():
        data = doc.to_dict() or {}
        name = data.get("name")
        email = data.get("email")
        if (
            isinstance(name, str)
            and term_lower in name.lower()
        ) or (
            isinstance(email, str)
            and term_lower in email.lower()
        ):
            results.append(_format_result(doc, source="businesses"))

    # 2) All subcollections named `businesses` (e.g. `customers/{id}/businesses`)
    #    via a collection group query, then client-side filter on name.
    for doc in db.collection_group("businesses").stream():
        # This will include any subcollection named `businesses`, which is
        # typically where `customers/{customerId}/businesses` lives.
        data = doc.to_dict() or {}
        name = data.get("name")
        email = data.get("email")
        if (
            isinstance(name, str)
            and term_lower in name.lower()
        ) or (
            isinstance(email, str)
            and term_lower in email.lower()
        ):
            results.append(_format_result(doc, source="collection_group:businesses"))

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Search Firestore `businesses` and `customers/*/businesses` "
            "for documents whose `name` or `email` contains a given term."
        )
    )
    parser.add_argument(
        "--term",
        required=True,
        help="Search term to look for in the `name` or `email` field (case-insensitive).",
    )

    args = parser.parse_args()

    matches = search_businesses_by_name(args.term)

    print(f"Found {len(matches)} matching document(s) for term '{args.term}':\n")
    for m in matches:
        print("- Source:", m["source"])
        print("  Path:  ", m["path"])
        print("  ID:    ", m["id"])
        print("  Name:  ", m["name"])
        print("  Email: ", m["email"])
        print()


if __name__ == "__main__":
    main()
