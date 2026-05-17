"""Pure helpers for Worker hostname handling (easy to unit test without Firestore)."""


def normalize_original_host(header_val: str) -> str:
    if not header_val or not str(header_val).strip():
        return ""
    return str(header_val).strip().lower().split(":", 1)[0]
