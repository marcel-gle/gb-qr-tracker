from dotenv import load_dotenv
import os
import time
from urllib.parse import urlparse

import pandas as pd
import requests


def get_access_token(client_id: str, client_secret: str) -> str:
    """
    Get OAuth access token for Snov.io.
    """
    params = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    res = requests.post("https://api.snov.io/v1/oauth/access_token", data=params)
    res.raise_for_status()
    data = res.json()
    return data["access_token"]


def get_domain_search(domain: str, token: str, type: str = "all", limit: int = 100, last_id: int = 0):
    """
    Call Snov.io domain-emails-with-info endpoint for a given domain.
    """
    params = {
        "domain": domain,
        "type": type,
        "limit": limit,
        "lastId": last_id,
    }
    headers = {
        "Authorization": f"Bearer {token}",
    }
    res = requests.get("https://api.snov.io/v2/domain-emails-with-info", params=params, headers=headers)
    res.raise_for_status()
    return res.json()


def correct_url(url: str) -> str:
    """
    Fix known malformed URL patterns.
    """
    if not isinstance(url, str):
        return ""
    url = url.strip()
    if url.startswith("http://https://"):
        return url[len("http://") :]
    return url


def extract_domain_from_string(value: str) -> str:
    """
    Extract a bare domain from a URL, email address or raw domain string.
    """
    if not isinstance(value, str):
        return ""
    value = value.strip()
    if not value:
        return ""

    # If it looks like an email address, take part after @
    if "@" in value and " " not in value:
        domain = value.split("@", 1)[1]
    else:
        url = correct_url(value)
        parsed = urlparse(url if "://" in url else f"https://{url}")
        domain = parsed.netloc or parsed.path.split("/")[0]

    if domain.startswith("www."):
        domain = domain[4:]
    return domain.lower()


GENERIC_EMAIL_PREFIXES = (
    "info@",
    "kontakt@",
    "contact@",
    "sales@",
    "support@",
    "service@",
    "office@",
    "mail@",
    "hello@",
    "admin@",
    "noreply@",
    "no-reply@",
    "bewerbung@",
)


def is_generic_email(email: str) -> bool:
    if not isinstance(email, str):
        return False
    email = email.lower()
    return email.startswith(GENERIC_EMAIL_PREFIXES)


def choose_best_email(contacts, target_first: str, target_last: str):
    """
    Choose the best email for a given person from Snov.io contacts.

    Priority:
    1. Personal: exact (or very close) first+last name match -> email_level="personal"
    2. Enhanced: non-generic email from same domain -> email_level="enhanced"
    3. Generic: generic email like info@... -> email_level="generic"
    """
    target_first = (target_first or "").strip().lower()
    target_last = (target_last or "").strip().lower()

    personal_candidates = []
    enhanced_candidates = []
    generic_candidates = []

    for c in contacts:
        email = c.get("email")
        if not email:
            continue
        email_l = email.lower()
        first = (c.get("first_name") or "").strip().lower()
        last = (c.get("last_name") or "").strip().lower()

        # Name match logic: exact match or last name match + same first initial
        name_match = False
        if target_last:
            if last == target_last and target_first:
                if first == target_first:
                    name_match = True
                elif first and first[0] == target_first[0]:
                    name_match = True

        if name_match:
            personal_candidates.append(email_l)
        elif not is_generic_email(email_l):
            enhanced_candidates.append(email_l)
        else:
            generic_candidates.append(email_l)

    if personal_candidates:
        return personal_candidates[0], "personal"
    if enhanced_candidates:
        return enhanced_candidates[0], "enhanced"
    if generic_candidates:
        return generic_candidates[0], "generic"
    return None, None


def infer_domain_for_row(row, domain_col: str, website_col: str, email_col: str) -> str:
    """
    Determine the best domain for a row using Domain, Website or E-Mail-Adresse.
    Priority: Domain > Website > E-Mail-Adresse.
    """
    for col in (domain_col, website_col, email_col):
        if col and col in row and isinstance(row[col], str) and row[col].strip():
            domain = extract_domain_from_string(row[col])
            if domain:
                return domain
    return ""


def process_csv_with_snov(
    csv_path: str,
    client_id: str,
    client_secret: str,
    encoding: str = "utf-8",
    delimiter: str = ";",
    domain_column: str = "Domain",
    website_column: str = "Website",
    email_column: str = "E-Mail-Adresse",
    first_name_column: str = "Entscheider 1 Vorname",
    last_name_column: str = "Entscheider 1 Nachname",
    max_requests_per_hour: int = 490,
) -> None:
    """
    Main pipeline:
    - Read CSV.
    - Derive domain per row from Domain/Website/E-Mail-Adresse.
    - Fetch contacts from Snov.io per unique domain.
    - For each row, choose best email based on person's name.
    - Write best_email and email_level columns back to CSV.
    """
    print(f"Loading CSV from {csv_path}")
    df = pd.read_csv(csv_path, encoding=encoding, delimiter=delimiter)
    print(f"Rows loaded: {len(df)}")

    # Ensure output columns exist
    if "best_email" not in df.columns:
        df["best_email"] = ""
    if "email_level" not in df.columns:
        df["email_level"] = ""

    # Pre-compute domains per row
    domains = []
    for _, row in df.iterrows():
        domain = infer_domain_for_row(row, domain_column, website_column, email_column)
        domains.append(domain)
    df["__domain__"] = domains

    unique_domains = sorted({d for d in domains if d})
    print(f"Unique non-empty domains: {len(unique_domains)}")

    if not unique_domains:
        print("No domains found. Nothing to do.")
        return

    # Get token once
    token = get_access_token(client_id, client_secret)

    # Rate limiting
    request_interval = 3600 / max_requests_per_hour
    last_request_time = 0.0

    # Fetch contacts per domain
    domain_contacts = {}
    for idx, domain in enumerate(unique_domains, start=1):
        # Respect rate limit
        now = time.time()
        since_last = now - last_request_time
        if since_last < request_interval:
            sleep_time = request_interval - since_last
            time.sleep(sleep_time)

        print(f"[{idx}/{len(unique_domains)}] Fetching contacts for domain: {domain}")
        try:
            result = get_domain_search(domain, token)
            contacts = result.get("data", []) or []
            domain_contacts[domain] = contacts
            last_request_time = time.time()
        except Exception as e:
            print(f"Error fetching domain {domain}: {e}")
            domain_contacts[domain] = []

    # Match emails per row
    updated_rows = 0
    for i, row in df.iterrows():
        domain = row["__domain__"]
        if not domain:
            continue

        contacts = domain_contacts.get(domain, [])
        if not contacts:
            continue

        first_name = row.get(first_name_column, "")
        last_name = row.get(last_name_column, "")

        best_email, email_level = choose_best_email(contacts, first_name, last_name)
        if best_email and (not isinstance(row["best_email"], str) or not row["best_email"].strip()):
            df.at[i, "best_email"] = best_email
            df.at[i, "email_level"] = email_level or ""
            updated_rows += 1

    print(f"Rows updated with best_email: {updated_rows}")

    # Clean up helper column
    df.drop(columns=["__domain__"], inplace=True)

    df.to_csv(csv_path, index=False, encoding=encoding, sep=delimiter)
    print(f"Updated CSV saved to {csv_path}")


if __name__ == "__main__":
    load_dotenv()

    # Configuration – adjust these values as needed
    ENCODING = os.getenv("CSV_ENCODING", "ISO-8859-15")
    DELIMITER = ";"

    client_id = os.getenv("SNOVIO_CLIENT_ID", "")
    client_secret = os.getenv("SNOVIO_CLIENT_SECRET", "")

    if not client_id or not client_secret:
        raise RuntimeError("SNOVIO_CLIENT_ID and SNOVIO_CLIENT_SECRET must be set in environment or .env file.")

    # Path to your input CSV (must be absolute or relative to project root)
    CSV_FILE = os.getenv("SNOVIO_INPUT_CSV", "/path/to/your/example_file.csv")

    process_csv_with_snov(
        csv_path=CSV_FILE,
        client_id=client_id,
        client_secret=client_secret,
        encoding=ENCODING,
        delimiter=DELIMITER,
        # With your current format, there is no explicit Domain/Website column,
        # so we rely on E-Mail-Adresse and extract the domain from it.
        domain_column="",
        website_column="",
        email_column="E-Mail-Adresse",
        first_name_column="Entscheider 1 Vorname",
        last_name_column="Entscheider 1 Nachname",
    )


