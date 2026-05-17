"""
Filtert Eventlocation-Leads auf "echte Eventlocations".

Pipeline:
1) Dedupliziert Adressen (Spalte "address")
2) Entfernt nicht-deutsche Adressen
3) Entfernt ungueltige/auffaellige Adressen
4) Entfernt unerwuenschte Typen (regelbasiert + optional LLM)

Beispiel:
    python scripts/scrapers/eventlocation_filter.py input.csv
    python scripts/scrapers/eventlocation_filter.py input.csv --skip-llm
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from openai import OpenAI
from pandas.errors import ParserError

DEFAULT_MODEL = "openai/gpt-oss-20b"
LM_STUDIO_BASE_URL = "http://localhost:1234/v1"
REQUEST_TIMEOUT = 15
LLM_TIMEOUT = 120
DELAY_BETWEEN_REQUESTS = 0.5
COUNTRY_CHECK_BATCH_SIZE = 25

NON_GERMANY_MARKERS = {
    "austria",
    "oesterreich",
    "switzerland",
    "schweiz",
    "netherlands",
    "niederlande",
    "belgium",
    "france",
    "italy",
    "spain",
    "uk",
    "united kingdom",
    "england",
    "usa",
    "united states",
    "poland",
    "czech",
    "luxembourg",
    "denmark",
}

INVALID_ADDRESS_MARKERS = {
    "n/a",
    "na",
    "unknown",
    "unbekannt",
    "test",
    "dummy",
    "null",
    "none",
    "keine adresse",
}

EXCLUSION_RULES = {
    "sportvereine": [
        "sportverein",
        "verein",
        "sv ",
        "fc ",
        "turnverein",
        "tennisclub",
        "sportclub",
    ],
    "konzerthallen": [
        "konzerthalle",
        "concert hall",
        "philhamonie",
        "philharmonie",
        "music hall",
    ],
    "coworking_business_center": [
        "coworking",
        "co-working",
        "business center",
        "businesscentre",
        "flex office",
        "shared office",
        "serviced office",
    ],
    "hotelketten": [
        "best western",
        "ibis",
        "motel one",
        "marriott",
        "hilton",
        "radisson",
        "mercure",
        "nh hotel",
        "holiday inn",
    ],
    "event_hochzeitsfotografen": [
        "fotograf",
        "photograph",
        "hochzeitsfotograf",
        "wedding photographer",
        "eventfotograf",
    ],
    "kinos": [
        "kino",
        "cinema",
        "cineplex",
        "cinestar",
    ],
    "sportstaetten": [
        "stadion",
        "arena",
        "sportpark",
        "sporthalle",
        "sportstaette",
        "sportst tte",
    ],
    "stadthallen_oeffentliche_locations": [
        "stadthalle",
        "mehrzweckhalle",
        "rathaus",
        "gemeindehalle",
        "kulturamt",
        "stadtverwaltung",
    ],
    "kirchen_kirchengemeinden": [
        "kirche",
        "kirchengemeinde",
        "pfarrei",
        "pfarrgemeinde",
        "evangelisch",
        "katholisch",
    ],
    "eventplaner_agenturen": [
        "eventagentur",
        "event agentur",
        "eventplaner",
        "wedding planner",
        "full service event",
    ],
}

LLM_SYSTEM_PROMPT = """Du klassifizierst Leads fuer Eventlocations in Deutschland.

Ziel:
- Wir wollen NUR echte Eventlocations.
- Alles andere muss ausgeschlossen werden.

Ausschlusskategorien:
1) Sportvereine
2) Konzerthallen
3) Coworking spaces / flexible Bueros / Business Centers
4) Hotelketten (z.B. Best Western, ibis, Motel One, ...)
5) Event- oder Hochzeitsfotografen
6) Locations nicht in Deutschland
7) Kinos
8) Sportstaetten
9) Stadthallen / Eventlocations oeffentlicher Einrichtungen
10) Kirchen / Kirchengemeinden
11) Eventplaner / Eventagenturen

Gib NUR valides JSON zurueck:
{
  "is_real_eventlocation": <true|false>,
  "excluded_category": "<eine der Kategorien oder leer>",
  "reason": "<kurz, konkret>",
  "confidence": "<hoch|mittel|niedrig>"
}
"""

ADDRESS_COUNTRY_SYSTEM_PROMPT = """Du pruefst Adressen auf Land = Deutschland.

Aufgabe:
- Entscheide nur, ob die Adresse in Deutschland liegt.
- Wenn unklar, triff die wahrscheinlichste Entscheidung und setze confidence entsprechend.

Antwort nur als valides JSON:
{
  "is_in_germany": <true|false>,
  "reason": "<kurz, konkret>",
  "confidence": "<hoch|mittel|niedrig>"
}
"""

ADDRESS_COUNTRY_BATCH_SYSTEM_PROMPT = """Du pruefst mehrere Adressen auf Land = Deutschland.

Aufgabe:
- Entscheide fuer jede Adresse nur, ob sie in Deutschland liegt.
- Wenn unklar, triff die wahrscheinlichste Entscheidung und setze confidence entsprechend.

WICHTIG:
- Gib fuer jede Eingabe-ID genau ein Ergebnis zurueck.
- JSON ONLY, keine zusaetzlichen Texte.

Antwortformat:
{
  "results": [
    {"id": "<id>", "is_in_germany": <true|false>, "reason": "<kurz>", "confidence": "<hoch|mittel|niedrig>"}
  ]
}
"""


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def normalize_address_for_dedupe(address: str) -> str:
    text = normalize_text(address)
    text = text.replace("str.", "strasse")
    text = text.replace("straße", "strasse")
    text = re.sub(r"[^a-z0-9äöüß,\-\s]", "", text)
    return re.sub(r"\s+", " ", text).strip(" ,")


def extract_domain(website_or_domain: str) -> str:
    raw = str(website_or_domain or "").strip()
    if not raw:
        return ""

    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    domain = (parsed.netloc or parsed.path).lower().strip("/")
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def looks_non_german(address: str) -> bool:
    text = normalize_text(address)
    if not text:
        return True

    if " deutschland" in f" {text}" or " germany" in f" {text}":
        return False

    return any(marker in text for marker in NON_GERMANY_MARKERS)


def looks_like_valid_address(address: str) -> tuple[bool, str]:
    text = str(address or "").strip()
    lower = normalize_text(text)

    if not text:
        return False, "leere_adresse"
    if len(text) < 10:
        return False, "zu_kurz"
    if "http://" in lower or "https://" in lower or "@" in lower:
        return False, "enthaelt_url_oder_email"

    if lower in INVALID_ADDRESS_MARKERS:
        return False, "placeholder_adresse"

    if re.fullmatch(r"-?\d+\.\d+\s*,\s*-?\d+\.\d+", lower):
        return False, "koordinaten_statt_adresse"
    if re.search(r"[23456789cfghjmpqrvwx]{4,}\+[23456789cfghjmpqrvwx]{2,}", lower):
        return False, "plus_code_statt_adresse"

    has_plz = bool(re.search(r"\b(?:d-)?\d{5}\b", lower))
    has_street_hint = bool(
        re.search(
            r"\b(strasse|straße|weg|platz|allee|gasse|ring|ufer|chaussee)\b",
            lower,
        )
    )
    has_house_number = bool(re.search(r"\b\d{1,4}[a-z]?\b", lower))

    if not has_plz:
        return False, "keine_deutsche_plz"
    if not has_street_hint and not has_house_number:
        return False, "keine_strassenstruktur"

    special_ratio = len(re.findall(r"[^a-z0-9äöüß,\-.\s]", lower)) / max(len(lower), 1)
    if special_ratio > 0.12:
        return False, "zu_viele_sonderzeichen"

    return True, ""


def match_exclusion_rule(text: str) -> tuple[bool, str]:
    haystack = normalize_text(text)
    for category, keywords in EXCLUSION_RULES.items():
        if any(kw in haystack for kw in keywords):
            return True, category
    return False, ""


def fetch_homepage_text(url_or_domain: str) -> str:
    url = str(url_or_domain or "").strip()
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"

    try:
        with httpx.Client(
            timeout=REQUEST_TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 EventlocationFilter/1.0"},
        ) as client:
            response = client.get(url)
            html = response.text
    except Exception:
        return ""

    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all(["script", "style", "noscript", "svg"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    return text[:5000]


def create_llm_client(base_url: str) -> OpenAI:
    return OpenAI(base_url=base_url, api_key="lm-studio", timeout=LLM_TIMEOUT)


def classify_with_llm(client: OpenAI, model: str, payload: dict) -> dict:
    raw = ""
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": LLM_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": (
                        "Klassifiziere diesen Datensatz:\n"
                        f"{json.dumps(payload, ensure_ascii=False)}"
                    ),
                },
            ],
            temperature=0.0,
            max_tokens=280,
        )
        raw = response.choices[0].message.content or ""
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            return {"error": "kein_json", "raw": raw[:200]}
        return json.loads(match.group())
    except Exception as exc:  # noqa: BLE001 - robustes CLI-Skript
        return {"error": str(exc), "raw": raw[:200]}


def classify_address_country_with_llm(client: OpenAI, model: str, address: str) -> dict:
    raw = ""
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": ADDRESS_COUNTRY_SYSTEM_PROMPT},
                {"role": "user", "content": f'Adresse: "{str(address or "").strip()}"'},
            ],
            temperature=0.0,
            max_tokens=120,
        )
        raw = response.choices[0].message.content or ""
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            return {"error": "kein_json", "raw": raw[:200]}
        return json.loads(match.group())
    except Exception as exc:  # noqa: BLE001 - robustes CLI-Skript
        return {"error": str(exc), "raw": raw[:200]}


def classify_address_countries_with_llm(
    client: OpenAI,
    model: str,
    batch: list[dict],
) -> dict:
    raw = ""
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": ADDRESS_COUNTRY_BATCH_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": (
                        "Pruefe diese Adressen:\n"
                        f"{json.dumps(batch, ensure_ascii=False)}"
                    ),
                },
            ],
            temperature=0.0,
            max_tokens=1500,
        )
        raw = response.choices[0].message.content or ""
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            return {"error": "kein_json", "raw": raw[:300]}
        parsed = json.loads(match.group())
        if not isinstance(parsed, dict) or not isinstance(parsed.get("results"), list):
            return {"error": "ungueltiges_batch_format", "raw": raw[:300]}
        return parsed
    except Exception as exc:  # noqa: BLE001 - robustes CLI-Skript
        return {"error": str(exc), "raw": raw[:300]}


def get_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_map = {col.lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    return None


def render_progress(prefix: str, current: int, total: int, width: int = 28) -> None:
    total = max(total, 1)
    ratio = min(max(current / total, 0.0), 1.0)
    done = int(width * ratio)
    bar = "#" * done + "-" * (width - done)
    print(f"\r{prefix} [{bar}] {current}/{total} ({ratio * 100:5.1f}%)", end="", flush=True)
    if current >= total:
        print()


def chunk_list(items: list, chunk_size: int) -> list[list]:
    chunk_size = max(1, chunk_size)
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def process_csv(
    input_csv: str,
    output_csv: str | None,
    removed_csv: str | None,
    model: str,
    base_url: str,
    skip_llm: bool,
    limit: int | None,
) -> None:
    try:
        df = pd.read_csv(input_csv, sep=None, engine="python")
    except ParserError as exc:
        print(f"FEHLER: CSV konnte nicht eingelesen werden: {exc}")
        sys.exit(1)

    address_col = get_column(df, ["address"])
    if not address_col:
        print("FEHLER: Spalte 'address' wurde nicht gefunden.")
        print(f"Gefundene Spalten: {list(df.columns)}")
        sys.exit(1)

    website_col = get_column(df, ["website", "webseite"])
    domain_col = get_column(df, ["domain"])
    name_col = get_column(df, ["name"])

    if limit is not None:
        if limit <= 0:
            print("FEHLER: --limit muss > 0 sein.")
            sys.exit(1)
        df = df.head(limit).copy()

    total_before = len(df)
    removed_frames = []
    llm_client = None

    print("\n" + "=" * 64)
    print("START EVENTLOCATION FILTER")
    print("=" * 64)
    print(f"Input CSV:               {input_csv}")
    print(f"Zeilen (initial):        {total_before}")
    print(f"Address-Spalte:          {address_col}")
    print(f"Website-Spalte:          {website_col or '-'}")
    print(f"Domain-Spalte:           {domain_col or '-'}")
    print(f"Name-Spalte:             {name_col or '-'}")
    print(f"LLM aktiv:               {'nein (--skip-llm)' if skip_llm else 'ja'}")

    # 1) Adressduplikate entfernen
    print("\n[1/4] Entferne Adress-Duplikate ...")
    df["_address_norm"] = df[address_col].apply(normalize_address_for_dedupe)
    duplicate_mask = df["_address_norm"].duplicated(keep="first")
    if duplicate_mask.any():
        dropped = df[duplicate_mask].copy()
        dropped["remove_reason"] = "duplicate_address"
        removed_frames.append(dropped)
        df = df[~duplicate_mask].copy()
        print(f"  -> Entfernt: {len(dropped)}")
    else:
        print("  -> Entfernt: 0")
    print(f"  -> Verbleibend: {len(df)}")

    # LLM-Client vorbereiten (falls aktiv)
    if not skip_llm:
        print("\nLLM-Verbindung wird getestet ...")
        llm_client = create_llm_client(base_url)
        try:
            llm_client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": "Antwort nur mit OK"}],
                max_tokens=5,
            )
            print(f"LLM-Verbindung OK ({model})")
        except Exception as exc:
            print(f"WARNUNG: LLM nicht erreichbar, fallback auf Regeln. Fehler: {exc}")
            skip_llm = True
            llm_client = None

    # 2) Nur Deutschland (LLM-basiert, Rule-Fallback nur bei --skip-llm)
    print("\n[2/4] Pruefe Deutschland-Adressen ...")
    if skip_llm:
        non_de_mask = df[address_col].apply(looks_non_german)
        if non_de_mask.any():
            dropped = df[non_de_mask].copy()
            dropped["remove_reason"] = "non_german_address_rule"
            removed_frames.append(dropped)
            df = df[~non_de_mask].copy()
            print(f"  -> Regelbasiert entfernt: {len(dropped)}")
        else:
            print("  -> Regelbasiert entfernt: 0")
    else:
        address_country_cache: dict[str, tuple[bool, str]] = {}
        non_de_idx = []
        non_de_reason = {}
        normalized_addresses = df[address_col].apply(
            lambda value: normalize_address_for_dedupe(str(value or ""))
        )
        unique_norm_addresses = list(dict.fromkeys(normalized_addresses.tolist()))
        total_unique = len(unique_norm_addresses)
        print(
            f"  -> Unique Adressen fuer Country-Check: {total_unique} "
            f"(Batch-Size: {COUNTRY_CHECK_BATCH_SIZE})"
        )

        batches = chunk_list(unique_norm_addresses, COUNTRY_CHECK_BATCH_SIZE)
        processed_unique = 0

        for batch in batches:
            request_payload = [{"id": key, "address": key} for key in batch]
            batch_result = classify_address_countries_with_llm(llm_client, model, request_payload)

            if "error" in batch_result:
                # Fallback auf Einzel-Checks nur fuer diese Batch.
                for key in batch:
                    single_result = classify_address_country_with_llm(llm_client, model, key)
                    if "error" in single_result:
                        address_country_cache[key] = (True, "llm_country_check_error_keep")
                    else:
                        address_country_cache[key] = (
                            bool(single_result.get("is_in_germany", True)),
                            str(single_result.get("reason", "")).strip(),
                        )
                    processed_unique += 1
                    render_progress("  -> LLM Country-Check", processed_unique, total_unique)
                continue

            result_map = {}
            for item in batch_result.get("results", []):
                item_id = str(item.get("id", "")).strip()
                if not item_id:
                    continue
                result_map[item_id] = (
                    bool(item.get("is_in_germany", True)),
                    str(item.get("reason", "")).strip(),
                )

            for key in batch:
                address_country_cache[key] = result_map.get(
                    key,
                    (True, "batch_missing_result_keep"),
                )
                processed_unique += 1
                render_progress("  -> LLM Country-Check", processed_unique, total_unique)

        for idx, row in df.iterrows():
            cache_key = normalize_address_for_dedupe(str(row.get(address_col, "") or ""))
            is_in_germany, reason = address_country_cache.get(
                cache_key,
                (True, "country_cache_miss_keep"),
            )
            if not is_in_germany:
                non_de_idx.append(idx)
                non_de_reason[idx] = reason or "location_not_in_germany"

        if non_de_idx:
            dropped = df.loc[non_de_idx].copy()
            dropped["remove_reason"] = dropped.index.map(
                lambda i: f"non_german_address_llm:{non_de_reason.get(i, 'location_not_in_germany')}"
            )
            removed_frames.append(dropped)
            df = df.drop(index=non_de_idx).copy()
            print(f"  -> LLM-basiert entfernt: {len(dropped)}")
        else:
            print("  -> LLM-basiert entfernt: 0")
        print(f"  -> Country-Cache unique addresses: {len(address_country_cache)}")
    print(f"  -> Verbleibend: {len(df)}")

    # 3) Address-Format validieren
    print("\n[3/4] Validiere Adressformat ...")
    valid_series = df[address_col].apply(looks_like_valid_address)
    invalid_mask = valid_series.apply(lambda item: not item[0])
    if invalid_mask.any():
        dropped = df[invalid_mask].copy()
        dropped["remove_reason"] = valid_series[invalid_mask].apply(lambda item: f"invalid_address:{item[1]}")
        removed_frames.append(dropped)
        df = df[~invalid_mask].copy()
        print(f"  -> Entfernt: {len(dropped)}")
    else:
        print("  -> Entfernt: 0")
    print(f"  -> Verbleibend: {len(df)}")

    # 4) Domain/Website-Filter
    print("\n[4/4] Pruefe Domain/Website-Kategorien ...")
    if website_col:
        df["_domain_for_check"] = df[website_col].apply(extract_domain)
    elif domain_col:
        df["_domain_for_check"] = df[domain_col].apply(extract_domain)
    else:
        df["_domain_for_check"] = ""

    removed_idx = set()
    remove_reasons = {}
    homepage_cache = {}

    total_category_checks = len(df)
    for pos, (idx, row) in enumerate(df.iterrows(), start=1):
        domain = row.get("_domain_for_check", "")
        name_value = row.get(name_col, "") if name_col else ""
        address_value = row.get(address_col, "")
        website_value = row.get(website_col, "") if website_col else row.get(domain_col, "")

        rule_text = " | ".join(
            str(part or "")
            for part in [domain, name_value, address_value, website_value]
        )
        excluded, category = match_exclusion_rule(rule_text)
        if excluded:
            removed_idx.add(idx)
            remove_reasons[idx] = f"excluded_rule:{category}"
            continue

        if skip_llm:
            continue

        if not domain and not website_value:
            removed_idx.add(idx)
            remove_reasons[idx] = "excluded_rule:missing_domain"
            continue

        cache_key = domain or extract_domain(website_value)
        if cache_key not in homepage_cache:
            homepage_cache[cache_key] = fetch_homepage_text(website_value or domain)
            time.sleep(DELAY_BETWEEN_REQUESTS)
        homepage_excerpt = homepage_cache.get(cache_key, "")

        payload = {
            "name": str(name_value or ""),
            "domain": str(domain or ""),
            "website": str(website_value or ""),
            "address": str(address_value or ""),
            "homepage_excerpt": homepage_excerpt[:2000],
        }
        llm_result = classify_with_llm(llm_client, model, payload)

        if "error" in llm_result:
            # Bei LLM-Fehler konservativ behalten.
            continue

        is_real = bool(llm_result.get("is_real_eventlocation", False))
        excluded_category = str(llm_result.get("excluded_category", "")).strip()
        if not is_real or excluded_category:
            removed_idx.add(idx)
            remove_reasons[idx] = f"excluded_llm:{excluded_category or 'not_real_eventlocation'}"

        render_progress("  -> Domain/Category-Check", pos, total_category_checks)

    if removed_idx:
        dropped = df.loc[sorted(removed_idx)].copy()
        dropped["remove_reason"] = dropped.index.map(remove_reasons.get)
        removed_frames.append(dropped)
        df = df.drop(index=sorted(removed_idx)).copy()
        print(f"  -> Entfernt: {len(dropped)}")
    else:
        print("  -> Entfernt: 0")
    print(f"  -> Verbleibend: {len(df)}")

    # Aufraeumen
    cleanup_cols = [col for col in ["_address_norm", "_domain_for_check"] if col in df.columns]
    df = df.drop(columns=cleanup_cols, errors="ignore")

    removed_df = pd.concat(removed_frames, axis=0) if removed_frames else pd.DataFrame()
    removed_df = removed_df.drop(columns=cleanup_cols, errors="ignore")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    stem = Path(input_csv).stem
    output_csv = output_csv or f"{stem}_eventlocations_clean_{timestamp}.csv"
    removed_csv = removed_csv or f"{stem}_eventlocations_removed_{timestamp}.csv"

    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    removed_df.to_csv(removed_csv, index=False, encoding="utf-8-sig")

    print("\n" + "=" * 64)
    print("FILTER-ZUSAMMENFASSUNG")
    print("=" * 64)
    print(f"Input-Zeilen:            {total_before}")
    print(f"Verbleibende Zeilen:     {len(df)}")
    print(f"Entfernte Zeilen:        {len(removed_df)}")
    if len(removed_df):
        print("\nTop remove_reason:")
        counts = removed_df["remove_reason"].fillna("unknown").value_counts().head(15)
        for reason, count in counts.items():
            print(f"  - {reason:<45} {count}")
    print(f"\nClean CSV:               {output_csv}")
    print(f"Removed CSV:             {removed_csv}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Filtert Leads auf echte Eventlocations in Deutschland."
    )
    parser.add_argument("input_csv", help="Input CSV mit mindestens Spalte 'address'")
    parser.add_argument("--output", default=None, help="Output CSV fuer behaltene Leads")
    parser.add_argument("--removed-output", default=None, help="Output CSV fuer entfernte Leads")
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Modell fuer LLM-Check (Default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--base-url",
        default=LM_STUDIO_BASE_URL,
        help=f"LM Studio URL (Default: {LM_STUDIO_BASE_URL})",
    )
    parser.add_argument(
        "--skip-llm",
        action="store_true",
        help="Nur regelbasiert filtern (ohne LLM-Nachpruefung)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Nur die ersten N Zeilen verarbeiten",
    )

    args = parser.parse_args()
    process_csv(
        input_csv=args.input_csv,
        output_csv=args.output,
        removed_csv=args.removed_output,
        model=args.model,
        base_url=args.base_url,
        skip_llm=args.skip_llm,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
