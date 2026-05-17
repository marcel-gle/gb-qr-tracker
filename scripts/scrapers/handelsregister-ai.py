"""
Pensionsrückstellungs-Scanner via handelsregister.ai API

Anleitung:
1. pip install requests
2. API-Key von handelsregister.ai holen (500 Credits kostenlos)
3. Firmenliste als CSV bereitstellen (Spalte "firma" und optional "ort")
4. Starten:
   python pensionsscanner.py --key DEIN_API_KEY --csv firmen.csv
   
   Oder ohne CSV mit Beispielfirmen:
   python pensionsscanner.py --key DEIN_API_KEY --demo

Credit-Verbrauch pro Firma: ~15-21 Credits (je nach Features)
"""

import argparse
import csv
from datetime import datetime, timezone
import io
import json
import sys
import time
from pathlib import Path

import requests

API_BASE = "https://handelsregister.ai/api/v1"

# Suchbegriffe für Pensionsrückstellungen in Bilanzdaten
PENSION_KEYWORDS = [
    "pension", "pensionsrückstellung", "pensionsverpflichtung",
    "altersversorgung", "altersvorsorge", "betriebliche altersversorgung",
    "direktzusage", "pensionszusage", "versorgungszusage",
    "rückstellung für pensionen", "ähnliche verpflichtungen",
]

DEMO_FIRMEN = [
    {"firma": "Sixt SE", "ort": "München"},
    {"firma": "Rational AG", "ort": "Landsberg am Lech"},
    {"firma": "Fielmann Group AG", "ort": "Hamburg"},
]


def _mask_api_key(api_key: str) -> str:
    if len(api_key) <= 8:
        return "*" * len(api_key)
    return f"{api_key[:4]}...{api_key[-4:]}"


def _append_api_log(api_log_path: Path | None, payload: dict) -> None:
    """Schreibt einen API-Aufruf als JSON-Zeile in die Logdatei."""
    if not api_log_path:
        return

    api_log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(api_log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")


def fetch_organization(
    api_key: str,
    firma: str,
    ort: str = "",
    api_log_path: Path | None = None,
    print_api: bool = False,
) -> dict:
    """Ruft Firmendaten mit Bilanz- und Jahresabschluss-Features ab."""
    query = f"{firma} aus {ort}" if ort else firma

    params = {
        "q": query,
        "feature": [
            "balance_sheet_accounts",
            "financial_kpi",
            "annual_financial_statements",
        ],
        "ai_search": "on-default",
    }
    headers = {"x-api-key": api_key, "Accept": "application/json"}
    url = f"{API_BASE}/fetch-organization"

    resp = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=60,
    )
    response_text = resp.text
    response_json = None
    try:
        response_json = resp.json()
    except ValueError:
        response_json = None

    log_payload = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "endpoint": "fetch-organization",
        "method": "GET",
        "firma": firma,
        "ort": ort,
        "request": {
            "url": url,
            "params": params,
            "headers": {
                "Accept": headers["Accept"],
                "x-api-key": _mask_api_key(api_key),
            },
        },
        "response": {
            "status_code": resp.status_code,
            "ok": resp.ok,
            "headers": dict(resp.headers),
            "text": response_text,
            "json": response_json,
        },
    }
    _append_api_log(api_log_path, log_payload)

    if print_api:
        print("\n  🔎 API-Request")
        print(json.dumps(log_payload["request"], ensure_ascii=False, indent=2))
        print("  🔎 API-Response")
        print(json.dumps(log_payload["response"], ensure_ascii=False, indent=2, default=str))

    resp.raise_for_status()
    if response_json is None:
        raise ValueError("API-Antwort ist kein valides JSON.")
    return response_json


def suche_in_bilanz(data: dict) -> dict:
    """
    Durchsucht balance_sheet_accounts nach Pensionsrückstellungen.
    Gibt gefundene Positionen mit Beträgen zurück.
    """
    ergebnis = {"gefunden": False, "positionen": [], "jahre": []}

    bilanz = data.get("balance_sheet_accounts")
    if not bilanz:
        return ergebnis

    # Rekursiv alle Bilanzpositionen durchsuchen
    def durchsuche(obj, pfad=""):
        if isinstance(obj, dict):
            for key, val in obj.items():
                aktueller_pfad = f"{pfad} > {key}" if pfad else key
                key_lower = key.lower()

                # Prüfe ob der Schlüssel ein Pensions-Keyword enthält
                if any(kw in key_lower for kw in PENSION_KEYWORDS):
                    ergebnis["gefunden"] = True
                    ergebnis["positionen"].append({
                        "position": aktueller_pfad,
                        "wert": val,
                    })
                else:
                    durchsuche(val, aktueller_pfad)

        elif isinstance(obj, list):
            for item in obj:
                durchsuche(item, pfad)

    durchsuche(bilanz)
    return ergebnis


def suche_in_jahresabschluss(data: dict) -> dict:
    """
    Durchsucht annual_financial_statements (Volltext/Markdown)
    nach Pensionsrückstellungs-Erwähnungen.
    """
    ergebnis = {"gefunden": False, "textstellen": []}

    statements = data.get("annual_financial_statements")
    if not statements:
        return ergebnis

    # Kann ein einzelnes Objekt oder eine Liste sein
    if isinstance(statements, dict):
        statements = [statements]

    for stmt in statements:
        # Volltext kann in verschiedenen Feldern liegen
        text = ""
        if isinstance(stmt, dict):
            text = stmt.get("content", "") or stmt.get("text", "") or str(stmt)
        elif isinstance(stmt, str):
            text = stmt

        text_lower = text.lower()

        for kw in PENSION_KEYWORDS:
            pos = text_lower.find(kw)
            while pos != -1:
                # Kontext extrahieren: 120 Zeichen um den Treffer
                start = max(0, pos - 60)
                end = min(len(text), pos + len(kw) + 60)
                kontext = text[start:end].replace("\n", " ").strip()

                ergebnis["gefunden"] = True
                ergebnis["textstellen"].append({
                    "keyword": kw,
                    "kontext": f"...{kontext}...",
                })

                # Nächsten Treffer für dieses Keyword suchen
                pos = text_lower.find(kw, pos + len(kw))

            # Max 3 Treffer pro Keyword
            if len(ergebnis["textstellen"]) > 10:
                break

    # Deduplizieren
    seen = set()
    unique = []
    for t in ergebnis["textstellen"]:
        if t["kontext"] not in seen:
            seen.add(t["kontext"])
            unique.append(t)
    ergebnis["textstellen"] = unique[:8]

    return ergebnis


def extrahiere_kpis(data: dict) -> dict:
    """Zieht Mitarbeiterzahl, Umsatz und andere KPIs raus."""
    kpis = {
        "mitarbeiter": None,
        "umsatz_eur": None,
        "jahresüberschuss_eur": None,
        "bilanzsumme_eur": None,
        "jahr": None,
    }

    financial_kpi = data.get("financial_kpi")
    if not financial_kpi:
        return kpis

    # financial_kpi kann eine Liste (nach Jahr) oder ein Dict sein
    entries = financial_kpi if isinstance(financial_kpi, list) else [financial_kpi]

    if not entries:
        return kpis

    # Neuestes Jahr nehmen
    aktuell = entries[0] if entries else {}
    if isinstance(aktuell, dict):
        kpis["mitarbeiter"] = aktuell.get("employees") or aktuell.get("mitarbeiter")
        kpis["umsatz_eur"] = aktuell.get("revenue") or aktuell.get("umsatz")
        kpis["jahresüberschuss_eur"] = aktuell.get("net_income") or aktuell.get("jahresueberschuss")
        kpis["bilanzsumme_eur"] = aktuell.get("total_assets") or aktuell.get("bilanzsumme")
        kpis["jahr"] = aktuell.get("year") or aktuell.get("jahr")

    return kpis


def bewerte_lead(kpis: dict, bilanz_ergebnis: dict, volltext_ergebnis: dict) -> dict:
    """
    Bewertet die Firma als Lead für bAV-Beratung.
    
    Hot Lead:  Keine Pensionsrückstellung bei 20+ MA
    Warm Lead: Pensionsrückstellung vorhanden (ggf. unterfinanziert)
    Cold:      Zu klein, keine Daten, oder bereits gut versorgt
    """
    score = 0
    gruende = []
    ma = kpis.get("mitarbeiter")

    # Mitarbeiter-Check
    if ma is not None:
        if 20 <= ma <= 250:
            score += 30
            gruende.append(f"Sweet Spot: {ma} Mitarbeiter")
        elif ma > 250:
            score += 10
            gruende.append(f"Großes Unternehmen: {ma} MA")
        else:
            gruende.append(f"Kleines Unternehmen: {ma} MA")

    # Pensionsrückstellungs-Check
    if bilanz_ergebnis["gefunden"]:
        score += 20
        gruende.append("Pensionsrückstellung in Bilanz gefunden → Prüfe Deckung")
    elif volltext_ergebnis["gefunden"]:
        score += 15
        gruende.append("Altersversorgung im Anhang erwähnt → Details prüfen")
    else:
        if ma and ma >= 20:
            score += 40
            gruende.append("KEINE Pensionsrückstellung bei 20+ MA → Hot Lead!")
        else:
            gruende.append("Keine Pensionsdaten gefunden")

    # Bewertung
    if score >= 60:
        kategorie = "🔥 HOT"
    elif score >= 30:
        kategorie = "🟡 WARM"
    else:
        kategorie = "⚪ COLD"

    return {"score": score, "kategorie": kategorie, "gruende": gruende}


def analysiere_firma(
    api_key: str,
    firma: str,
    ort: str = "",
    api_log_path: Path | None = None,
    print_api: bool = False,
) -> dict:
    """Komplette Analyse einer Firma."""
    print(f"\n{'='*60}")
    print(f"  Analysiere: {firma}" + (f" ({ort})" if ort else ""))
    print(f"{'='*60}")

    try:
        data = fetch_organization(
            api_key,
            firma,
            ort,
            api_log_path=api_log_path,
            print_api=print_api,
        )
    except requests.HTTPError as e:
        print(f"  ❌ API-Fehler: {e}")
        return {"firma": firma, "fehler": str(e)}
    except ValueError as e:
        print(f"  ❌ Antwort konnte nicht verarbeitet werden: {e}")
        return {"firma": firma, "fehler": str(e)}

    # Basisdaten
    name = data.get("name", firma)
    rechtsform = data.get("legal_form", "?")
    status = data.get("status", "?")
    adresse = data.get("address", {})

    print(f"  Name:       {name}")
    print(f"  Rechtsform: {rechtsform}")
    print(f"  Status:     {status}")

    # KPIs
    kpis = extrahiere_kpis(data)
    if kpis["mitarbeiter"]:
        print(f"  Mitarbeiter: {kpis['mitarbeiter']} ({kpis['jahr']})")
    if kpis["umsatz_eur"]:
        print(f"  Umsatz:      {kpis['umsatz_eur']:,.0f} EUR")

    # Bilanz-Suche
    bilanz = suche_in_bilanz(data)
    if bilanz["gefunden"]:
        print(f"\n  📊 Pensionsrückstellung in Bilanz GEFUNDEN:")
        for pos in bilanz["positionen"][:5]:
            print(f"     → {pos['position']}: {pos['wert']}")
    else:
        print(f"\n  📊 Keine Pensionsrückstellung in Bilanzdaten")

    # Volltext-Suche
    volltext = suche_in_jahresabschluss(data)
    if volltext["gefunden"]:
        print(f"\n  📝 Altersversorgung im Jahresabschluss erwähnt:")
        for t in volltext["textstellen"][:3]:
            print(f"     → [{t['keyword']}] {t['kontext']}")
    else:
        print(f"\n  📝 Keine Erwähnung im Jahresabschluss-Volltext")

    # Bewertung
    bewertung = bewerte_lead(kpis, bilanz, volltext)
    print(f"\n  {bewertung['kategorie']}  Score: {bewertung['score']}")
    for grund in bewertung["gruende"]:
        print(f"     • {grund}")

    return {
        "firma": name,
        "rechtsform": rechtsform,
        "status": status,
        "kpis": kpis,
        "pensionsrueckstellung_bilanz": bilanz,
        "pensionsrueckstellung_volltext": volltext,
        "bewertung": bewertung,
        "rohdaten_keys": list(data.keys()),
    }


def _decode_csv_text(csv_pfad: str) -> str:
    """CSV-Text dekodieren: UTF-8, dann typische Windows/Excel-Zeichensätze."""
    raw = Path(csv_pfad).read_bytes()
    for enc in ("utf-8-sig", "utf-8", "cp1252", "iso-8859-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1")


def lade_firmenliste(csv_pfad: str) -> list[dict]:
    """Lädt Firmenliste aus CSV (Spalten: firma, ort)."""
    firmen = []
    text = _decode_csv_text(csv_pfad)
    f = io.StringIO(text)

    reader = csv.DictReader(f, delimiter=";")

    # Fallback: Komma als Delimiter
    f.seek(0)
    first_line = f.readline()
    f.seek(0)
    if ";" in first_line:
        reader = csv.DictReader(f, delimiter=";")
    else:
        reader = csv.DictReader(f, delimiter=",")

    for row in reader:
        # Flexible Spaltennamen
        firma = (
            row.get("firma") or row.get("Firma") or
            row.get("name") or row.get("Name") or
            row.get("company") or row.get("Company") or ""
        ).strip()

        ort = (
            row.get("ort") or row.get("Ort") or
            row.get("city") or row.get("City") or
            row.get("stadt") or row.get("Stadt") or ""
        ).strip()

        if firma:
            firmen.append({"firma": firma, "ort": ort})

    return firmen


def main():
    parser = argparse.ArgumentParser(
        description="Pensionsrückstellungs-Scanner via handelsregister.ai"
    )
    parser.add_argument("--key", required=True, help="handelsregister.ai API-Key")
    parser.add_argument("--csv", help="CSV-Datei mit Firmenliste (Spalten: firma, ort)")
    parser.add_argument("--demo", action="store_true", help="Demo mit 3 Beispielfirmen")
    parser.add_argument("--output", default="scan_ergebnis.json", help="Output-JSON")
    parser.add_argument("--pause", type=float, default=2.0, help="Pause zwischen Abrufen (Sek.)")
    parser.add_argument(
        "--api-log",
        default="output/handelsregister_api_calls.jsonl",
        help="Pfad für API-Logs als JSONL (ein Eintrag pro Aufruf).",
    )
    parser.add_argument(
        "--print-api",
        action="store_true",
        help="API-Request/Response zusätzlich auf der Konsole ausgeben.",
    )

    args = parser.parse_args()
    api_log_path = Path(args.api_log) if args.api_log else None

    # Firmenliste bestimmen
    if args.csv:
        firmen = lade_firmenliste(args.csv)
        print(f"📂 {len(firmen)} Firmen aus {args.csv} geladen")
    elif args.demo:
        firmen = DEMO_FIRMEN
        print(f"🧪 Demo-Modus: {len(firmen)} Beispielfirmen")
    else:
        print("Fehler: Gib --csv DATEI oder --demo an")
        sys.exit(1)

    # Geschätzter Credit-Verbrauch
    geschaetzt = len(firmen) * 18  # ~18 Credits pro Firma
    print(f"💳 Geschätzter Verbrauch: ~{geschaetzt} Credits")
    print(f"   (500 Gratis-Credits reichen für ~{500 // 18} Firmen)")

    antwort = input(f"\n{len(firmen)} Firmen scannen? (j/n): ").strip().lower()
    if antwort != "j":
        print("Abgebrochen.")
        sys.exit(0)

    # Scan durchführen
    ergebnisse = []
    for i, firma in enumerate(firmen):
        result = analysiere_firma(
            args.key,
            firma["firma"],
            firma.get("ort", ""),
            api_log_path=api_log_path,
            print_api=args.print_api,
        )
        ergebnisse.append(result)

        if i < len(firmen) - 1:
            time.sleep(args.pause)

    # Zusammenfassung
    print(f"\n\n{'='*60}")
    print(f"  ZUSAMMENFASSUNG")
    print(f"{'='*60}")

    hot = [e for e in ergebnisse if e.get("bewertung", {}).get("kategorie", "").startswith("🔥")]
    warm = [e for e in ergebnisse if e.get("bewertung", {}).get("kategorie", "").startswith("🟡")]
    cold = [e for e in ergebnisse if e.get("bewertung", {}).get("kategorie", "").startswith("⚪")]

    print(f"  🔥 Hot Leads:  {len(hot)}")
    for e in hot:
        print(f"     → {e['firma']}")
    print(f"  🟡 Warm Leads: {len(warm)}")
    for e in warm:
        print(f"     → {e['firma']}")
    print(f"  ⚪ Cold:       {len(cold)}")

    # Ergebnisse speichern
    output_path = Path(args.output)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(ergebnisse, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n💾 Ergebnisse gespeichert: {output_path}")


if __name__ == "__main__":
    main()