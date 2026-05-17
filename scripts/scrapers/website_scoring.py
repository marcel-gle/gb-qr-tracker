"""
Website-Analyzer für Eventlocations
====================================
Liest eine CSV mit Spalte "webseite" ein, ruft jede Seite ab,
bereinigt das HTML und lässt ein lokales LLM (LM Studio) die Seite
nach Modernität (1-5) und Buchungstool bewerten.

Voraussetzungen:
    pip install beautifulsoup4 httpx pandas lxml openai
    LM Studio muss laufen mit geladenem Modell (http://localhost:1234)

Verwendung:
    python website_analyzer.py leads.csv
    python website_analyzer.py leads.csv --model openai/gpt-oss-20b
    python website_analyzer.py leads.csv --base-url http://192.168.1.50:1234/v1
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx
import pandas as pd
from bs4 import BeautifulSoup, Comment
from openai import OpenAI
from pandas.errors import ParserError

# ──────────────────────────────────────────────
# Konfiguration
# ──────────────────────────────────────────────
DEFAULT_MODEL = "openai/gpt-oss-20b"
LM_STUDIO_BASE_URL = "http://localhost:1234/v1"
REQUEST_TIMEOUT = 20          # Sekunden pro Webseite
LLM_TIMEOUT = 180             # Sekunden für LLM-Antwort
MAX_HTML_CHARS = 12_000       # Max. Zeichen die ans LLM gehen
DELAY_BETWEEN_REQUESTS = 1.5  # Sekunden Pause zwischen Abrufen

# ──────────────────────────────────────────────
# HTML abrufen
# ──────────────────────────────────────────────
def fetch_html(url: str) -> dict:
    """Ruft eine URL ab und gibt Status-Infos + HTML zurück."""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    result = {
        "url": url,
        "html": None,
        "status_code": None,
        "is_https": url.startswith("https://"),
        "error": None,
        "redirect_url": None,
    }

    try:
        with httpx.Client(
            timeout=REQUEST_TIMEOUT,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "de-DE,de;q=0.9,en;q=0.5",
            },
        ) as client:
            resp = client.get(url)
            result["status_code"] = resp.status_code
            result["html"] = resp.text
            if str(resp.url) != url:
                result["redirect_url"] = str(resp.url)
                result["is_https"] = str(resp.url).startswith("https://")
    except Exception as e:
        result["error"] = str(e)

    return result


# ──────────────────────────────────────────────
# HTML bereinigen
# ──────────────────────────────────────────────
def clean_html(raw_html: str, max_chars: int = MAX_HTML_CHARS) -> str:
    """
    Entfernt allen Ballast aus dem HTML und behält nur das,
    was für eine Bewertung relevant ist:
    - Meta-Tags (viewport, generator, og-tags)
    - Sichtbarer Text mit Struktur (Überschriften, Links, Formulare)
    - Script/Style-Quellen (nicht den Inhalt)
    - Formular-Elemente und iframes (Buchungstool-Hinweise)
    """
    soup = BeautifulSoup(raw_html, "lxml")

    # ── Head-Infos extrahieren ──
    head_info = []

    for meta in soup.find_all("meta"):
        attrs = dict(meta.attrs)
        if any(k in attrs for k in ["name", "property", "http-equiv"]):
            meta_attrs = " ".join(f'{k}="{v}"' for k, v in attrs.items())
            head_info.append(f"<meta {meta_attrs} />")

    if soup.title and soup.title.string:
        head_info.append(f"<title>{soup.title.string.strip()}</title>")

    for tag in soup.find_all("link", rel="stylesheet"):
        if tag.get("href"):
            head_info.append(f'<link rel="stylesheet" href="{tag["href"]}" />')

    for tag in soup.find_all("script", src=True):
        head_info.append(f'<script src="{tag["src"]}"></script>')

    # ── Body bereinigen ──
    body = soup.body if soup.body else soup

    for element in body.find_all(["style", "noscript", "svg", "path"]):
        element.decompose()

    for element in body.find_all("script"):
        if not element.get("src"):
            element.decompose()

    for comment in body.find_all(string=lambda t: isinstance(t, Comment)):
        comment.extract()

    KEEP_ATTRS = {
        "href", "src", "action", "method", "type", "name",
        "class", "id", "placeholder", "alt", "title",
        "data-booking", "data-calendly", "data-widget",
    }
    for tag in body.find_all(True):
        attrs = dict(tag.attrs)
        for attr in attrs:
            if attr not in KEEP_ATTRS:
                del tag[attr]

    # ── Zusammenbauen ──
    cleaned = "=== HEAD INFO ===\n"
    cleaned += "\n".join(head_info[:40])
    cleaned += "\n\n=== BODY CONTENT ===\n"
    cleaned += body.get_text(separator="\n", strip=True)

    # ── Formulare & iframes ──
    forms_iframes = []
    for form in soup.find_all("form"):
        action = form.get("action", "keine")
        inputs = [
            inp.get("type", "text") + ":" + (inp.get("name") or inp.get("placeholder") or "?")
            for inp in form.find_all("input")
        ]
        forms_iframes.append(f"FORM action={action} inputs=[{', '.join(inputs)}]")

    for iframe in soup.find_all("iframe"):
        src = iframe.get("src", "keine src")
        forms_iframes.append(f"IFRAME src={src}")

    if forms_iframes:
        cleaned += "\n\n=== FORMULARE & IFRAMES ===\n"
        cleaned += "\n".join(forms_iframes)

    # ── Buchungsrelevante Links ──
    booking_keywords = [
        "buch", "book", "reserv", "anfrage", "kontakt", "termin",
        "kalend", "calend", "event", "mieten", "miete",
    ]
    relevant_links = []
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        href = a["href"].lower()
        if any(kw in text or kw in href for kw in booking_keywords):
            relevant_links.append(f'LINK text="{a.get_text(strip=True)}" href="{a["href"]}"')

    if relevant_links:
        cleaned += "\n\n=== RELEVANTE LINKS ===\n"
        cleaned += "\n".join(relevant_links[:20])

    if len(cleaned) > max_chars:
        half = max_chars // 2
        cleaned = cleaned[:half] + "\n\n[... GEKÜRZT ...]\n\n" + cleaned[-half:]

    return cleaned


# ──────────────────────────────────────────────
# Copyright-Jahr aus Footer extrahieren
# ──────────────────────────────────────────────
def extract_copyright_year(html: str) -> str | None:
    """Findet das Copyright-Jahr im HTML (oft im Footer)."""
    soup = BeautifulSoup(html, "lxml")
    footer = soup.find("footer") or soup

    text = footer.get_text()
    matches = re.findall(r"©\s*(\d{4})|copyright\s*(\d{4})", text, re.IGNORECASE)
    years = [int(y) for pair in matches for y in pair if y]

    if years:
        return str(max(years))
    return None


# ──────────────────────────────────────────────
# LLM-Analyse via LM Studio (OpenAI-kompatible API)
# ──────────────────────────────────────────────

ANALYSIS_SYSTEM_PROMPT = """Du bist ein Experte für Webdesign und digitale Strategie.
Du analysierst die Webseite einer Eventlocation.

Deine Aufgabe:
1. Bewerte die MODERNITÄT der Webseite auf einer Skala von 1 bis 5:
   1 = Völlig veraltet (Tabellen-Layout, Flash, kein Responsive, sieht aus wie 2005-2010)
   2 = Deutlich veraltet (altes CMS-Theme, kaum responsive, veraltete Technik, ca. 2012-2016)
   3 = Durchschnittlich (funktional aber uninspiriert, Standard-Template, ca. 2017-2020)
   4 = Modern (gutes Design, responsive, aktuelle Technik, zeitgemäß)
   5 = Sehr modern (herausragendes Design, schnelle Technik, perfekte UX)

2. Prüfe ob ein ONLINE-BUCHUNGSTOOL vorhanden ist:
   - Ja: z.B. Calendly, SimplyBook, Bookingkit, regiondo, FairPlaner, eigenes Booking-System,
         Online-Formular mit Datumsauswahl, Verfügbarkeitskalender
   - Nein: Nur Kontaktformular, nur Telefonnummer/E-Mail, "Anfrage per Mail"
   - Unklar: Hinweise vorhanden aber nicht eindeutig

3. Gib eine kurze BEGRÜNDUNG (2-3 Sätze) mit konkreten Beobachtungen.

4. Gib eine VERKAUFSEMPFEHLUNG von 1-5:
   1 = Kein Bedarf (top moderne Seite mit Buchungstool)
   2 = Geringer Bedarf
   3 = Mittlerer Bedarf
   4 = Hoher Bedarf (veraltete Seite ODER fehlendes Buchungstool)
   5 = Sehr hoher Bedarf (veraltete Seite UND fehlendes Buchungstool)

Antworte NUR mit validem JSON in exakt diesem Format, kein anderer Text:
{
  "modernitaet_score": <1-5>,
  "buchungstool_vorhanden": "<ja/nein/unklar>",
  "buchungstool_details": "<Was gefunden wurde oder fehlt>",
  "begruendung": "<2-3 Sätze>",
  "verkaufsempfehlung": <1-5>,
  "erkannte_technologien": ["<tech1>", "<tech2>"]
}"""


def create_llm_client(base_url: str) -> OpenAI:
    """Erstellt einen OpenAI-Client für LM Studio."""
    return OpenAI(
        base_url=base_url,
        api_key="lm-studio",  # LM Studio braucht keinen echten Key
        timeout=LLM_TIMEOUT,
    )


def analyze_with_llm(
    cleaned_html: str, url: str, model: str, client: OpenAI
) -> dict:
    """Sendet bereinigtes HTML an LM Studio und parst die JSON-Antwort."""

    user_prompt = f"""Hier ist der bereinigte Inhalt der Webseite {url}:

---
{cleaned_html}
---

Deine JSON-Analyse:"""

    raw = ""
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": ANALYSIS_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.1,
            max_tokens=600,
        )

        raw = response.choices[0].message.content or ""

        # JSON aus der Antwort extrahieren
        json_match = re.search(r"\{.*\}", raw, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
        else:
            return {"error": "Kein JSON in LLM-Antwort", "raw": raw[:300]}

    except json.JSONDecodeError as e:
        return {"error": f"JSON-Parse-Fehler: {e}", "raw": raw[:300]}
    except Exception as e:
        return {"error": f"LLM-Fehler: {e}"}


# ──────────────────────────────────────────────
# Hauptprogramm
# ──────────────────────────────────────────────
def process_leads(
    input_csv: str,
    model: str,
    base_url: str,
    output_csv: str | None = None,
    limit: int | None = None,
):
    """Verarbeitet die gesamte Lead-Liste."""

    try:
        # sep=None erkennt Trennzeichen wie "," oder ";" automatisch
        df = pd.read_csv(input_csv, sep=None, engine="python")
    except ParserError as e:
        print("FEHLER: CSV konnte nicht eingelesen werden.")
        print("→ Prüfe Trennzeichen/Quotes oder exportiere die Datei als saubere CSV.")
        print(f"→ Parser-Fehler: {e}")
        sys.exit(1)

    if "website" not in df.columns:
        print("FEHLER: CSV muss eine Spalte 'website' enthalten.")
        print(f"Gefundene Spalten: {list(df.columns)}")
        sys.exit(1)

    if limit is not None:
        if limit <= 0:
            print("FEHLER: --limit muss eine positive Zahl sein.")
            sys.exit(1)
        df = df.head(limit)

    # LM Studio Client erstellen
    client = create_llm_client(base_url)

    # Verbindung testen
    print(f"\n  Teste Verbindung zu LM Studio ({base_url})...", end=" ", flush=True)
    try:
        client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "Antworte nur mit: OK"}],
            max_tokens=5,
        )
        print(f"OK ✓  (Modell: {model})")
    except Exception as e:
        print(f"\n\n  FEHLER: Keine Verbindung zu LM Studio!")
        print(f"  → Läuft LM Studio unter {base_url}?")
        print(f"  → Ist das Modell '{model}' geladen?")
        print(f"  → Fehler: {e}\n")
        sys.exit(1)

    total = len(df)
    print(f"\n{'='*60}")
    print(f"  Website-Analyzer für Eventlocations")
    print(f"  {total} Webseiten | Modell: {model}")
    print(f"  LM Studio: {base_url}")
    if limit is not None:
        print(f"  Limit aktiv: nur die ersten {total} Einträge")
    print(f"{'='*60}\n")

    results = []

    for position, (_, row) in enumerate(df.iterrows(), start=1):
        url = str(row["website"]).strip()
        if not url or url == "nan":
            results.append({})
            continue

        print(f"[{position}/{total}] {url}")
        print(f"  ↳ Abrufen...", end=" ", flush=True)

        # 1. HTML abrufen
        fetch_result = fetch_html(url)

        if fetch_result["error"]:
            print(f"FEHLER: {fetch_result['error']}")
            results.append({
                "url_bereinigt": fetch_result["url"],
                "status": "fehler",
                "fehler": fetch_result["error"],
            })
            time.sleep(DELAY_BETWEEN_REQUESTS)
            continue

        print(f"OK ({fetch_result['status_code']})")

        # 2. Copyright-Jahr extrahieren
        copyright_year = extract_copyright_year(fetch_result["html"])

        # 3. HTML bereinigen
        print(f"  ↳ Bereinigen...", end=" ", flush=True)
        cleaned = clean_html(fetch_result["html"])
        print(f"OK ({len(cleaned)} Zeichen)")

        # 4. LLM-Analyse
        print(f"  ↳ LLM-Analyse...", end=" ", flush=True)
        analysis = analyze_with_llm(cleaned, url, model, client)

        if "error" in analysis:
            print(f"FEHLER: {analysis['error']}")
        else:
            score = analysis.get("modernitaet_score", "?")
            booking = analysis.get("buchungstool_vorhanden", "?")
            empfehlung = analysis.get("verkaufsempfehlung", "?")
            print(f"OK → Modernität: {score}/5 | Buchungstool: {booking} | Empfehlung: {empfehlung}/5")

        result = {
            "url_bereinigt": fetch_result["url"],
            "status": "ok",
            "https": fetch_result["is_https"],
            "copyright_jahr": copyright_year,
            "redirect": fetch_result["redirect_url"] or "",
            "modernitaet_score": analysis.get("modernitaet_score"),
            "buchungstool_vorhanden": analysis.get("buchungstool_vorhanden"),
            "buchungstool_details": analysis.get("buchungstool_details"),
            "begruendung": analysis.get("begruendung"),
            "verkaufsempfehlung": analysis.get("verkaufsempfehlung"),
            "erkannte_technologien": ", ".join(analysis.get("erkannte_technologien", [])),
            "fehler": analysis.get("error", ""),
        }
        results.append(result)

        time.sleep(DELAY_BETWEEN_REQUESTS)

    # ── Ergebnisse speichern ──
    if not output_csv:
        stem = Path(input_csv).stem
        output_csv = f"{stem}_analysiert_{datetime.now():%Y%m%d_%H%M}.csv"

    results_df = pd.DataFrame(results)
    output_df = pd.concat([df.reset_index(drop=True), results_df], axis=1)

    if "verkaufsempfehlung" in output_df.columns:
        output_df = output_df.sort_values(
            "verkaufsempfehlung", ascending=False, na_position="last"
        )

    output_df.to_csv(output_csv, index=False, encoding="utf-8-sig")

    # ── Zusammenfassung ──
    print(f"\n{'='*60}")
    print(f"  ZUSAMMENFASSUNG")
    print(f"{'='*60}")

    ok_results = [
        r for r in results if r.get("status") == "ok" and r.get("modernitaet_score")
    ]
    if ok_results:
        avg_modern = sum(r["modernitaet_score"] for r in ok_results) / len(ok_results)
        empf_results = [r for r in ok_results if r.get("verkaufsempfehlung")]
        avg_empf = (
            sum(r["verkaufsempfehlung"] for r in empf_results) / len(empf_results)
            if empf_results
            else 0
        )
        no_booking = sum(
            1 for r in ok_results if r.get("buchungstool_vorhanden") == "nein"
        )
        outdated = sum(
            1 for r in ok_results if (r.get("modernitaet_score") or 5) <= 2
        )

        print(f"  Erfolgreich analysiert:  {len(ok_results)}/{total}")
        print(f"  Ø Modernität:            {avg_modern:.1f}/5")
        print(f"  Ø Verkaufsempfehlung:    {avg_empf:.1f}/5")
        print(
            f"  Ohne Buchungstool:       {no_booking}"
            f" ({no_booking / len(ok_results) * 100:.0f}%)"
        )
        print(
            f"  Deutlich veraltet (≤2):  {outdated}"
            f" ({outdated / len(ok_results) * 100:.0f}%)"
        )

    errors = sum(1 for r in results if r.get("status") == "fehler")
    if errors:
        print(f"  Fehler:                  {errors}")

    print(f"\n  Ergebnisse gespeichert: {output_csv}")
    print(f"  → Sortiert nach Verkaufsempfehlung (beste Leads zuerst)\n")


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analysiert Eventlocation-Webseiten auf Modernität und Buchungstools."
    )
    parser.add_argument("input_csv", help="CSV-Datei mit Spalte 'webseite'")
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"LM Studio Modell-ID (Standard: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--base-url",
        default=LM_STUDIO_BASE_URL,
        help=f"LM Studio API URL (Standard: {LM_STUDIO_BASE_URL})",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Ausgabe-CSV (Standard: <input>_analysiert_<datum>.csv)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Begrenzt die Analyse auf die ersten N Zeilen der CSV",
    )

    args = parser.parse_args()
    process_leads(args.input_csv, args.model, args.base_url, args.output, args.limit)