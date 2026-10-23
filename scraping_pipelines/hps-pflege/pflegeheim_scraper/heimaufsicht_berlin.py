#!/usr/bin/env python3
"""
Berliner Pflegeheime aus den Prüfberichten der Heimaufsicht (LAGeSo) ziehen.

Jeder Prüfbericht nennt auf Seite 2: Name, Adresse, Telefon, E-Mail, Website, Träger,
Einrichtungsart und die Anzahl der angezeigten Plätze. Das Skript sammelt alle PDF-Links,
lädt die Berichte, liest diese Felder aus und behält je Einrichtung den neuesten Bericht.

Schritt 1  Links sammeln:   python heimaufsicht_berlin.py links
Schritt 2  PDFs auswerten:  python heimaufsicht_berlin.py auswerten
Alles in einem:             python heimaufsicht_berlin.py alles

Die PDF-Liste kommt aus der öffentlichen LAGeSo-CSV (Datawrapper auf berlin.de).
Optional lokal gespeicherte HTML-Seiten:
    python heimaufsicht_berlin.py links --html seite1.html seite2.html

Ergebnis:  heimaufsicht_berlin.csv   (alle Einrichtungen, Spalte „ab 80 Plätze“)
Benötigt:  pip install requests beautifulsoup4 pdfplumber
"""
import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
START = "https://www.berlin.de/lageso/soziales/heimaufsicht/pruefberichte/"
CSV_URL = "https://data.lageso.de/lageso/heimaufsicht/csv/Heimaufsicht.csv"
LINKDATEI = os.path.join(BASE_DIR, "heimaufsicht_pdf_links.txt")
CACHE = os.path.join(BASE_DIR, "pdf_cache_heimaufsicht")
OUTPUT = os.path.join(BASE_DIR, "heimaufsicht_berlin.csv")
MAX_SEITEN = 400
PAUSE = 0.5
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
           "Accept-Language": "de-DE,de;q=0.9",
           "Referer": START}
FREEMAIL = {"gmx.de", "gmx.net", "web.de", "t-online.de", "gmail.com", "googlemail.com", "freenet.de",
            "online.de", "yahoo.de", "outlook.de", "hotmail.de", "arcor.de"}
PDF_URL_RE = re.compile(r"https?://[^\s\"'<>|]+\.pdf", re.I)


# ------------------------------------------------------------------ Schritt 1: Links sammeln

def pdf_links_aus_text(text, basis=START):
    """PDF-URLs aus HTML, CSV oder beliebigem Text ziehen."""
    pdfs = set()
    for m in PDF_URL_RE.finditer(text):
        pdfs.add(m.group(0).rstrip(").,;"))
    soup = BeautifulSoup(text, "html.parser")
    seiten = set()
    for a in soup.find_all("a", href=True):
        url = urljoin(basis, a["href"].strip()).split("#")[0]
        if ".pdf" in url.lower():
            pdfs.add(url)
        elif url.startswith(START):
            seiten.add(url)
    return pdfs, seiten


def links_aus_lageso_csv(session):
    """PDF-Links aus der öffentlichen Heimaufsicht-CSV (Quelle hinter dem Datawrapper)."""
    r = session.get(CSV_URL, headers=HEADERS, timeout=60)
    r.raise_for_status()
    # BOM / Encoding
    text = r.content.decode("utf-8-sig", errors="replace")
    pdfs, _ = pdf_links_aus_text(text)
    # Nur echte Prüfberichte (…_P.pdf), keine Gegendarstellungen/Ergänzungen
    pruef = {u for u in pdfs if re.search(r"_P\.pdf$", urlparse(u).path, re.I)}
    return pruef or pdfs


def links_aus_lokaler_csv():
    """Falls eine zuvor heruntergeladene Heimaufsicht_quelle.csv im Skript-Ordner liegt."""
    lokal = os.path.join(BASE_DIR, "Heimaufsicht_quelle.csv")
    if not os.path.exists(lokal):
        return set()
    with open(lokal, encoding="utf-8-sig", errors="replace") as f:
        pdfs, _ = pdf_links_aus_text(f.read())
    pruef = {u for u in pdfs if re.search(r"_P\.pdf$", urlparse(u).path, re.I)}
    return pruef or pdfs


def links_sammeln(html_dateien=None):
    pdfs = set()
    if html_dateien:
        for pfad in html_dateien:
            with open(pfad, encoding="utf-8", errors="replace") as f:
                gefunden, _ = pdf_links_aus_text(f.read(), START)
            print(f"  {pfad}: {len(gefunden)} PDF-Links")
            pdfs |= gefunden
    else:
        s = requests.Session()
        # Primär: LAGeSo-CSV (berlin.de zeigt die Tabelle nur noch per Datawrapper-iframe)
        try:
            print(f"  Lade {CSV_URL} …")
            pdfs = links_aus_lageso_csv(s)
            print(f"  CSV: {len(pdfs)} Prüfbericht-Links")
        except Exception as e:
            print(f"  ✗ CSV nicht erreichbar: {str(e)[:100]}")
            pdfs = links_aus_lokaler_csv()
            if pdfs:
                print(f"  Lokale Heimaufsicht_quelle.csv: {len(pdfs)} Prüfbericht-Links")
            else:
                print("  Fallback: berlin.de-Übersicht crawlen …")
                offen, besucht = [START], set()
                while offen and len(besucht) < MAX_SEITEN:
                    url = offen.pop(0)
                    if url in besucht:
                        continue
                    besucht.add(url)
                    try:
                        r = s.get(url, headers=HEADERS, timeout=30)
                        r.raise_for_status()
                    except Exception as err:
                        print(f"  ✗ {url}: {str(err)[:80]}")
                        if len(besucht) == 1:
                            sys.exit(
                                "Weder CSV noch Übersicht erreichbar. "
                                "Heimaufsicht.csv speichern als Heimaufsicht_quelle.csv im Skript-Ordner "
                                "oder HTML mit --html einlesen."
                            )
                        continue
                    gefunden, seiten = pdf_links_aus_text(r.text, r.url)
                    pdfs |= gefunden
                    offen += sorted(seiten - besucht)
                    sys.stdout.write(f"\r  Seiten besucht: {len(besucht)} | PDF-Links: {len(pdfs)}   ")
                    sys.stdout.flush()
                    time.sleep(PAUSE)
                print()
    with open(LINKDATEI, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(pdfs)))
        if pdfs:
            f.write("\n")
    print(f"{len(pdfs)} PDF-Links gespeichert in {LINKDATEI}")
    if not pdfs:
        print("Hinweis: Keine PDFs gefunden. Bitte die Übersichtsseite im Browser speichern und mit --html einlesen.")


# ------------------------------------------------------------------ Schritt 2: PDFs auswerten

def lade_pdf(session, url):
    os.makedirs(CACHE, exist_ok=True)
    name = re.sub(r"[^\w.\-]", "_", urlparse(url).path.split("/")[-1]) or "bericht.pdf"
    pfad = os.path.join(CACHE, name)
    if not os.path.exists(pfad):
        r = session.get(url, headers=HEADERS, timeout=60)
        r.raise_for_status()
        with open(pfad, "wb") as f:
            f.write(r.content)
        time.sleep(PAUSE)
    return pfad


def pdf_text(pfad, seiten=4):
    import pdfplumber
    with pdfplumber.open(pfad) as pdf:
        return "\n".join((p.extract_text() or "") for p in pdf.pages[:seiten])


def feld(block, label):
    """Wert hinter 'Label:' in derselben Zeile (leer, wenn die Zeile nach dem Doppelpunkt leer ist)."""
    m = re.search(rf"^\s*{label}\s*:[ \t]*(.*)$", block, re.M)
    return m.group(1).strip() if m else ""


def domain_von(wert):
    w = wert.strip().lower()
    if not w:
        return ""
    if "@" in w:
        d = w.split("@")[-1]
        return "" if d in FREEMAIL else d.strip(".")
    w = re.sub(r"^https?://", "", w).split("/")[0]
    return w[4:] if w.startswith("www.") else w


def telefon(t):
    t = t.strip()
    if re.fullmatch(r"[1-9]\d{5,8}", re.sub(r"\D", "", t) or "x") and not t.startswith(("0", "+")):
        return "030 " + t                     # Berliner Nummern stehen oft ohne Vorwahl im Bericht
    return t


def auswerten_text(text, url):
    erg = {"PDF": url, "Status": ""}
    if "Prüfbericht vom" not in text:
        erg["Status"] = "kein Prüfbericht (z. B. Gegendarstellung)"
        return erg
    m = re.search(r"Prüfbericht vom\s*(\d{2}\.\d{2}\.\d{4})", text)
    erg["Prüfdatum"] = m.group(1) if m else ""
    m = re.search(r"Prüfung\s*\n?\s*einer\s+(.+?)\s+nach\s+§", text, re.S)
    erg["Prüfung von"] = " ".join(m.group(1).split()) if m else ""

    a = text.find("Geprüfte Einrichtung")
    b = text.find("Träger der Einrichtung")
    c = text.find("Einrichtungsart")
    if a < 0 or b < 0:
        erg["Status"] = "Seite 2 nicht lesbar"
        return erg
    eb, tb = text[a:b], text[b:c if c > b else b + 600]

    erg["Name"] = feld(eb, "Name")
    erg["Straße"] = feld(eb, "Straße")
    m = re.search(r"^\s*(\d{5})\s+([^,\n]+?)(?:,\s*Bezirk:\s*(.+))?$", eb, re.M)
    erg["PLZ"], erg["Ort"], erg["Bezirk"] = (m.group(1), m.group(2).strip(), (m.group(3) or "").strip()) if m else ("", "", "")
    erg["Telefon"] = telefon(feld(eb, "Telefon"))
    erg["E-Mail"] = feld(eb, "E-Mail-Adresse")
    erg["Website"] = feld(eb, "Internet")

    erg["Träger"] = feld(tb, "Name")
    erg["Träger Anschrift"] = feld(tb, "Anschrift")
    m = re.search(r"^\s*(\d{5}\s+[^\n]+)$", tb, re.M)
    erg["Träger PLZ Ort"] = m.group(1).strip() if m else ""
    erg["Träger Telefon"] = telefon(feld(tb, "Telefon"))
    erg["Träger E-Mail"] = feld(tb, "E-Mail-Adresse")
    erg["Träger Website"] = feld(tb, "Internet")

    erg["Einrichtungsart"] = feld(text, "Einrichtungsart")
    erg["Zielgruppen"] = feld(text, r"Spezialisierungen\s*/\s*besondere\s+Zielgruppen")
    m = re.search(r"Anzahl der angezeigten Plätze\s*:\s*(\d+)", text)
    erg["Plätze"] = int(m.group(1)) if m else ""

    # Domain: Einrichtung vor Träger, Website vor E-Mail
    for quelle, wert in (("Website Einrichtung", erg["Website"]), ("E-Mail Einrichtung", erg["E-Mail"]),
                         ("Website Träger", erg["Träger Website"]), ("E-Mail Träger", erg["Träger E-Mail"])):
        d = domain_von(wert)
        if d:
            erg["Domain"], erg["Domain-Quelle"] = d, quelle
            break
    else:
        erg["Domain"], erg["Domain-Quelle"] = "", ""

    # Kaufsignal: Befund beim Personal (Prüfpunkt 09)
    m = re.search(r"09\s+Ausreichender Personaleinsatz(.*?)(?:\n\s*10\s+Fort|\Z)", text, re.S)
    if m:
        befund = " ".join(m.group(1).split())
        if "nicht Gegenstand" in befund:
            erg["Personal-Befund"] = "nicht geprüft"
        elif "keine Abweichungen" in befund:
            erg["Personal-Befund"] = "keine Abweichungen"
        else:
            # Beschriftung der Tabellenzeile entfernen, nur den Befundtext behalten
            for teil in (r"sowie mit der", r"erforderlichen pers[öo]n", r"lichen und fachlichen", r"Eignung des eingesetzten Personals",
                         r"§\s*17 Abs\. 2 Nr\. 14 WTG.*?WTG-PersV", r"in Verbindung mit", r"Seite \d+ von \d+"):
                befund = re.sub(teil, " ", befund)
            erg["Personal-Befund"] = "ABWEICHUNG: " + " ".join(befund.split())[:220]
    else:
        erg["Personal-Befund"] = ""

    art = erg["Einrichtungsart"].lower() + " " + erg["Prüfung von"].lower()
    erg["Pflegeheim"] = "ja" if ("pflege" in art and not re.search(r"eingliederung|behinderung|wohngemeinschaft|wohnform", art)) else "nein"
    p = erg["Plätze"]
    erg["ab 80 Plätze"] = "ja" if isinstance(p, int) and p >= 80 else ("nein" if isinstance(p, int) else "unbekannt")
    erg["Status"] = "ok"
    return erg


FELDER = ["Name", "Straße", "PLZ", "Ort", "Bezirk", "Telefon", "E-Mail", "Website", "Domain", "Domain-Quelle",
          "Plätze", "ab 80 Plätze", "Pflegeheim", "Einrichtungsart", "Zielgruppen", "Personal-Befund", "Prüfdatum",
          "Träger", "Träger Anschrift", "Träger PLZ Ort", "Träger Telefon", "Träger E-Mail", "Träger Website",
          "Prüfung von", "PDF", "Status"]


def datum(d):
    try:
        return datetime.strptime(d, "%d.%m.%Y")
    except Exception:
        return datetime.min


def auswerten(limit=None):
    if not os.path.exists(LINKDATEI):
        sys.exit(f"{LINKDATEI} fehlt. Bitte zuerst: python heimaufsicht_berlin.py links")
    with open(LINKDATEI, encoding="utf-8") as f:
        links = [l.strip() for l in f if l.strip()]
    if limit:
        links = links[:limit]
    s = requests.Session()
    alle, start = [], time.time()
    for i, url in enumerate(links, 1):
        try:
            erg = auswerten_text(pdf_text(lade_pdf(s, url)), url)
        except Exception as e:
            erg = {"PDF": url, "Status": f"Fehler: {str(e)[:60]}"}
        alle.append(erg)
        rest = (time.time() - start) / i * (len(links) - i)
        balken = "█" * int(30 * i / len(links)) + "░" * (30 - int(30 * i / len(links)))
        sys.stdout.write(f"\r[{balken}] {i}/{len(links)} | noch ca. {int(rest // 60)}:{int(rest % 60):02d} min ")
        sys.stdout.flush()
    print()

    # je Einrichtung nur der neueste Bericht
    neueste = {}
    for e in alle:
        if e.get("Status") != "ok":
            continue
        key = (re.sub(r"\W", "", e["Name"].lower()), re.sub(r"\W", "", e["Straße"].lower()))
        if key not in neueste or datum(e["Prüfdatum"]) > datum(neueste[key]["Prüfdatum"]):
            neueste[key] = e
    zeilen = sorted(neueste.values(), key=lambda e: (e["Pflegeheim"] != "ja", e["ab 80 Plätze"] != "ja", e["Name"]))
    with open(OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FELDER, delimiter=";", extrasaction="ignore")
        w.writeheader()
        w.writerows(zeilen)

    heime = [e for e in zeilen if e["Pflegeheim"] == "ja"]
    treffer = [e for e in heime if e["ab 80 Plätze"] == "ja"]
    print(f"\nBerichte gelesen: {len(alle)} | davon kein Prüfbericht/Fehler: {sum(e.get('Status') != 'ok' for e in alle)}")
    print(f"Einrichtungen (neuester Bericht): {len(zeilen)} | davon Pflegeheime: {len(heime)}")
    print(f"Pflegeheime ab 80 Plätzen: {len(treffer)} | davon mit Domain: {sum(bool(e['Domain']) for e in treffer)}"
          f" | mit Personal-Abweichung: {sum(e['Personal-Befund'].startswith('ABWEICHUNG') for e in treffer)}")
    print(f"Ergebnis: {OUTPUT}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("schritt", choices=["links", "auswerten", "alles"])
    p.add_argument("--html", nargs="+", help="lokal gespeicherte Übersichtsseiten statt Live-Abruf")
    p.add_argument("--limit", type=int, help="nur die ersten N PDFs (zum Testen)")
    a = p.parse_args()
    if a.schritt in ("links", "alles"):
        links_sammeln(a.html)
    if a.schritt in ("auswerten", "alles"):
        auswerten(a.limit)


if __name__ == "__main__":
    main()
