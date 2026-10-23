#!/usr/bin/env python3
"""
Ansprechpartner zu Kliniken aus deren Websites ziehen (Version 2).

Liest die Leadliste (XLSX), besucht pro Haus das Impressum und Leitungsseiten und
extrahiert Geschäftsführung, Pflegedirektion und Personalleitung.

Aufruf:   python ansprechpartner_scraper.py Leadliste.xlsx
Optionen: --blatt "1 Kommunal KH"   nur ein Blatt
          --prio "A hoch"           nur eine Prioritätsstufe
          --limit 50                nur die ersten N Häuser (zum Testen)
          --workers 6               parallele Abfragen
          --neu                     vorhandene ansprechpartner.csv ignorieren und neu anfangen
          --browser                 JavaScript-Seiten per Playwright nachladen (optional)
Debug:    python ansprechpartner_scraper.py --debug https://www.beispiel.de/impressum
          zeigt den bereinigten Text und alle Treffer mit Begründung

Ausgabe:  ansprechpartner.csv  (wird laufend fortgeschrieben, Neustart setzt fort)
Benötigt: pip install requests beautifulsoup4 pandas openpyxl gender-guesser
Optional: pip install playwright && playwright install chromium   (für --browser)
"""
import argparse
import csv
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    import gender_guesser.detector as _gg
    _VORNAMEN = _gg.Detector(case_sensitive=False)
except ImportError:
    sys.exit("Bitte zuerst installieren: pip install gender-guesser")

OUTPUT = "ansprechpartner-full.csv"
TIMEOUT = 20
PAUSE = 0.5
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; lead-research/1.0)",
           "Accept-Language": "de-DE,de;q=0.9"}
FENSTER = 160          # max. Zeichen zwischen Label und Name
MAX_ZEILEN = 4         # max. Zeilenumbrüche zwischen Label und Name

# ------------------------------------------------------------------ Namenserkennung

ANREDE = r"(?:(?:Herrn?|Frau)[ \t]+)?"
TITEL = (r"(?:(?:Prof|Dr|Dipl|PD|MBA|M\.A|B\.A|M\.Sc|B\.Sc|LL\.M)\.?(?:-?[ \t]*(?:med|dent|rer\.?[ \t]*\w+|jur|oec|phil|"
         r"nat|habil|h\.c|Ing|Kfm|Kffr|Kauffr|Betriebsw|Pflegew|Volksw|Psych|Inf|Soz\.?\s*Päd)\.?)*,?[ \t]+){0,5}")
WORT = r"[A-ZÄÖÜ][a-zäöüß]+(?:-[A-ZÄÖÜ][a-zäöüß]+)?"
PARTIKEL = r"(?:(?:von|van|de|der|den|zu|zur|vom)[ \t]+){0,2}"
# Vorname [Zweitname] [Partikel] Nachname; Zeilenumbrüche innerhalb eines Namens sind nicht erlaubt
NAME = re.compile(rf"{ANREDE}({TITEL})({WORT})[ \t]+(?:({WORT})[ \t]+)?{PARTIKEL}({WORT})")


def ist_vorname(wort):
    """Prüft den ersten Teil (bei Doppelnamen wie Hans-Peter den ersten Bestandteil)."""
    teil = wort.split("-")[0]
    return _VORNAMEN.get_gender(teil) != "unknown"


# Wörter, die direkt hinter einem Namen stehen können, aber nie ein Nachname sind
KEIN_NACHNAME = re.compile(
    r"^(?:Gesch[äa]ftsf[üu]hr\w*|Direktor\w*|Vorstand\w*|Vorsitz\w*|Leit(?:er|erin|ung)\w*|Pflege\w*|Personal\w*|"
    r"Kaufm\w*|Ärztlich\w*|Prokurist\w*|Sprecher\w*|Telefon|Tel|Fax|Mail|Registergericht|Amtsgericht|Sitz|Stadt|"
    r"Landkreis|Klinik\w*|Krankenhaus\w*|Gesellschaft\w*|Stellvertret\w*|Chefarzt\w*|Chefärzt\w*)$")


def namen_finden(text, maximal=4):
    """Personennamen finden; das erste Wort muss ein echter Vorname sein.

    Ein abgelehnter Treffer überspringt nur sein erstes Wort, damit z. B. in
    'Geschäftsführerin Anna Beispiel' der Vorname 'Anna' nicht mitverschluckt wird.
    """
    gefunden, pos = [], 0
    while len(gefunden) < maximal:
        m = NAME.search(text, pos)
        if not m:
            break
        vor, mitte, nach = m.group(2), m.group(3), m.group(4)
        if not ist_vorname(vor):
            pos = m.start(2) + len(vor)          # nur das erste Wort überspringen
            continue
        roh = re.sub(r"^(?:Herrn?|Frau)\s+", "", " ".join(m.group(0).split()))
        if KEIN_NACHNAME.match(nach):
            if mitte and not KEIN_NACHNAME.match(mitte):
                roh = roh[: roh.rfind(nach)].strip()     # 'Georg Schmidt Geschäftsführer' -> 'Georg Schmidt'
            else:
                pos = m.start(2) + len(vor)
                continue
        if roh not in gefunden:
            gefunden.append(roh)
        pos = m.end()
    return gefunden


# ------------------------------------------------------------------ Labels

GF_LABEL = re.compile(
    r"(?P<lab>Gesch[äa]ftsf[üu]hr(?:er|erin|ung|erinnen|ende[rn]?)?(?:\s+Gesellschafter(?:in)?)?|"
    r"[Vv]ertreten\s+durch(?:\s+(?:den|die|das|ihre[n]?|seine[n]?))?(?:\s+\w+){0,3}?|"
    r"Vertretungsberechtigt(?:e[rn]?)?|Vorstand(?:svorsitzende[rn]?)?|Vorst[äa]nde|Alleinvorstand|"
    r"Kaufm[äa]nnische[rn]?\s+Direktor(?:in)?|Werkleit(?:er|erin|ung))",
)
# Gewichtung: echte GF-Bezeichnungen vor Vorstand
GF_GEWICHT = [(re.compile(r"Gesch[äa]ftsf|[Vv]ertretungsberecht|[Vv]ertreten"), 3),
              (re.compile(r"Vorst|Werkleit"), 2),
              (re.compile(r"Kaufm"), 1)]

ROLLEN = {
    "Pflegedirektion": re.compile(
        r"Pflegedirektor(?:in)?|Pflegedirektion|Pflegedienstleit(?:er|erin|ung)|Direktor(?:in)?\s+(?:f[üu]r\s+)?Pflege\w*|"
        r"Leit(?:er|erin|ung)\s+(?:des\s+)?Pflege(?:dienst|management)\w*|Pflegevorst(?:and|[äa]ndin)|Chief\s+Nursing\s+Officer"),
    "Personalleitung": re.compile(
        r"Personalleit(?:er|erin|ung)|Leit(?:er|erin|ung)\s+(?:des\s+|der\s+)?(?:Personal\w*|Abteilung\s+Personal)|"
        r"Personaldirektor(?:in)?|Leit(?:er|erin|ung)\s+(?:Human\s+Resources|HR)|Bereichsleit(?:er|erin|ung)\s+Personal|"
        r"Personalchef(?:in)?|Head\s+of\s+HR"),
}

# Ein Label endet am nächsten Label: verhindert, dass Namen aus dem Folgeblock übernommen werden
NAECHSTES_LABEL = re.compile(
    r"Registergericht|Amtsgericht|Handelsregister|Registernummer|HRB|Umsatzsteuer|USt|Steuernummer|Aufsichtsrat|"
    r"Verantwortlich|Inhaltlich|Datenschutzbeauftragt|Redaktion|Aufsichtsbeh[öo]rde|Kammer|Berufsbezeichnung|"
    r"Ärztliche[rn]?\s+Direktor|Pflegedirektor|Personalleit|Telefon|Fax|E-?Mail|Bildnachweis|Haftung", re.I)

# ------------------------------------------------------------------ HTML-Bereinigung

RAUSCH_TAGS = ["script", "style", "noscript", "nav", "header", "footer", "aside", "form", "svg", "iframe", "button"]
RAUSCH_KLASSEN = re.compile(
    r"nav|menu|megamenu|breadcrumb|header|footer|sidebar|cookie|consent|banner|teaser|social|search|"
    r"skip|toolbar|offcanvas|flyout|dropdown|quicklinks|related", re.I)
INHALT_SELEKTOREN = ["main", "article", "[role=main]", "#content", "#main", "#inhalt", ".content", ".main-content", ".page-content"]


def bereinigter_text(soup):
    """Navigation, Kopf, Fuß und Menüs entfernen, dann den Hauptinhalt bevorzugen."""
    for tag in soup(RAUSCH_TAGS):
        tag.decompose()
    for tag in soup.find_all(attrs={"role": re.compile("navigation|banner|contentinfo|menu", re.I)}):
        tag.decompose()
    for tag in list(soup.find_all(True)):
        if getattr(tag, "decomposed", False):
            continue
        attrs = getattr(tag, "attrs", None)
        if not attrs:
            continue
        kennung = " ".join(attrs.get("class", []) if isinstance(attrs.get("class"), list) else [str(attrs.get("class", ""))])
        kennung += " " + str(attrs.get("id", ""))
        if kennung.strip() and RAUSCH_KLASSEN.search(kennung) and tag.name not in ("body", "html", "main"):
            tag.decompose()
    for sel in INHALT_SELEKTOREN:
        knoten = soup.select_one(sel)
        if knoten and len(knoten.get_text(strip=True)) > 200:
            soup = knoten
            break
    text = soup.get_text("\n")
    text = text.replace("\xa0", " ").replace("­", "")      # geschützte Leerzeichen, weiche Trennstriche
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text


def ausschnitt_nach(text, pos):
    """Text direkt nach einem Label: max. FENSTER Zeichen, max. MAX_ZEILEN Zeilen, bis zum nächsten Label."""
    stueck = text[pos:pos + FENSTER]
    stueck = "\n".join(stueck.split("\n")[:MAX_ZEILEN + 1])
    m = NAECHSTES_LABEL.search(stueck)
    return stueck[:m.start()] if m else stueck


def ausschnitt_vor(text, pos):
    """Für Layouts 'Name \\n Funktion': die Zeile direkt vor dem Label."""
    zeilen = text[max(0, pos - 120):pos].rstrip().split("\n")
    return zeilen[-1] if zeilen else ""


def gf_suchen(text, debug=False):
    kandidaten = []
    for m in GF_LABEL.finditer(text):
        label = m.group("lab")
        gewicht = next((g for rx, g in GF_GEWICHT if rx.search(label)), 1)
        namen = namen_finden(ausschnitt_nach(text, m.end()))
        if not namen:                                    # Layout: 'Dr. Anna Beispiel' \n 'Geschäftsführerin'
            namen = namen_finden(ausschnitt_vor(text, m.start()), 1)
            gewicht -= 0.5
        if debug:
            print(f"  Label {label!r:40} -> {namen or '-'}")
        if namen:
            kandidaten.append((gewicht, m.start(), namen, label))
    if not kandidaten:
        return [], ""
    kandidaten.sort(key=lambda k: (-k[0], k[1]))
    return kandidaten[0][2], kandidaten[0][3]


def rollen_suchen(text, ergebnis, debug=False):
    for rolle, rx in ROLLEN.items():
        if ergebnis.get(rolle):
            continue
        for m in rx.finditer(text):
            namen = namen_finden(ausschnitt_nach(text, m.end()), 1) or namen_finden(ausschnitt_vor(text, m.start()), 1)
            if debug:
                print(f"  {rolle:16} Label {m.group(0)!r:35} -> {namen or '-'}")
            if namen:
                ergebnis[rolle] = namen[0]
                break


# ------------------------------------------------------------------ HTTP

_browser_lock = threading.Lock()
_BROWSER = {"pw": None, "b": None}


def per_browser(url):
    """Optional: Seite mit Chromium rendern (für JavaScript-Seiten)."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None
    with _browser_lock:
        try:
            with sync_playwright() as pw:
                b = pw.chromium.launch()
                s = b.new_page(user_agent=HEADERS["User-Agent"])
                s.goto(url, timeout=TIMEOUT * 1000, wait_until="networkidle")
                html = s.content()
                b.close()
                return BeautifulSoup(html, "html.parser")
        except Exception:
            return None


def hole(session, url):
    try:
        r = session.get(url, timeout=TIMEOUT, headers=HEADERS, allow_redirects=True)
        if r.status_code != 200 or "html" not in r.headers.get("Content-Type", "").lower():
            return None, None
        # Kodierung: Server-Angabe bzw. <meta charset> vertrauen; nur ohne Angabe raten lassen
        if "charset" in r.headers.get("Content-Type", "").lower():
            soup = BeautifulSoup(r.text, "html.parser")
        else:
            soup = BeautifulSoup(r.content, "html.parser")   # liest <meta charset>
        return soup, r.url
    except Exception:
        return None, None


def domain_kern(url):
    """www.klinikum-x.de -> klinikum-x.de (für den Domainvergleich)."""
    host = urlparse(url).netloc.lower().split(":")[0]
    return host[4:] if host.startswith("www.") else host


def finde_links(soup, basis, worte, maximal=4, fremde_domains=False):
    out = []
    for a in soup.find_all("a", href=True):
        ziel = a["href"].strip()
        if ziel.startswith(("mailto:", "tel:", "javascript:", "#")):
            continue
        probe = (ziel + " " + a.get_text(" ")).lower()
        if any(w in probe for w in worte):
            voll = urljoin(basis, ziel).split("#")[0]
            if (fremde_domains or domain_kern(voll) == domain_kern(basis)) and voll not in out:
                out.append(voll)
        if len(out) >= maximal:
            break
    return out


LINK_IMPRESSUM = ("impressum", "imprint", "anbieterkennzeichnung", "legal-notice")
LINK_LEITUNG = ("leitung", "direktorium", "vorstand", "geschaeftsfuehrung", "geschäftsführung", "klinikleitung",
                "betriebsleitung", "ueber-uns", "über-uns", "ueberuns", "wir-ueber-uns", "unternehmen", "organisation",
                "ansprechpartner", "pflege", "pflegedirektion", "personal", "verwaltung", "team")

FELDER = ["KH-ID", "Bundesland", "Krankenhaus", "Ort", "Website", "Geschäftsführung", "Weitere GF", "GF-Label",
          "Pflegedirektion", "Personalleitung", "Impressum-URL", "Status"]


def bearbeite(zeile, browser=False):
    website = zeile["Website"]
    erg = {k: "" for k in FELDER}
    erg.update({"KH-ID": zeile.get("KH-ID (Destatis)", ""), "Bundesland": zeile.get("Bundesland", ""),
                "Krankenhaus": zeile["Krankenhaus"], "Ort": zeile.get("Ort", ""), "Website": website})
    if not website:
        erg["Status"] = "keine Website"
        return erg

    session = requests.Session()
    start, basis = hole(session, website)
    if start is None:
        erg["Status"] = "Startseite nicht erreichbar"
        return erg
    # basis = tatsächliche URL nach Weiterleitung (z. B. klinikum-ludwigsburg.de -> rkh-gesundheit.de)

    # 1) Impressum: verlinktes Impressum darf auf einer anderen Domain liegen (Konzern-Impressum)
    kandidaten = finde_links(start, basis, LINK_IMPRESSUM, 3, fremde_domains=True)
    for b in dict.fromkeys([basis, website]):
        kandidaten += [urljoin(b, p) for p in ("/impressum", "/impressum/", "/impressum.html", "/de/impressum")]
    impressum_text = None
    for url in dict.fromkeys(kandidaten):
        soup, echte_url = hole(session, url)
        if soup is None:
            continue
        roh = soup.get_text(" ")
        if not re.search(r"mpressum|nbieterkennzeichnung|Angaben gem[äa](?:ß|ss)", roh):
            continue
        txt = bereinigter_text(soup)
        if browser and not GF_LABEL.search(txt):
            gerendert = per_browser(echte_url)
            if gerendert is not None:
                txt = bereinigter_text(gerendert)
        erg["Impressum-URL"] = echte_url
        impressum_text = txt
        namen, label = gf_suchen(txt)
        if namen:
            erg["Geschäftsführung"], erg["Weitere GF"], erg["GF-Label"] = namen[0], "; ".join(namen[1:]), label
        rollen_suchen(txt, erg)
        break

    # 2) Leitungsseiten (gleiche Domain wie nach der Weiterleitung)
    if not (erg["Pflegedirektion"] and erg["Personalleitung"] and erg["Geschäftsführung"]):
        for url in finde_links(start, basis, LINK_LEITUNG, 6):
            soup, _ = hole(session, url)
            if soup is None:
                continue
            txt = bereinigter_text(soup)
            if not erg["Geschäftsführung"]:
                namen, label = gf_suchen(txt)
                if namen:
                    erg["Geschäftsführung"], erg["Weitere GF"], erg["GF-Label"] = namen[0], "; ".join(namen[1:]), label + " (Leitungsseite)"
            rollen_suchen(txt, erg)
            if erg["Pflegedirektion"] and erg["Personalleitung"] and erg["Geschäftsführung"]:
                break

    gefunden = [f for f in ("Geschäftsführung", "Pflegedirektion", "Personalleitung") if erg[f]]
    if gefunden:
        erg["Status"] = "ok: " + ", ".join(gefunden)
    elif impressum_text is None:
        erg["Status"] = "Impressum nicht gefunden"
    elif not GF_LABEL.search(impressum_text):
        erg["Status"] = "Impressum ohne GF-Label (JavaScript? -> --browser)"
    else:
        erg["Status"] = "Label gefunden, aber kein gültiger Name"
    time.sleep(PAUSE)
    return erg


# ------------------------------------------------------------------ Debug

def debug(url, browser=False):
    session = requests.Session()
    soup, echte = hole(session, url)
    if soup is None:
        print("Seite nicht abrufbar.")
        return
    if browser:
        soup = per_browser(echte) or soup
    print(f"URL nach Weiterleitung: {echte}\n")
    txt = bereinigter_text(soup)
    print("=" * 70, "\nBEREINIGTER TEXT (erste 2500 Zeichen)\n" + "=" * 70)
    print(txt[:2500])
    print("=" * 70, "\nGESCHÄFTSFÜHRUNG – alle Label-Treffer\n" + "=" * 70)
    namen, label = gf_suchen(txt, debug=True)
    print(f"\n=> gewählt: {namen} (Label: {label!r})")
    print("=" * 70, "\nROLLEN\n" + "=" * 70)
    e = {}
    rollen_suchen(txt, e, debug=True)
    print(f"\n=> {e}")


# ------------------------------------------------------------------ Lauf

def fortschritt(fertig, gesamt, start):
    vergangen = time.time() - start
    rest = vergangen / fertig * (gesamt - fertig) if fertig else 0
    anteil = fertig / gesamt
    balken = "█" * int(30 * anteil) + "░" * (30 - int(30 * anteil))
    sys.stdout.write(f"\r[{balken}] {fertig}/{gesamt} ({anteil:5.1%}) | {int(vergangen // 60)}:{int(vergangen % 60):02d} "
                     f"| noch ca. {int(rest // 60)}:{int(rest % 60):02d} min ")
    sys.stdout.flush()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("xlsx", nargs="?", help="Pfad zur Leadliste")
    p.add_argument("--blatt")
    p.add_argument("--prio")
    p.add_argument("--limit", type=int)
    p.add_argument("--workers", type=int, default=6)
    p.add_argument("--neu", action="store_true", help="vorhandene Ergebnisse verwerfen")
    p.add_argument("--browser", action="store_true", help="JavaScript-Seiten per Playwright rendern")
    p.add_argument("--debug", metavar="URL", help="eine einzelne Seite analysieren")
    a = p.parse_args()

    if a.debug:
        debug(a.debug, a.browser)
        return
    if not a.xlsx:
        p.error("Pfad zur Leadliste fehlt")

    blaetter = [a.blatt] if a.blatt else [s for s in pd.ExcelFile(a.xlsx).sheet_names if s[0].isdigit()]
    teile = []
    for b in blaetter:
        d = pd.read_excel(a.xlsx, sheet_name=b, dtype=str).fillna("")
        d["Krankenhaus"] = d["Krankenhaus"] if "Krankenhaus" in d else d.get("Reha-Einrichtung", "")
        teile.append(d)
    df = pd.concat(teile, ignore_index=True)
    if a.prio:
        df = df[df["Priorität"] == a.prio]
    df = df[df["Website"] != ""].drop_duplicates(subset=["Website"])
    if a.limit:
        df = df.head(a.limit)

    if a.neu and os.path.exists(OUTPUT):
        os.rename(OUTPUT, OUTPUT.replace(".csv", f"_alt_{int(time.time())}.csv"))
    erledigt = set()
    if os.path.exists(OUTPUT):
        with open(OUTPUT, encoding="utf-8-sig") as f:
            r = csv.DictReader(f, delimiter=";")
            if "GF-Label" not in (r.fieldnames or []):
                sys.exit("Die vorhandene ansprechpartner.csv stammt aus Version 1. Bitte mit --neu starten.")
            erledigt = {z["Website"] for z in r}
        print(f"{len(erledigt)} Häuser bereits erledigt, werden übersprungen.")
    zeilen = [r for _, r in df.iterrows() if r["Website"] not in erledigt]
    if not zeilen:
        print("Nichts zu tun.")
        return

    neu = not os.path.exists(OUTPUT)
    f = open(OUTPUT, "a", newline="", encoding="utf-8-sig")
    w = csv.DictWriter(f, fieldnames=FELDER, delimiter=";")
    if neu:
        w.writeheader()

    workers = 1 if a.browser else a.workers
    print(f"{len(zeilen)} Häuser werden abgefragt, {workers} parallel.\n")
    start = time.time()
    treffer = {"Geschäftsführung": 0, "Pflegedirektion": 0, "Personalleitung": 0}
    lock = threading.Lock()
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(bearbeite, z, a.browser) for z in zeilen]
            for i, fut in enumerate(as_completed(futures), 1):
                try:
                    erg = fut.result()
                except Exception as e:
                    erg = {k: "" for k in FELDER}
                    erg["Status"] = f"Fehler: {str(e)[:60]}"
                with lock:
                    w.writerow(erg)
                    f.flush()
                    for k in treffer:
                        treffer[k] += bool(erg.get(k))
                    fortschritt(i, len(zeilen), start)
    finally:
        f.close()

    n = len(zeilen)
    print(f"\n\nFertig in {int((time.time() - start) // 60)} min. Ergebnis in {OUTPUT}")
    for k, v in treffer.items():
        print(f"  {k}: {v} von {n} ({v / n:.0%})")


if __name__ == "__main__":
    main()
