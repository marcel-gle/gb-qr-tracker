#!/usr/bin/env python3
"""
Pflegeheime großer Ketten aus deren Website-Sitemaps ziehen – mit Adresse, Platzzahl und Bundesland.

Die Standortsuchen der Ketten laden ihre Ergebnisse meist per JavaScript. Die Sitemap
(sitemap.xml) enthält dagegen alle Unterseiten, also auch jede Haus-Seite. Pro Kette
braucht das Skript ein URL-Muster, an dem es Haus-Seiten erkennt.

Schritt 1  Muster finden:     python ketten_standorte.py erkunden alloheim
           -> zeigt die URL-Gruppen der Sitemap mit Beispielen und schlägt ein Muster vor.
              Das Muster in KETTEN unten eintragen (Feld "muster").
Schritt 2  Haus-URLs sammeln: python ketten_standorte.py sammeln
Schritt 3  Häuser auswerten:  python ketten_standorte.py auswerten
Alles nacheinander:           python ketten_standorte.py alles

Optionen:  --ketten korian,alloheim         nur bestimmte Ketten
           --bundeslaender alle             alle Bundesländer statt der vier Zielregionen
           --limit 20                       nur die ersten N Häuser je Kette (Test)

Ergebnis:  ketten_standorte.csv   (Spalte „ab 80 Plätze“: ja / nein / unbekannt)
Benötigt:  pip install requests beautifulsoup4 pgeocode   (pgeocode optional, genauere Bundesländer)
"""
import argparse
import csv
import gzip
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
from plz_bundesland import bundesland  # noqa: E402

# ------------------------------------------------------------------ Konfiguration
# muster: regulärer Ausdruck für Haus-Seiten (aus „erkunden“). None = noch erkunden.
KETTEN = {
    "korian":    {"url": "https://www.korian.de", "muster": r"/pflege/stationaere-pflege/[^/]+-stationaere-pflege/?$"},
    "alloheim":  {"url": "https://www.alloheim.de", "muster": None},
    "domicil":   {"url": "https://www.domicil-seniorenresidenzen.de", "muster": None},
    "proseniore": {"url": "https://www.pro-seniore.de", "muster": None},
    "kursana":   {"url": "https://www.kursana.de", "muster": None},
    "vitanas":   {"url": "https://www.vitanas.de", "muster": None,
                  "hinweis": "Vitanas Ambulant steht auf der Negativliste (Status Offen) – Heime nur mit Hinweis"},
    "alpenland": {"url": "https://www.alpenland-berlin.de", "muster": None},
    "dana":      {"url": "https://www.dana-gmbh.de", "muster": None},
    "renafan":   {"url": "https://www.renafan.de", "muster": None},
    "deutsche-seniorenstift": {"url": "https://deutsche-seniorenstift.de", "muster": None},
    # Katharinenhof bewusst nicht enthalten: Zentrale bereits im Cold-Call (Negativliste)
}
ZIELREGIONEN = ["Berlin", "Brandenburg", "Schleswig-Holstein", "Saarland"]
LINKDATEI = os.path.join(BASE_DIR, "ketten_links.csv")
OUTPUT = os.path.join(BASE_DIR, "ketten_standorte.csv")
PAUSE = 1.0
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
           "Accept-Language": "de-DE,de;q=0.9"}


# ------------------------------------------------------------------ Sitemap

def hole(session, url, timeout=30):
    r = session.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    inhalt = r.content
    if inhalt[:2] == b"\x1f\x8b":
        inhalt = gzip.decompress(inhalt)
    return inhalt.decode("utf-8", errors="replace"), r.url


def sitemap_urls(session, basis, maximal_sitemaps=60):
    """Alle Seiten-URLs aus robots.txt-Sitemaps bzw. üblichen Sitemap-Pfaden."""
    kandidaten = []
    try:
        robots, _ = hole(session, urljoin(basis, "/robots.txt"))
        kandidaten += re.findall(r"(?im)^\s*sitemap:\s*(\S+)", robots)
    except Exception:
        pass
    kandidaten += [urljoin(basis, p) for p in ("/sitemap.xml", "/sitemap_index.xml", "/wp-sitemap.xml",
                                                "/sitemap.xml.gz", "/de/sitemap.xml")]
    seiten, offen, besucht = set(), list(dict.fromkeys(kandidaten)), set()
    while offen and len(besucht) < maximal_sitemaps:
        sm = offen.pop(0)
        if sm in besucht:
            continue
        besucht.add(sm)
        try:
            xml, _ = hole(session, sm)
        except Exception:
            continue
        locs = [l.strip() for l in re.findall(r"<loc>\s*(.*?)\s*</loc>", xml, re.S)]
        if "<sitemapindex" in xml:
            offen += [l for l in locs if l not in besucht]
        else:
            seiten.update(locs)
        time.sleep(PAUSE / 2)
    return sorted(seiten)


def erkunden(name):
    k = KETTEN.get(name, {"url": name if name.startswith("http") else "https://" + name})
    s = requests.Session()
    urls = sitemap_urls(s, k["url"])
    if not urls:
        print("Keine Sitemap gefunden. Dann im Browser eine Haus-Seite öffnen und das Muster aus deren URL ableiten.")
        return
    gruppen = defaultdict(list)
    for u in urls:
        teile = [t for t in urlparse(u).path.split("/") if t]
        gruppen["/" + "/".join(teile[:2]) if len(teile) > 2 else "/" + "/".join(teile[:1])].append(u)
    print(f"{len(urls)} URLs in der Sitemap von {k['url']}\n")
    print("Größte URL-Gruppen (Haus-Seiten sind meist eine große Gruppe mit ähnlichen Namen):")
    wort = re.compile(r"pflege|senior|residenz|haus|heim|stift|standort|einrichtung|zentrum|wohnen", re.I)
    beste = None
    for g, liste in sorted(gruppen.items(), key=lambda x: -len(x[1]))[:15]:
        anteil = sum(bool(wort.search(u)) for u in liste) / len(liste)
        print(f"\n  {g}/…   {len(liste)} URLs   (Anteil mit Pflege-Begriffen: {anteil:.0%})")
        for u in liste[:3]:
            print(f"     {u}")
        if beste is None and len(liste) >= 5 and anteil >= 0.5:
            beste = g
    if beste:
        print(f"\nVorschlag für das Muster:  r\"{re.escape(beste)}/[^/]+/?$\"")
        print("Bitte an 2–3 Beispiel-URLs im Browser prüfen, ob das wirklich einzelne Häuser sind.")


def sammeln(ketten):
    s = requests.Session()
    zeilen = []
    for name in ketten:
        k = KETTEN[name]
        if not k.get("muster"):
            print(f"  {name}: kein Muster eingetragen -> zuerst „python ketten_standorte.py erkunden {name}“")
            continue
        urls = sitemap_urls(s, k["url"])
        rx = re.compile(k["muster"])
        treffer = sorted({u for u in urls if rx.search(urlparse(u).path)})
        print(f"  {name}: {len(urls)} Sitemap-URLs, davon {len(treffer)} Haus-Seiten")
        zeilen += [{"Kette": name, "URL": u} for u in treffer]
    with open(LINKDATEI, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["Kette", "URL"], delimiter=";")
        w.writeheader()
        w.writerows(zeilen)
    print(f"{len(zeilen)} Haus-URLs gespeichert in {LINKDATEI}")


# ------------------------------------------------------------------ Haus-Seite auswerten

RAUSCH = ["script", "style", "noscript", "nav", "header", "footer", "aside", "form", "svg", "iframe"]
RAUSCH_KLASSEN = re.compile(r"nav|menu|breadcrumb|header|footer|sidebar|cookie|consent|teaser|related|newsletter", re.I)
STRASSE = re.compile(r"([A-ZÄÖÜ][\wäöüß.\- ]{2,40}?(?:straße|str\.|weg|allee|platz|ring|damm|chaussee|gasse|ufer|"
                     r"park|hof|berg|steig|pfad|markt|feld|garten|wall|graben|kamp|stieg|twiete|redder)\s*\d+\s*[a-zA-Z]?"
                     r"(?:\s*[-–]\s*\d+\s*[a-zA-Z]?)?)", re.I)
PLZ_ORT = re.compile(r"\b(\d{5})\s+([A-ZÄÖÜ][\wäöüß.\-]+(?:[ \t]+(?:an der|am|im|in|bei|ob der|vor der)?[ \t]*[A-ZÄÖÜ(][\wäöüß.\-)]+)?)")
TELEFON = re.compile(r"(?:Tel(?:efon)?\.?|Fon|T)\s*[:.]?\s*(\+?[\d][\d /()\-–]{6,}\d)")
MAIL = re.compile(r"[\w.+\-]+@[\w\-]+\.[\w.\-]+")
# Platzzahl: direkte Angaben vor Zimmer-Angaben
PLAETZE = [
    re.compile(r"(\d{2,3})\s*(?:vollstationäre[n]?\s+|stationäre[n]?\s+|Langzeit)?(?:Pflegeplätze[n]?|Pflegeplatz|Plätze[n]?|Betten|"
               r"Bewohnerplätze[n]?|Pflegeappartements|Appartements)", re.I),
    re.compile(r"(?:Pflegeplätze|Plätze|Betten|Kapazität)\s*[:\-]?\s*(\d{2,3})", re.I),
    re.compile(r"(?:für|bis zu)\s+(\d{2,3})\s+(?:Bewohner(?:innen)?|Senior(?:inn)?en|Menschen|pflegebedürftige)", re.I),
]
EINZEL = re.compile(r"(\d{1,3})\s*Einzelzimmer", re.I)
DOPPEL = re.compile(r"(\d{1,3})\s*Doppelzimmer", re.I)


def hauptinhalt(soup):
    for t in soup(RAUSCH):
        t.decompose()
    for t in list(soup.find_all(True)):
        if getattr(t, "decomposed", False) or not getattr(t, "attrs", None):
            continue
        kennung = " ".join(t.attrs.get("class", []) if isinstance(t.attrs.get("class"), list) else []) + " " + str(t.attrs.get("id", ""))
        if kennung.strip() and RAUSCH_KLASSEN.search(kennung) and t.name not in ("body", "html", "main"):
            t.decompose()
    knoten = soup.select_one("main") or soup.select_one("article") or soup.body or soup
    text = knoten.get_text("\n").replace("\xa0", " ").replace("­", "")
    return re.sub(r"\n\s*\n+", "\n", re.sub(r"[ \t]+", " ", text))


def json_ld(soup):
    """Adresse, Telefon und Name aus schema.org-Daten (viele Ketten pflegen diese für Google)."""
    erg = {}
    def walk(o):
        if isinstance(o, dict):
            adr = o.get("address")
            if isinstance(adr, dict) and adr.get("postalCode") and not erg.get("PLZ"):
                erg.update({"Straße": adr.get("streetAddress", ""), "PLZ": str(adr.get("postalCode", "")),
                            "Ort": adr.get("addressLocality", ""), "Name": o.get("name", "")})
                if o.get("telephone"):
                    erg["Telefon"] = o["telephone"]
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            walk(json.loads(s.string or "{}"))
        except Exception:
            continue
    return erg


def plaetze_finden(text):
    werte = []
    for rx in PLAETZE:
        werte += [int(x) for x in rx.findall(text) if 20 <= int(x) <= 400]
    if werte:
        return max(werte), "Angabe auf der Seite"
    e = sum(int(x) for x in EINZEL.findall(text))
    d = sum(int(x) for x in DOPPEL.findall(text))
    if e or d:
        return e + 2 * d, "aus Zimmerzahl berechnet"
    return "", ""


def haus_auswerten(session, kette, url):
    erg = {"Kette": kette, "URL": url, "Domain": urlparse(url).netloc.replace("www.", "")}
    try:
        r = session.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        erg["Status"] = f"nicht erreichbar: {str(e)[:50]}"
        return erg
    soup = BeautifulSoup(r.content, "html.parser")
    titel = (soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else
             (soup.title.get_text(strip=True).split("|")[0].split(" - ")[0] if soup.title else ""))
    ld = json_ld(soup)
    text = hauptinhalt(soup)

    erg["Name"] = ld.get("Name") or titel
    if ld.get("PLZ"):
        erg.update({"Straße": ld.get("Straße", ""), "PLZ": ld["PLZ"], "Ort": ld.get("Ort", ""), "Adresse-Quelle": "strukturierte Daten"})
    else:
        m = PLZ_ORT.search(text)
        if m:
            vorher = text[max(0, m.start() - 120):m.start()]
            st = STRASSE.findall(vorher)
            erg.update({"PLZ": m.group(1), "Ort": m.group(2).strip(), "Straße": st[-1].strip() if st else "",
                        "Adresse-Quelle": "Seitentext"})
    m = TELEFON.search(text)
    erg["Telefon"] = ld.get("Telefon") or (m.group(1).strip() if m else "")
    m = MAIL.search(text)
    erg["E-Mail"] = m.group(0) if m else ""
    erg["Plätze"], erg["Plätze-Quelle"] = plaetze_finden(text)
    erg["Bundesland"] = bundesland(erg.get("PLZ", "")) if erg.get("PLZ") else ""
    p = erg["Plätze"]
    if isinstance(p, int) and p >= 80:
        erg["ab 80 Plätze"] = "ja"
    elif isinstance(p, int) and erg["Plätze-Quelle"] == "aus Zimmerzahl berechnet":
        erg["ab 80 Plätze"] = "unbekannt"          # Zimmerangaben sind oft unvollständig -> nicht als zu klein werten
    elif isinstance(p, int):
        erg["ab 80 Plätze"] = "nein"
    else:
        erg["ab 80 Plätze"] = "unbekannt"
    erg["Hinweis"] = KETTEN.get(kette, {}).get("hinweis", "")
    erg["Status"] = "ok" if erg.get("PLZ") else "keine Adresse gefunden"
    return erg


FELDER = ["Kette", "Name", "Straße", "PLZ", "Ort", "Bundesland", "Telefon", "E-Mail", "URL", "Domain", "Plätze",
          "Plätze-Quelle", "ab 80 Plätze", "Adresse-Quelle", "Hinweis", "Status"]


def auswerten(ketten, regionen, limit=None, linkdatei=None):
    linkdatei = linkdatei or LINKDATEI
    if not os.path.exists(linkdatei):
        sys.exit(f"{linkdatei} fehlt. Bitte zuerst: python ketten_standorte.py sammeln")
    with open(linkdatei, encoding="utf-8-sig") as f:
        links = [z for z in csv.DictReader(f, delimiter=";") if ketten is None or z["Kette"] in ketten]
    if limit:
        je = Counter()
        links = [z for z in links if (je.update([z["Kette"]]) or je[z["Kette"]] <= limit)]
    erledigt = set()
    if os.path.exists(OUTPUT):
        with open(OUTPUT, encoding="utf-8-sig") as f:
            erledigt = {z["URL"] for z in csv.DictReader(f, delimiter=";")}
        print(f"{len(erledigt)} Häuser bereits erledigt, werden übersprungen.")
    offen = [z for z in links if z["URL"] not in erledigt]
    neu = not os.path.exists(OUTPUT)
    s, start, zaehler = requests.Session(), time.time(), Counter()
    with open(OUTPUT, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FELDER, delimiter=";", extrasaction="ignore")
        if neu:
            w.writeheader()
        for i, z in enumerate(offen, 1):
            erg = haus_auswerten(s, z["Kette"], z["URL"])
            if regionen and erg.get("Bundesland") and erg["Bundesland"] not in regionen:
                erg["Status"] = "außerhalb der Zielregion"
            w.writerow(erg)
            f.flush()
            zaehler[erg["Status"] if erg["Status"] != "ok" else "ok, ab 80: " + erg["ab 80 Plätze"]] += 1
            rest = (time.time() - start) / i * (len(offen) - i)
            balken = "█" * int(30 * i / len(offen)) + "░" * (30 - int(30 * i / len(offen)))
            sys.stdout.write(f"\r[{balken}] {i}/{len(offen)} | noch ca. {int(rest // 60)}:{int(rest % 60):02d} min ")
            sys.stdout.flush()
            time.sleep(PAUSE)
    print(f"\n\nErgebnis in {OUTPUT}:")
    for k, v in zaehler.most_common():
        print(f"  {k}: {v}")


def main():
    global OUTPUT
    p = argparse.ArgumentParser()
    p.add_argument("schritt", choices=["erkunden", "sammeln", "auswerten", "alles"])
    p.add_argument("kette", nargs="?", help="nur für „erkunden“: Kettenname aus KETTEN oder eine Domain")
    p.add_argument("--ketten", help="Komma-Liste, z. B. korian,alloheim (Standard: alle mit Muster)")
    p.add_argument("--bundeslaender", default=",".join(ZIELREGIONEN), help="Komma-Liste oder „alle“")
    p.add_argument("--limit", type=int)
    p.add_argument("--links", help="eigene Linkdatei (Spalten Kette;URL), z. B. osm_groesse_pruefen.csv")
    p.add_argument("--out", help="Ausgabedatei (Standard: ketten_standorte.csv)")
    a = p.parse_args()
    if a.out:
        OUTPUT = a.out
    if a.schritt == "erkunden":
        if not a.kette:
            p.error("Bitte eine Kette angeben, z. B.: erkunden alloheim")
        erkunden(a.kette)
        return
    if a.links:                                   # eigene Linkdatei: alle Zeilen auswerten, keine Sitemap nötig
        regionen = None if a.bundeslaender.lower() == "alle" else [b.strip() for b in a.bundeslaender.split(",")]
        auswerten(None, regionen, a.limit, a.links)
        return
    ketten = a.ketten.split(",") if a.ketten else list(KETTEN)
    unbekannt = [k for k in ketten if k not in KETTEN]
    if unbekannt:
        p.error(f"Unbekannte Kette(n): {', '.join(unbekannt)}")
    regionen = None if a.bundeslaender.lower() == "alle" else [b.strip() for b in a.bundeslaender.split(",")]
    if a.schritt in ("sammeln", "alles"):
        sammeln(ketten)
    if a.schritt in ("auswerten", "alles"):
        auswerten(ketten, regionen, a.limit)


if __name__ == "__main__":
    main()
