#!/usr/bin/env python3
"""Pflegeheime NRW aus den Ergebnisberichten der WTG-Behörden (Heimaufsicht).

In NRW veröffentlicht jede WTG-Behörde (Kreis / kreisfreie Stadt) ihre Prüfberichte selbst,
aber im landesweit gleichen Formular („Ergebnisbericht“ nach WTG). Darin stehen u. a.
Name, Anschrift, Kapazität (= Platzzahl), Leistungsangebot, Telefon, Homepage und Prüfdatum.

Ablauf:
  1. links      – Übersichtsseiten der Behörden durchsuchen, PDF-Links sammeln   -> nrw_pdf_links.csv
  2. auswerten  – PDFs laden (Cache), Kopfdaten auslesen                          -> nrw_heimaufsicht.csv
  alles         – beides nacheinander

Beispiele:
  python nrw_heimaufsicht.py alles
  python nrw_heimaufsicht.py alles --behoerde koeln          # nur Köln
  python nrw_heimaufsicht.py auswerten --limit 10            # Testlauf
  python nrw_heimaufsicht.py --debug <PDF-URL>               # ausgelesenen Text eines Berichts zeigen
  python nrw_heimaufsicht.py links --start "Essen=https://…" # weitere Behörde ohne Code-Änderung

Benötigt: pip install requests beautifulsoup4 pdfplumber
"""
import argparse, csv, hashlib, io, os, re, sys, time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    import pdfplumber
except ImportError:
    sys.exit('Bitte zuerst installieren: pip install pdfplumber')

# --------------------------------------------------------------------------- Behörden
# Start = Seite(n), auf denen die Ergebnisberichte der STATIONÄREN Altenpflege verlinkt sind.
# Neue Behörde: Übersichtsseite im Browser suchen („<Stadt/Kreis> WTG Ergebnisberichte“) und hier eintragen.
BEHOERDEN = {
    'koeln': ('Stadt Köln', [
        'https://www.stadt-koeln.de/leben-in-koeln/soziales/seniorinnen/stationaere-einrichtungen-fuer-pflegebeduerftige-menschen']),
    'muenster': ('Stadt Münster', [
        'https://www.stadt-muenster.de/sozialamt/heimaufsicht/altenpflegeeinrichtungen']),
    'dortmund': ('Stadt Dortmund', [
        'https://www.dortmund.de/themen/senior-innen/pruefungsberichte-der-heimaufsicht/']),
    'essen': ('Stadt Essen', [
        'https://www.essen.de/leben/soziales_und_arbeit/heimsachgebiet/ergebnisberichte_regelpruefungen_2018_.de.html']),
    'duisburg': ('Stadt Duisburg', [
        'https://www.duisburg.de/vv/produkte/pro_du/dez_iii/50/Ergebnisbericht2020']),
}

# PDFs/Seiten, die sicher keine stationäre Altenpflege sind
AUSSCHLUSS_URL = re.compile(r'behinder|werkstat|eingliederung|tagespflege|hospiz|wohngemeinschaft|servicewohnen|'
                            r't%C3%A4tigkeit|tätigkeit|taetigkeit|datenschutz|flyer|formular|merkblatt|wfbm|kinder|jugend', re.I)
# Unterseiten, denen der Crawler auf derselben Domain folgt
FOLGEN = re.compile(r'station|altenpflege|alten-?heim|pflegeeinricht|pflegeheim|ergebnisbericht|pruef|prüf|heimaufsicht|wtg|senior', re.I)

S = requests.Session()
S.headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36'
CACHE = 'pdf_cache_nrw'
FREEMAIL = {'gmx.de', 'gmx.net', 'web.de', 't-online.de', 'gmail.com', 'googlemail.com', 'freenet.de', 'online.de', 'yahoo.de',
            'outlook.de', 'hotmail.de', 'arcor.de'}


def holen(url, tries=3):
    for t in range(tries):
        try:
            r = S.get(url, timeout=40)
            if r.status_code == 200:
                return r
            if r.status_code in (403, 404):
                return None
        except requests.RequestException:
            pass
        time.sleep(2 * (t + 1))
    return None


def fortschritt(i, n, txt=''):
    b = int(30 * i / max(n, 1))
    sys.stdout.write(f'\r[{"#" * b}{"." * (30 - b)}] {i}/{n} {txt[:50]:<50}')
    sys.stdout.flush()


# --------------------------------------------------------------------------- 1. Links
def ist_pdf(href):
    h = href.lower()
    return h.split('?')[0].endswith('.pdf') or 'getfile' in h or '/download/' in h


def links_sammeln(behoerden, max_seiten=80, tiefe=2):
    rows = []
    for key, (name, starts) in behoerden.items():
        host = {urlparse(u).netloc for u in starts}
        gesehen, pdfs = set(), {}
        queue = [(u, 0) for u in starts]
        print(f'\n{name}:')
        while queue and len(gesehen) < max_seiten:
            url, d = queue.pop(0)
            if url in gesehen:
                continue
            gesehen.add(url)
            r = holen(url)
            if not r or 'html' not in r.headers.get('Content-Type', 'html'):
                print(f'  nicht erreichbar: {url}') if d == 0 else None
                continue
            soup = BeautifulSoup(r.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                href = urljoin(url, a['href'].strip()); text = a.get_text(' ', strip=True)
                if AUSSCHLUSS_URL.search(href) or AUSSCHLUSS_URL.search(text):
                    continue
                if ist_pdf(href):
                    pdfs.setdefault(href.split('#')[0], text)
                elif d < tiefe and urlparse(href).netloc in host and (FOLGEN.search(href) or FOLGEN.search(text)) and href not in gesehen:
                    queue.append((href.split('#')[0], d + 1))
            time.sleep(0.5)
        print(f'  {len(gesehen)} Seiten durchsucht, {len(pdfs)} PDF-Links')
        rows += [{'Behörde': name, 'Linktext': t, 'PDF': u} for u, t in pdfs.items()]
    return rows


# --------------------------------------------------------------------------- 2. PDFs auswerten
def pdf_text(url):
    os.makedirs(CACHE, exist_ok=True)
    fn = os.path.join(CACHE, hashlib.md5(url.encode()).hexdigest() + '.pdf')
    if not os.path.exists(fn):
        r = holen(url)
        if not r or not r.content.startswith(b'%PDF'):
            return None
        with open(fn, 'wb') as f:
            f.write(r.content)
        time.sleep(0.4)
    try:
        with pdfplumber.open(fn) as pdf:
            return '\n'.join((p.extract_text() or '') for p in pdf.pages[:3])
    except Exception:
        return None


def feld(txt, *labels, mehrzeilig=False):
    for lab in labels:
        m = re.search(rf'(?im)^\s*{lab}\s*[:\-]?\s*(.*)$', txt)
        if m:
            v = m.group(1).strip()
            if not v and mehrzeilig:                      # Wert in der nächsten Zeile
                rest = txt[m.end():].lstrip('\n').split('\n')
                v = rest[0].strip() if rest else ''
            if v:
                return v
    return ''


def auslesen(txt):
    # Abschnitt „Allgemeine Angaben“ bis zum ersten Bewertungsabschnitt
    a = re.search(r'Allgemeine Angaben', txt)
    e = re.search(r'Wohnqualit|Hauswirtschaftliche Versorgung', txt[a.end():] if a else txt)
    kopf = txt[a.start(): a.end() + e.start()] if a and e else txt[:4000]
    d = {}
    d['Einrichtungsart'] = feld(kopf, r'(?:Art der )?Einrichtung(?!\s*mit\b)', r'Angebotstyp', r'Angebotsform')
    d['Einrichtung'] = feld(kopf, r'Name(?: der Einrichtung)?', mehrzeilig=True)
    d['Anschrift'] = feld(kopf, r'Anschrift', r'Adresse', mehrzeilig=True)
    kap = re.search(r'Kapazit[äa]t[^\d\n]{0,40}(\d{1,4})', kopf) or re.search(r'(\d{1,4})\s*(?:Pflege)?pl[äa]tze', kopf, re.I)
    d['Plätze'] = int(kap.group(1)) if kap else ''
    d['Leistungsangebot'] = feld(kopf, r'Leistungsangebot(?:\s*\([^)]*\))?', mehrzeilig=True)
    d['Telefon'] = (re.search(r'Telefon(?:nummer)?\s*[:\-]?\s*([\d][\d /\-()+]{5,})', kopf) or [None, ''])[1].strip()
    mails = re.findall(r'[\w.+-]+@[\w-]+(?:\.[\w-]+)+', kopf)
    webs = [w for w in re.findall(r'(?:https?://|www\.)[^\s,;]+', kopf, re.I) if '@' not in w]
    d['E-Mail'] = mails[0] if mails else ''
    d['Website'] = webs[0].rstrip('.)') if webs else ''
    dt = re.search(r'erfolgte am[^\d]{0,30}(\d{1,2}\.\s?\d{1,2}\.\s?\d{2,4})', txt) or re.search(r'Prüf\w* am\D{0,20}(\d{1,2}\.\d{1,2}\.\d{4})', txt)
    d['Prüfdatum'] = dt.group(1).replace(' ', '') if dt else ''
    m = re.search(r'^(.*?),?\s*(\d{5})\s+(.+)$', d['Anschrift'])
    d['Straße'], d['PLZ'], d['Ort'] = (m.group(1).strip(' ,'), m.group(2), m.group(3).strip()) if m else (d['Anschrift'], '', '')
    if not d['PLZ']:
        m = re.search(r'\b(\d{5})\s+([A-ZÄÖÜ][\wäöüß\-. ]+)', kopf)
        if m:
            d['PLZ'], d['Ort'] = m.group(1), m.group(2).strip()
    return d


def domain(d):
    for v, q in ((d['Website'], 'Website Einrichtung'), (d['E-Mail'], 'E-Mail Einrichtung')):
        v = v.strip().lower()
        if not v:
            continue
        if '@' in v:
            h = v.split('@')[-1]
            if h in FREEMAIL:
                continue
        else:
            h = urlparse(v if '://' in v else 'http://' + v).netloc.split(':')[0]
        h = h[4:] if h.startswith('www.') else h
        if '.' in h:
            return h, q
    return '', ''


def stationaer(d, linktext, url):
    la = (d['Leistungsangebot'] + ' ' + d['Einrichtungsart']).lower()
    if re.search(r'tagespflege|hospiz|wohngemeinschaft|werkst', la + linktext.lower() + url.lower()) and 'vollstation' not in la:
        return False
    if re.search(r'eingliederung', la) and 'pflege' not in la.replace('eingliederung', ''):
        return False
    if re.fullmatch(r'\s*kurzzeitpflege\s*', d['Leistungsangebot'].lower()):
        return False
    return True


def datum_key(s):
    m = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{2,4})', s or '')
    if not m:
        return (0, 0, 0)
    y = int(m.group(3)); y = y + 2000 if y < 100 else y
    return (y, int(m.group(2)), int(m.group(1)))


def auswerten(links, minimum, limit=None):
    if limit:
        links = links[:limit]
    roh = []
    for i, l in enumerate(links, 1):
        fortschritt(i, len(links), l['Linktext'] or l['PDF'].rsplit('/', 1)[-1])
        txt = pdf_text(l['PDF'])
        if not txt:
            roh.append({'Status': 'PDF nicht lesbar', 'Behörde': l['Behörde'], 'PDF': l['PDF']}); continue
        if not re.search(r'Wohn- und Teilhabegesetz|WTG|Ergebnisbericht|Kapazit', txt):
            roh.append({'Status': 'kein Ergebnisbericht', 'Behörde': l['Behörde'], 'PDF': l['PDF']}); continue
        d = auslesen(txt)
        d.update({'Behörde': l['Behörde'], 'PDF': l['PDF'], 'Linktext': l['Linktext']})
        if not d['Einrichtung']:
            d['Einrichtung'] = re.sub(r'(?i)^ergebnisbericht[ _]*\d{4}[ _]*', '', l['Linktext'] or '').strip()
        d['Status'] = 'ok' if d['Plätze'] != '' else 'Kapazität nicht gefunden'
        if d['Status'] == 'ok' and not stationaer(d, l['Linktext'], l['PDF']):
            d['Status'] = 'nicht stationäre Altenpflege'
        roh.append(d)
    print()
    # je Haus der neueste Bericht
    best = {}
    for d in roh:
        if d.get('Status') != 'ok':
            continue
        k = re.sub(r'[^a-z0-9]', '', (d['Straße'] + d['PLZ']).lower().replace('straße', 'str').replace('str.', 'str')) or d['Einrichtung'].lower()
        if k not in best or datum_key(d['Prüfdatum']) > datum_key(best[k]['Prüfdatum']):
            best[k] = d
    out = []
    for d in best.values():
        dom, q = domain(d)
        out.append({'Einrichtung': d['Einrichtung'], 'Straße': d['Straße'], 'PLZ': d['PLZ'], 'Ort': d['Ort'],
                    'Bundesland': 'Nordrhein-Westfalen', 'Telefon': d['Telefon'], 'E-Mail': d['E-Mail'], 'Website': d['Website'],
                    'Domain': dom, 'Domain-Quelle': q, 'Plätze': d['Plätze'], f'ab {minimum} Plätze': 'ja' if d['Plätze'] >= minimum else 'nein',
                    'Leistungsangebot': d['Leistungsangebot'], 'Einrichtungsart': d['Einrichtungsart'], 'Prüfdatum': d['Prüfdatum'],
                    'Behörde': d['Behörde'], 'PDF': d['PDF']})
    out.sort(key=lambda r: -r['Plätze'])
    fehler = [d for d in roh if d.get('Status') != 'ok']
    return out, fehler


def schreiben(fn, rows):
    if not rows:
        return
    keys = list(dict.fromkeys(k for r in rows for k in r))
    with open(fn, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=keys, delimiter=';', extrasaction='ignore'); w.writeheader(); w.writerows(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('befehl', nargs='?', choices=['links', 'auswerten', 'alles'], default='alles')
    ap.add_argument('--behoerde', nargs='*', help=f'Auswahl: {", ".join(BEHOERDEN)} (Standard: alle)')
    ap.add_argument('--start', nargs='*', default=[], help='zusätzliche Behörde als "Name=URL"')
    ap.add_argument('--min', type=int, default=80)
    ap.add_argument('--limit', type=int)
    ap.add_argument('--alle-groessen', action='store_true', help='auch Heime unter --min in die Ergebnisdatei')
    ap.add_argument('--debug', metavar='PDF_URL')
    a = ap.parse_args()

    if a.debug:
        txt = pdf_text(a.debug)
        print(txt[:3500] if txt else 'PDF nicht lesbar'); print('\n--- ausgelesen ---')
        if txt:
            for k, v in auslesen(txt).items(): print(f'{k:16} {v}')
        return

    beh = {k: v for k, v in BEHOERDEN.items() if not a.behoerde or k in a.behoerde}
    for s in a.start:
        n, u = s.split('=', 1); beh[n.lower()] = (n, [u])

    if a.befehl in ('links', 'alles'):
        links = links_sammeln(beh)
        schreiben('nrw_pdf_links.csv', links)
        print(f'\n{len(links)} PDF-Links gespeichert: nrw_pdf_links.csv')
    if a.befehl in ('auswerten', 'alles'):
        with open('nrw_pdf_links.csv', encoding='utf-8-sig') as f:
            links = list(csv.DictReader(f, delimiter=';'))
        if a.behoerde or a.start:
            namen = {v[0] for v in beh.values()}; links = [l for l in links if l['Behörde'] in namen]
        rows, fehler = auswerten(links, a.min, a.limit)
        treffer = rows if a.alle_groessen else [r for r in rows if r['Plätze'] >= a.min]
        schreiben('nrw_heimaufsicht.csv', treffer)
        schreiben('nrw_nicht_ausgewertet.csv', fehler)
        print(f'Stationäre Häuser erkannt: {len(rows)} | davon ab {a.min} Plätzen: {sum(r["Plätze"] >= a.min for r in rows)}'
              f' | ohne Domain: {sum(1 for r in treffer if not r["Domain"])}')
        from collections import Counter
        print('Nicht ausgewertet:', dict(Counter(d['Status'] for d in fehler)))
        print('Ergebnis: nrw_heimaufsicht.csv  (Prüfliste: nrw_nicht_ausgewertet.csv)')


if __name__ == '__main__':
    main()
