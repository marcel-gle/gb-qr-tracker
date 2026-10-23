#!/usr/bin/env python3
"""Vollstationäre Pflegeeinrichtungen Hamburg (offizieller Datensatz der Sozialbehörde, mit Platzzahl).

Quelle: Transparenzportal Hamburg, WFS "HH_WFS_Vollstationaere_Pflegeeinrichtungen"
        Lizenz: Datenlizenz Deutschland – Namensnennung 2.0

Aufruf:
    python hamburg_pflegeheime.py                 # lädt live vom Geodienst
    python hamburg_pflegeheime.py --datei x.json  # falls der Download im Browser gemacht wurde
    python hamburg_pflegeheime.py --min 0         # alle Heime, egal welche Größe

Ergebnis: hamburg_pflegeheime.csv (Semikolon, UTF-8) – nur Standardbibliothek nötig.
"""
import argparse, csv, json, re, sys, urllib.request
from urllib.parse import urlparse

WFS = ("https://geodienste.hamburg.de/HH_WFS_Vollstationaere_Pflegeeinrichtungen?service=WFS&version=2.0.0"
       "&request=GetFeature&typeNames=de.hh.up:vollstationaere_pflegeeinrichtungen&outputFormat=application/geo%2Bjson")

FREEMAIL = {'gmx.de', 'web.de', 'gmail.com', 't-online.de', 'googlemail.com', 'hotmail.com', 'yahoo.de', 'outlook.de', 'freenet.de'}


def domain(url):
    url = (url or '').strip().split()[0] if (url or '').strip() else ''
    if not url:
        return ''
    if '://' not in url:
        url = 'http://' + url
    d = urlparse(url).netloc.lower().split(':')[0]
    return d[4:] if d.startswith('www.') else d


def mail_domain(m):
    m = (m or '').strip().lower()
    d = m.split('@')[-1] if '@' in m else ''
    return '' if d in FREEMAIL else d


def laden(datei):
    if datei:
        with open(datei, encoding='utf-8') as f:
            return json.load(f)
    print('Lade Datensatz vom Geodienst Hamburg …')
    req = urllib.request.Request(WFS, headers={'User-Agent': 'Mozilla/5.0 (Datenabruf Pflegeheime)'})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode('utf-8'))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--datei', help='lokal gespeicherte GeoJSON-Datei statt Live-Abruf')
    ap.add_argument('--min', type=int, default=80, help='Mindestzahl Plätze (Standard 80)')
    ap.add_argument('--out', default='hamburg_pflegeheime.csv')
    a = ap.parse_args()

    try:
        gj = laden(a.datei)
    except Exception as e:
        sys.exit(f'Abruf fehlgeschlagen: {e}\nURL im Browser öffnen, als .json speichern und mit --datei übergeben:\n{WFS}')

    zeilen, alle = [], 0
    for ft in gj.get('features', []):
        p = ft.get('properties', {}) or {}
        alle += 1
        try:
            plaetze = int(p.get('plaetze') or 0)
        except ValueError:
            plaetze = 0
        ort_feld = (p.get('ort') or '').strip()
        m = re.match(r'(\d{5})\s*(.*)', ort_feld)
        plz, ort = (m.group(1), m.group(2) or 'Hamburg') if m else ('', ort_feld or 'Hamburg')
        web = (p.get('homepage') or '').strip()
        web_t = (p.get('homepage_traeger') or '').strip()
        dom, dq = domain(web), 'Website Einrichtung'
        if not dom:
            dom, dq = mail_domain(p.get('email')), 'E-Mail Einrichtung'
        if not dom:
            dom, dq = domain(web_t), 'Website Träger'
        if not dom:
            dom, dq = mail_domain(p.get('email_traeger')), 'E-Mail Träger'
        if not dom:
            dq = ''
        geo = (ft.get('geometry') or {}).get('coordinates') or ['', '']
        zeilen.append({
            'Einrichtung': (p.get('pflegeeinrichtung') or '').strip(),
            'Straße': (p.get('adresse') or '').strip(), 'PLZ': plz, 'Ort': ort, 'Bundesland': 'Hamburg',
            'Telefon': (p.get('telefonnummer') or '').strip(), 'E-Mail': (p.get('email') or '').strip(),
            'Website': web or web_t, 'Domain': dom, 'Domain-Quelle': dq,
            'Plätze': plaetze, f'ab {a.min} Plätze': 'ja' if plaetze >= a.min else 'nein',
            'Versorgungsangebote': (p.get('versorgungsangebote') or '').strip(),
            'Trägername': (p.get('traeger') or '').strip(),
            'Träger Adresse': ' '.join(x for x in [(p.get('adresse_traeger') or '').strip(), (p.get('ort_traeger') or '').strip()] if x),
            'Träger Telefon': (p.get('telefonnummer_traeger') or '').strip(),
            'Träger E-Mail': (p.get('email_traeger') or '').strip(), 'Träger Website': web_t,
            'Längengrad/Rechtswert': geo[0] if len(geo) > 0 else '', 'Breitengrad/Hochwert': geo[1] if len(geo) > 1 else '',
            'Datenquelle': 'Transparenzportal Hamburg – Vollstationäre Pflegeeinrichtungen (Sozialbehörde)',
        })

    treffer = [z for z in zeilen if z['Plätze'] >= a.min]
    treffer.sort(key=lambda z: -z['Plätze'])
    if not zeilen:
        sys.exit('Keine Einrichtungen im Datensatz gefunden – Format geändert?')
    with open(a.out, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=list(zeilen[0].keys()), delimiter=';')
        w.writeheader(); w.writerows(treffer)
    print(f'Einrichtungen gesamt: {alle}')
    print(f'davon ab {a.min} Plätzen: {len(treffer)}')
    print(f'davon ohne Domain: {sum(1 for z in treffer if not z["Domain"])}')
    print(f'gespeichert: {a.out}')


if __name__ == '__main__':
    main()
