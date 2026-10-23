#!/usr/bin/env python3
"""
lod2_gewerbe.py — Gewerbegebäude aus den Bayern-OpenData-LoD2-Daten
für Nürnberg + Umland herunterladen und filtern.

Funktionsweise:
  1. Lädt für jede Gemeinde / jeden Landkreis die Metalink-Datei (.meta4)
     der Bayerischen Vermessungsverwaltung.
  2. Lädt alle darin referenzierten 2x2-km-CityGML-Kacheln herunter
     (mit SHA-256-Verifikation; bereits geladene, gültige Kacheln
     werden übersprungen — Abbruch + Neustart ist also unproblematisch).
  3. Filtert alle Gebäude mit ALKIS-Gebäudefunktion "Wirtschaft/Gewerbe"
     (Codes 2xxx, optional auch Mischnutzung 1120/1130/1220/1230).
  4. Exportiert:
       - gewerbe_gebaeude.geojson   (2D-Grundrisse + Attribute, EPSG:25832)
       - gewerbe_gebaeude.csv       (Attributtabelle)
       - gewerbe_lod2.gml           (optional: gefiltertes CityGML mit 3D-Geometrie)

Benötigt nur die Python-Standardbibliothek (Python >= 3.9).

Beispiele:
  python3 lod2_gewerbe.py --all                        # Download + Filter, alle Gebiete
  python3 lod2_gewerbe.py --download --nur 09564000    # nur Stadt Nürnberg laden
  python3 lod2_gewerbe.py --filter --gml-export        # Filter inkl. CityGML-Export
  python3 lod2_gewerbe.py --filter --mit-mischnutzung  # auch Wohn-/Geschäftsgebäude
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

# --------------------------------------------------------------------------
# Gebiete: Amtliche Schlüssel (AGS) für Nürnberg und Umland
# 8-stellig = Gemeinde/kreisfreie Stadt, 5-stellig = ganzer Landkreis
# --------------------------------------------------------------------------
GEBIETE: dict[str, str] = {
    "09564000": "Nürnberg (Stadt)",
    "09563000": "Fürth (Stadt)",
    "09562000": "Erlangen (Stadt)",
    "09565000": "Schwabach (Stadt)",
    "09574":    "Landkreis Nürnberger Land",
    "09573":    "Landkreis Fürth",
    "09572":    "Landkreis Erlangen-Höchstadt",
    "09576":    "Landkreis Roth",
}

METALINK_URL = "https://geodaten.bayern.de/odd/a/lod2/citygml/meta/metalink/{ags}.meta4"

# --------------------------------------------------------------------------
# ALKIS-Gebäudefunktion (Codeliste 31001_*): relevante Gewerbe-Codes
# 2xxx = Gebäude für Wirtschaft oder Gewerbe (inkl. Handel, Industrie, Lager)
# --------------------------------------------------------------------------
FUNKTION_LABELS: dict[str, str] = {
    "1120": "Wohngebäude mit Gewerbe und Industrie",
    "1130": "Wohngebäude mit Handel und Dienstleistungen",
    "1220": "Wohn- und Geschäftsgebäude",
    "1230": "Wohn- und Betriebsgebäude",
    "2000": "Gebäude für Wirtschaft oder Gewerbe",
    "2010": "Gebäude für Handel und Dienstleistungen",
    "2020": "Bürogebäude",
    "2030": "Kreditinstitut",
    "2040": "Versicherung",
    "2050": "Geschäftsgebäude",
    "2051": "Kaufhaus",
    "2052": "Einkaufszentrum",
    "2053": "Markthalle",
    "2054": "Laden",
    "2055": "Kiosk",
    "2056": "Apotheke",
    "2060": "Messehalle",
    "2070": "Gebäude für Beherbergung",
    "2071": "Hotel, Motel, Pension",
    "2074": "Gästehaus",
    "2080": "Gebäude für Bewirtung",
    "2081": "Gaststätte, Restaurant",
    "2083": "Kantine, Mensa",
    "2090": "Freizeit- und Vergnügungsstätte",
    "2100": "Gebäude für Gewerbe und Industrie",
    "2110": "Produktionsgebäude",
    "2112": "Fabrik",
    "2113": "Betriebsgebäude",
    "2114": "Brauerei",
    "2120": "Werkstatt",
    "2121": "Sägewerk",
    "2130": "Tankstelle",
    "2131": "Waschstraße, Waschanlage",
    "2140": "Gebäude für Vorratshaltung",
    "2141": "Kühlhaus",
    "2143": "Lagerhalle, Lagerschuppen, Lagerhaus",
    "2150": "Speditionsgebäude",
    "2160": "Gebäude für Forschungszwecke",
    "2170": "Gebäude für Grundstoffgewinnung",
    "2180": "Gebäude für betriebliche Sozialeinrichtung",
    "2200": "Sonstiges Gebäude für Gewerbe und Industrie",
    "2210": "Mühle",
    "2213": "Windmühle",
    "2220": "Wetterstation",
    "2310": "Gebäude für Handel und Dienstleistung mit Wohnen",
    "2320": "Gebäude für Gewerbe und Industrie mit Wohnen",
    "2400": "Gebäude für Versorgung",  # optional; siehe --mit-versorgung
    "2500": "Gebäude für Entsorgung",
    "2700": "Gebäude für Land- und Forstwirtschaft",
}

MISCHNUTZUNG_CODES = {"1120", "1130", "1220", "1230"}

# --------------------------------------------------------------------------
# XML-Namespaces (CityGML 1.0 und 2.0, Bayern liefert je nach Stand beides)
# --------------------------------------------------------------------------
NS_CANDIDATES = {
    "core": [
        "http://www.opengis.net/citygml/2.0",
        "http://www.opengis.net/citygml/1.0",
    ],
    "bldg": [
        "http://www.opengis.net/citygml/building/2.0",
        "http://www.opengis.net/citygml/building/1.0",
    ],
    "gen": [
        "http://www.opengis.net/citygml/generics/2.0",
        "http://www.opengis.net/citygml/generics/1.0",
    ],
}
NS_GML = "http://www.opengis.net/gml"
NS_XAL = "urn:oasis:names:tc:ciq:xsdschema:xAL:2.0"
NS_METALINK = "urn:ietf:params:xml:ns:metalink"

XML_PREFIXES = {
    "core1": "http://www.opengis.net/citygml/1.0",
    "core": "http://www.opengis.net/citygml/2.0",
    "bldg1": "http://www.opengis.net/citygml/building/1.0",
    "bldg": "http://www.opengis.net/citygml/building/2.0",
    "gen1": "http://www.opengis.net/citygml/generics/1.0",
    "gen": "http://www.opengis.net/citygml/generics/2.0",
    "app": "http://www.opengis.net/citygml/appearance/2.0",
    "gml": NS_GML,
    "xAL": NS_XAL,
    "xlink": "http://www.w3.org/1999/xlink",
}
for _p, _u in XML_PREFIXES.items():
    ET.register_namespace(_p, _u)


# ==========================================================================
# Teil 1: Download
# ==========================================================================

def http_get(url: str, timeout: int = 300, retries: int = 3) -> bytes:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "lod2-gewerbe-tool/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last_err = e
            wait = 5 * attempt
            print(f"    ! Versuch {attempt}/{retries} fehlgeschlagen ({e}), warte {wait}s ...")
            time.sleep(wait)
    raise RuntimeError(f"Download fehlgeschlagen: {url} ({last_err})")


def parse_metalink(data: bytes) -> list[dict]:
    """Liest eine .meta4-Datei und gibt [{name, url, sha256}] zurück."""
    root = ET.fromstring(data)
    ml = f"{{{NS_METALINK}}}"
    files = []
    for f in root.iter(f"{ml}file"):
        name = f.get("name")
        url_el = f.find(f"{ml}url")
        hash_el = None
        for h in f.findall(f"{ml}hash"):
            if (h.get("type") or "").lower() in ("sha-256", "sha256"):
                hash_el = h
                break
        if name and url_el is not None and url_el.text:
            files.append({
                "name": name,
                "url": url_el.text.strip(),
                "sha256": hash_el.text.strip().lower() if hash_el is not None and hash_el.text else None,
            })
    return files


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download_gebiet(ags: str, name: str, ziel: Path) -> int:
    print(f"\n=== {name} (AGS {ags}) ===")
    url = METALINK_URL.format(ags=ags)
    try:
        meta = http_get(url, timeout=60)
    except RuntimeError as e:
        print(f"  !! Metalink nicht erreichbar: {e}")
        print(f"     Prüfe ggf. den Schlüssel in gemeinde.kml / landkreis.kml unter")
        print(f"     https://geodaten.bayern.de/odd/a/lod2/citygml/meta/kml/")
        return 0

    tiles = parse_metalink(meta)
    print(f"  {len(tiles)} Kacheln laut Metalink")
    ziel.mkdir(parents=True, exist_ok=True)

    geladen = 0
    for i, t in enumerate(tiles, 1):
        out = ziel / t["name"]
        if out.exists() and t["sha256"] and sha256_of(out) == t["sha256"]:
            print(f"  [{i}/{len(tiles)}] {t['name']} — vorhanden & verifiziert, übersprungen")
            continue
        print(f"  [{i}/{len(tiles)}] {t['name']} — lade ...")
        data = http_get(t["url"])
        if t["sha256"]:
            got = hashlib.sha256(data).hexdigest()
            if got != t["sha256"]:
                print(f"    !! Checksummen-Fehler bei {t['name']} — Kachel wird übersprungen!")
                continue
        out.write_bytes(data)
        geladen += 1
    print(f"  -> {geladen} Kacheln neu geladen, Ziel: {ziel}")
    return geladen


# ==========================================================================
# Teil 2: Filtern
# ==========================================================================

def code_aus_funktion(text: str | None) -> str | None:
    """'31001_2020' -> '2020'; nackte Codes werden durchgereicht."""
    if not text:
        return None
    m = re.search(r"(\d{4})\s*$", text.strip())
    return m.group(1) if m else None


def ist_gewerbe(code: str, mit_mischnutzung: bool, mit_versorgung: bool,
                mit_landwirtschaft: bool) -> bool:
    if code in MISCHNUTZUNG_CODES:
        return mit_mischnutzung
    if not code.startswith("2"):
        return False
    # 24xx Versorgung / 25xx Entsorgung / 26xx+27xx Land-/Forstwirtschaft etc.
    if code.startswith(("24", "25")):
        return mit_versorgung
    if code.startswith(("26", "27")):
        return mit_landwirtschaft
    return True  # 20xx-23xx: Handel, Büro, Industrie, Lager, ...


def _findall_ns(elem: ET.Element, local: str, ns_key: str) -> list[ET.Element]:
    out: list[ET.Element] = []
    for uri in NS_CANDIDATES[ns_key]:
        out.extend(elem.iter(f"{{{uri}}}{local}"))
    return out


def _first_text(elem: ET.Element, local: str, ns_key: str) -> str | None:
    for uri in NS_CANDIDATES[ns_key]:
        found = elem.find(f".//{{{uri}}}{local}")
        if found is not None and found.text:
            return found.text.strip()
    return None


def ground_polygons(building: ET.Element) -> list[list[tuple[float, float]]]:
    """2D-Grundriss-Ringe aus allen GroundSurface-Elementen extrahieren."""
    rings: list[list[tuple[float, float]]] = []
    for gs in _findall_ns(building, "GroundSurface", "bldg"):
        for pos_list in gs.iter(f"{{{NS_GML}}}posList"):
            if not pos_list.text:
                continue
            vals = [float(v) for v in pos_list.text.split()]
            dim = int(pos_list.get("srsDimension", "3"))
            ring = [(vals[i], vals[i + 1]) for i in range(0, len(vals) - dim + 1, dim)]
            if len(ring) >= 4:
                rings.append(ring)
    return rings


def extrahiere_adresse(building: ET.Element) -> str | None:
    teile = []
    for local in ("ThoroughfareName", "ThoroughfareNumber", "PostalCodeNumber", "LocalityName"):
        el = building.find(f".//{{{NS_XAL}}}{local}")
        if el is not None and el.text:
            teile.append(el.text.strip())
    return " ".join(teile) if teile else None


def generische_attribute(building: ET.Element) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for uri in NS_CANDIDATES["gen"]:
        for kind in ("stringAttribute", "doubleAttribute", "intAttribute", "dateAttribute"):
            for a in building.iter(f"{{{uri}}}{kind}"):
                name = a.get("name")
                val = a.find(f"{{{uri}}}value")
                if name and val is not None and val.text:
                    attrs[name] = val.text.strip()
    return attrs


def filtere_kacheln(quelle: Path, args: argparse.Namespace) -> tuple[list[dict], list[bytes]]:
    features: list[dict] = []
    gml_members: list[bytes] = []
    gml_dateien = sorted(quelle.rglob("*.gml"))
    if not gml_dateien:
        print(f"!! Keine .gml-Dateien unter {quelle} gefunden. Erst --download ausführen?")
        sys.exit(1)

    member_tags = {f"{{{uri}}}cityObjectMember" for uri in NS_CANDIDATES["core"]}
    gesamt = 0

    for n, pfad in enumerate(gml_dateien, 1):
        gefunden_in_datei = 0
        try:
            for _, elem in ET.iterparse(pfad, events=("end",)):
                if elem.tag not in member_tags:
                    continue
                gesamt += 1
                fn_text = _first_text(elem, "function", "bldg")
                code = code_aus_funktion(fn_text)
                if code and ist_gewerbe(code, args.mit_mischnutzung,
                                        args.mit_versorgung, args.mit_landwirtschaft):
                    buildings = _findall_ns(elem, "Building", "bldg")
                    b = buildings[0] if buildings else elem
                    gml_id = b.get(f"{{{NS_GML}}}id")
                    gen = generische_attribute(b)
                    props = {
                        "gml_id": gml_id,
                        "funktion_code": code,
                        "funktion": FUNKTION_LABELS.get(code, fn_text),
                        "hoehe_m": _first_text(b, "measuredHeight", "bldg"),
                        "dachform_code": _first_text(b, "roofType", "bldg"),
                        "adresse": extrahiere_adresse(b),
                        "kachel": pfad.name,
                        "gemeindeschluessel": gen.get("Gemeindeschluessel"),
                    }
                    rings = ground_polygons(b)
                    geometry = None
                    if rings:
                        if len(rings) == 1:
                            geometry = {"type": "Polygon", "coordinates": [rings[0]]}
                        else:
                            geometry = {"type": "MultiPolygon",
                                        "coordinates": [[r] for r in rings]}
                    features.append({"type": "Feature", "geometry": geometry,
                                     "properties": props})
                    if args.gml_export:
                        gml_members.append(ET.tostring(elem, encoding="utf-8"))
                    gefunden_in_datei += 1
                elem.clear()
        except ET.ParseError as e:
            print(f"  !! Parse-Fehler in {pfad.name}: {e} — Datei übersprungen")
            continue
        print(f"[{n}/{len(gml_dateien)}] {pfad.name}: {gefunden_in_datei} Gewerbegebäude")

    print(f"\nGesamt: {len(features)} Gewerbegebäude von {gesamt} Gebäuden")
    return features, gml_members


def schreibe_geojson(features: list[dict], pfad: Path) -> None:
    fc = {
        "type": "FeatureCollection",
        "name": "gewerbe_gebaeude_lod2",
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::25832"}},
        "features": features,
    }
    pfad.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")
    print(f"-> {pfad}  ({len(features)} Features, EPSG:25832)")


def schreibe_csv(features: list[dict], pfad: Path) -> None:
    if not features:
        return
    felder = list(features[0]["properties"].keys()) + ["schwerpunkt_x", "schwerpunkt_y"]
    with open(pfad, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=felder, delimiter=";")
        w.writeheader()
        for f in features:
            row = dict(f["properties"])
            geom = f.get("geometry")
            if geom:
                coords = (geom["coordinates"][0] if geom["type"] == "Polygon"
                          else geom["coordinates"][0][0])
                xs = [c[0] for c in coords]
                ys = [c[1] for c in coords]
                row["schwerpunkt_x"] = round(sum(xs) / len(xs), 2)
                row["schwerpunkt_y"] = round(sum(ys) / len(ys), 2)
            w.writerow(row)
    print(f"-> {pfad}")


def schreibe_citygml(members: list[bytes], pfad: Path) -> None:
    header = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<core:CityModel'
        ' xmlns:core="http://www.opengis.net/citygml/2.0"'
        ' xmlns:bldg="http://www.opengis.net/citygml/building/2.0"'
        ' xmlns:gen="http://www.opengis.net/citygml/generics/2.0"'
        ' xmlns:gml="http://www.opengis.net/gml"'
        ' xmlns:xAL="urn:oasis:names:tc:ciq:xsdschema:xAL:2.0"'
        ' xmlns:xlink="http://www.w3.org/1999/xlink">\n'
    )
    with open(pfad, "wb") as fh:
        fh.write(header.encode())
        for m in members:
            fh.write(m)
            fh.write(b"\n")
        fh.write(b"</core:CityModel>\n")
    print(f"-> {pfad}  ({len(members)} Gebäude, 3D-CityGML)")


# ==========================================================================
# CLI
# ==========================================================================

def main() -> None:
    p = argparse.ArgumentParser(
        description="Gewerbegebäude aus Bayern-LoD2 für Nürnberg + Umland",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Gebiete:\n" + "\n".join(f"  {k}  {v}" for k, v in GEBIETE.items()),
    )
    p.add_argument("--download", action="store_true", help="Kacheln herunterladen")
    p.add_argument("--filter", action="store_true", help="Gewerbegebäude filtern + exportieren")
    p.add_argument("--all", action="store_true", help="Download + Filter")
    p.add_argument("--daten", default="lod2_daten", help="Verzeichnis für Roh-Kacheln")
    p.add_argument("--out", default="ergebnis", help="Verzeichnis für Ergebnisse")
    p.add_argument("--nur", nargs="+", metavar="AGS",
                   help="nur diese Schlüssel verarbeiten (Standard: alle 8 Gebiete)")
    p.add_argument("--gml-export", action="store_true",
                   help="zusätzlich gefiltertes 3D-CityGML schreiben")
    p.add_argument("--mit-mischnutzung", action="store_true",
                   help="auch Wohn-/Geschäftsgebäude (1120/1130/1220/1230)")
    p.add_argument("--mit-versorgung", action="store_true",
                   help="auch Ver-/Entsorgungsgebäude (24xx/25xx)")
    p.add_argument("--mit-landwirtschaft", action="store_true",
                   help="auch land-/forstwirtschaftliche Gebäude (26xx/27xx)")
    args = p.parse_args()

    if not (args.download or args.filter or args.all):
        p.print_help()
        sys.exit(0)

    gebiete = GEBIETE
    if args.nur:
        unbekannt = [a for a in args.nur if a not in GEBIETE]
        gebiete = {a: GEBIETE.get(a, f"AGS {a}") for a in args.nur}
        if unbekannt:
            print(f"Hinweis: {', '.join(unbekannt)} nicht in der Vorauswahl — versuche trotzdem.")

    daten = Path(args.daten)
    out = Path(args.out)

    if args.download or args.all:
        gesamt = 0
        for ags, name in gebiete.items():
            gesamt += download_gebiet(ags, name, daten / ags)
        print(f"\nDownload abgeschlossen: {gesamt} neue Kacheln.")

    if args.filter or args.all:
        out.mkdir(parents=True, exist_ok=True)
        quelle = daten if not args.nur else daten  # rglob findet Unterordner
        if args.nur:
            features, members = [], []
            for ags in gebiete:
                f, m = filtere_kacheln(daten / ags, args) if (daten / ags).exists() else ([], [])
                features.extend(f)
                members.extend(m)
        else:
            features, members = filtere_kacheln(quelle, args)
        schreibe_geojson(features, out / "gewerbe_gebaeude.geojson")
        schreibe_csv(features, out / "gewerbe_gebaeude.csv")
        if args.gml_export:
            schreibe_citygml(members, out / "gewerbe_lod2.gml")


if __name__ == "__main__":
    main()