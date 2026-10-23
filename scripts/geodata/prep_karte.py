#!/usr/bin/env python3
"""
prep_karte.py — Bereitet die LoD2-Kacheln für die Streamlit-Kartenapp auf.

Extrahiert alle Gewerbegebäude mit Grundriss-Polygon (WGS84), Dachfläche (3D),
Grundfläche, Höhe, Funktion und Adresse und schreibt eine kompakte GeoJSON,
die app_karte.py direkt laden kann.

Beispiel:
  python3 prep_karte.py --daten lod2_daten --out karte_daten.geojson
  python3 prep_karte.py --daten lod2_daten --min-dach 200   # kleinere Datei

Benötigt: pyproj  (pip install pyproj)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
import xml.etree.ElementTree as ET

try:
    from pyproj import Transformer
except ImportError:
    sys.exit("pyproj fehlt: pip install pyproj")

FUNKTION_LABELS = {
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
    "2310": "Gebäude für Handel und Dienstleistung mit Wohnen",
    "2320": "Gebäude für Gewerbe und Industrie mit Wohnen",
}
MISCHNUTZUNG_CODES = {"1120", "1130", "1220", "1230"}

NS_CORE = ["http://www.opengis.net/citygml/2.0", "http://www.opengis.net/citygml/1.0"]
NS_BLDG = ["http://www.opengis.net/citygml/building/2.0",
           "http://www.opengis.net/citygml/building/1.0"]
NS_GEN = ["http://www.opengis.net/citygml/generics/2.0",
          "http://www.opengis.net/citygml/generics/1.0"]
NS_GML = "http://www.opengis.net/gml"
NS_XAL = "urn:oasis:names:tc:ciq:xsdschema:xAL:2.0"

TRANSFORMER = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)


def code_aus_funktion(text):
    if not text:
        return None
    m = re.search(r"(\d{4})\s*$", text.strip())
    return m.group(1) if m else None


def ist_gewerbe(code, mit_mischnutzung, mit_versorgung, mit_landwirtschaft):
    if code in MISCHNUTZUNG_CODES:
        return mit_mischnutzung
    if not code.startswith("2"):
        return False
    if code.startswith(("24", "25")):
        return mit_versorgung
    if code.startswith(("26", "27")):
        return mit_landwirtschaft
    return True


def _first_text(elem, local, ns_list):
    for uri in ns_list:
        found = elem.find(f".//{{{uri}}}{local}")
        if found is not None and found.text:
            return found.text.strip()
    return None


def _iter_ns(elem, local, ns_list):
    for uri in ns_list:
        yield from elem.iter(f"{{{uri}}}{local}")


def _ringe(surface):
    for poly in surface.iter(f"{{{NS_GML}}}Polygon"):
        for kind, tag in (("ext", "exterior"), ("int", "interior")):
            for boundary in poly.findall(f"{{{NS_GML}}}{tag}"):
                for pos_list in boundary.iter(f"{{{NS_GML}}}posList"):
                    if not pos_list.text:
                        continue
                    vals = [float(v) for v in pos_list.text.split()]
                    dim = int(pos_list.get("srsDimension", "3"))
                    pts = [tuple(vals[i:i + dim]) for i in range(0, len(vals) - dim + 1, dim)]
                    if len(pts) >= 4:
                        yield kind, pts


def flaeche_3d(pts):
    nx = ny = nz = 0.0
    for i in range(len(pts) - 1):
        x1, y1, z1 = pts[i][0], pts[i][1], (pts[i][2] if len(pts[i]) > 2 else 0.0)
        x2, y2, z2 = pts[i + 1][0], pts[i + 1][1], (pts[i + 1][2] if len(pts[i + 1]) > 2 else 0.0)
        nx += (y1 - y2) * (z1 + z2)
        ny += (z1 - z2) * (x1 + x2)
        nz += (x1 - x2) * (y1 + y2)
    return 0.5 * (nx * nx + ny * ny + nz * nz) ** 0.5


def flaeche_2d(pts):
    a = 0.0
    for i in range(len(pts) - 1):
        a += pts[i][0] * pts[i + 1][1] - pts[i + 1][0] * pts[i][1]
    return abs(a) / 2.0


def surface_flaeche(building, local, dreid):
    total = 0.0
    for s in _iter_ns(building, local, NS_BLDG):
        for kind, pts in _ringe(s):
            f = flaeche_3d(pts) if dreid else flaeche_2d(pts)
            total += f if kind == "ext" else -f
    return max(total, 0.0)


def grundriss_wgs84(building):
    """Alle GroundSurface-Exterior-Ringe nach WGS84 (lon/lat, 5 Nachkommastellen ~ 1 m)."""
    rings = []
    for s in _iter_ns(building, "GroundSurface", NS_BLDG):
        for kind, pts in _ringe(s):
            if kind != "ext":
                continue
            lons, lats = TRANSFORMER.transform([p[0] for p in pts], [p[1] for p in pts])
            rings.append([[round(lo, 6), round(la, 6)] for lo, la in zip(lons, lats)])
    return rings


def adresse(building):
    teile = []
    for local in ("ThoroughfareName", "ThoroughfareNumber", "PostalCodeNumber", "LocalityName"):
        el = building.find(f".//{{{NS_XAL}}}{local}")
        if el is not None and el.text:
            teile.append(el.text.strip())
    return " ".join(teile) if teile else None


def gemeindeschluessel(building):
    for uri in NS_GEN:
        for a in building.iter(f"{{{uri}}}stringAttribute"):
            if a.get("name") == "Gemeindeschluessel":
                v = a.find(f"{{{uri}}}value")
                if v is not None and v.text:
                    return v.text.strip()
    return None


def main():
    p = argparse.ArgumentParser(description="LoD2-Gewerbedaten für die Kartenapp aufbereiten")
    p.add_argument("--daten", default="lod2_daten")
    p.add_argument("--out", default="karte_daten.geojson")
    p.add_argument("--min-dach", type=float, default=0.0,
                   help="Gebäude unterhalb dieser Dachfläche weglassen (kleinere Datei)")
    p.add_argument("--mit-mischnutzung", action="store_true")
    p.add_argument("--mit-versorgung", action="store_true")
    p.add_argument("--mit-landwirtschaft", action="store_true")
    args = p.parse_args()

    quelle = Path(args.daten)
    gml_dateien = sorted(quelle.rglob("*.gml"))
    if not gml_dateien:
        sys.exit(f"Keine .gml-Dateien unter {quelle} gefunden.")

    member_tags = {f"{{{uri}}}cityObjectMember" for uri in NS_CORE}
    features = []

    for n, pfad in enumerate(gml_dateien, 1):
        try:
            for _, elem in ET.iterparse(pfad, events=("end",)):
                if elem.tag not in member_tags:
                    continue
                code = code_aus_funktion(_first_text(elem, "function", NS_BLDG))
                if not code or not ist_gewerbe(code, args.mit_mischnutzung,
                                               args.mit_versorgung, args.mit_landwirtschaft):
                    elem.clear()
                    continue
                dach = surface_flaeche(elem, "RoofSurface", dreid=True)
                if dach < args.min_dach:
                    elem.clear()
                    continue
                rings = grundriss_wgs84(elem)
                if not rings:
                    elem.clear()
                    continue
                b = next(iter(_iter_ns(elem, "Building", NS_BLDG)), elem)
                hoehe = _first_text(elem, "measuredHeight", NS_BLDG)
                geometry = ({"type": "Polygon", "coordinates": [rings[0]]}
                            if len(rings) == 1 else
                            {"type": "MultiPolygon", "coordinates": [[r] for r in rings]})
                features.append({
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": {
                        "gml_id": b.get(f"{{{NS_GML}}}id"),
                        "funktion_code": code,
                        "funktion": FUNKTION_LABELS.get(code, code),
                        "dachflaeche_qm": round(dach, 1),
                        "grundflaeche_qm": round(surface_flaeche(elem, "GroundSurface", False), 1),
                        "hoehe_m": float(hoehe) if hoehe else None,
                        "adresse": adresse(elem),
                        "ags": gemeindeschluessel(elem),
                    },
                })
                elem.clear()
        except ET.ParseError as e:
            print(f"  !! Parse-Fehler in {pfad.name}: {e} — übersprungen")
        if n % 25 == 0 or n == len(gml_dateien):
            print(f"  ... {n}/{len(gml_dateien)} Kacheln, {len(features)} Gebäude")

    fc = {"type": "FeatureCollection", "features": features}
    Path(args.out).write_text(json.dumps(fc, ensure_ascii=False, separators=(",", ":")),
                              encoding="utf-8")
    mb = Path(args.out).stat().st_size / 1e6
    print(f"\n-> {args.out}  ({len(features)} Gebäude, {mb:.1f} MB)")
    print("Jetzt starten mit:  streamlit run app_karte.py")


if __name__ == "__main__":
    main()