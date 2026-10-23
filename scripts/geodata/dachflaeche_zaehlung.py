#!/usr/bin/env python3
"""
dachflaeche_zaehlung.py — Zählt Gewerbegebäude in den bereits heruntergeladenen
Bayern-LoD2-Kacheln nach Dachfläche.

Dachfläche = Summe aller RoofSurface-Polygone eines Gebäudes als echte
3D-Fläche (geneigte Dächer zählen also mit ihrer tatsächlichen Fläche,
nicht nur der Grundriss-Projektion). Zusätzlich wird die Grundfläche
(GroundSurface, 2D) mit ausgegeben.

Beispiele:
  python3 dachflaeche_zaehlung.py --daten lod2_daten
  python3 dachflaeche_zaehlung.py --daten lod2_daten --min 500 --max 20000
  python3 dachflaeche_zaehlung.py --daten lod2_daten --min 500 --max 20000 \\
      --csv treffer.csv --geojson treffer.geojson

Nur Python-Standardbibliothek (Python >= 3.9).
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path
import xml.etree.ElementTree as ET

# ---- gleiche Klassifikation wie in lod2_gewerbe.py ------------------------

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


def code_aus_funktion(text: str | None) -> str | None:
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
    if code.startswith(("24", "25")):
        return mit_versorgung
    if code.startswith(("26", "27")):
        return mit_landwirtschaft
    return True


def _first_text(elem: ET.Element, local: str, ns_list: list[str]) -> str | None:
    for uri in ns_list:
        found = elem.find(f".//{{{uri}}}{local}")
        if found is not None and found.text:
            return found.text.strip()
    return None


def _iter_ns(elem: ET.Element, local: str, ns_list: list[str]):
    for uri in ns_list:
        yield from elem.iter(f"{{{uri}}}{local}")


# ---- Geometrie ------------------------------------------------------------

def _ringe(surface: ET.Element, dim_default: int = 3):
    """Liefert (exterior?, ring) für alle LinearRings eines Surface-Elements."""
    for poly in surface.iter(f"{{{NS_GML}}}Polygon"):
        for kind, tag in (("ext", "exterior"), ("int", "interior")):
            for boundary in poly.findall(f"{{{NS_GML}}}{tag}"):
                for pos_list in boundary.iter(f"{{{NS_GML}}}posList"):
                    if not pos_list.text:
                        continue
                    vals = [float(v) for v in pos_list.text.split()]
                    dim = int(pos_list.get("srsDimension", str(dim_default)))
                    pts = [tuple(vals[i:i + dim]) for i in range(0, len(vals) - dim + 1, dim)]
                    if len(pts) >= 4:
                        yield kind, pts


def flaeche_3d(pts: list[tuple]) -> float:
    """Echte Fläche eines planaren 3D-Polygons (Newell-Methode)."""
    nx = ny = nz = 0.0
    n = len(pts)
    for i in range(n - 1):  # Ring ist geschlossen (letzter Punkt = erster)
        x1, y1, z1 = pts[i][0], pts[i][1], (pts[i][2] if len(pts[i]) > 2 else 0.0)
        x2, y2, z2 = pts[i + 1][0], pts[i + 1][1], (pts[i + 1][2] if len(pts[i + 1]) > 2 else 0.0)
        nx += (y1 - y2) * (z1 + z2)
        ny += (z1 - z2) * (x1 + x2)
        nz += (x1 - x2) * (y1 + y2)
    return 0.5 * (nx * nx + ny * ny + nz * nz) ** 0.5


def flaeche_2d(pts: list[tuple]) -> float:
    """Projizierte Grundfläche (Shoelace, nur x/y)."""
    a = 0.0
    for i in range(len(pts) - 1):
        a += pts[i][0] * pts[i + 1][1] - pts[i + 1][0] * pts[i][1]
    return abs(a) / 2.0


def surface_flaeche(building: ET.Element, local: str, dreid: bool) -> float:
    """Summierte Fläche aller Surfaces eines Typs (Löcher werden abgezogen)."""
    total = 0.0
    for s in _iter_ns(building, local, NS_BLDG):
        for kind, pts in _ringe(s):
            f = flaeche_3d(pts) if dreid else flaeche_2d(pts)
            total += f if kind == "ext" else -f
    return max(total, 0.0)


def adresse(building: ET.Element) -> str | None:
    teile = []
    for local in ("ThoroughfareName", "ThoroughfareNumber", "PostalCodeNumber", "LocalityName"):
        el = building.find(f".//{{{NS_XAL}}}{local}")
        if el is not None and el.text:
            teile.append(el.text.strip())
    return " ".join(teile) if teile else None


def schwerpunkt(building: ET.Element) -> tuple[float, float] | None:
    for s in _iter_ns(building, "GroundSurface", NS_BLDG):
        for kind, pts in _ringe(s):
            if kind == "ext":
                xs = [p[0] for p in pts]
                ys = [p[1] for p in pts]
                return (round(sum(xs) / len(xs), 2), round(sum(ys) / len(ys), 2))
    return None


def gemeindeschluessel(building: ET.Element) -> str | None:
    for uri in NS_GEN:
        for a in building.iter(f"{{{uri}}}stringAttribute"):
            if a.get("name") == "Gemeindeschluessel":
                v = a.find(f"{{{uri}}}value")
                if v is not None and v.text:
                    return v.text.strip()
    return None


# ---- Hauptlauf ------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="Gewerbegebäude nach Dachfläche zählen")
    p.add_argument("--daten", default="lod2_daten", help="Verzeichnis mit den .gml-Kacheln")
    p.add_argument("--min", type=float, default=500.0, help="minimale Dachfläche in m²")
    p.add_argument("--max", type=float, default=20000.0, help="maximale Dachfläche in m²")
    p.add_argument("--csv", metavar="DATEI", help="Treffer als CSV exportieren")
    p.add_argument("--geojson", metavar="DATEI", help="Treffer als GeoJSON (Punkte) exportieren")
    p.add_argument("--mit-mischnutzung", action="store_true")
    p.add_argument("--mit-versorgung", action="store_true")
    p.add_argument("--mit-landwirtschaft", action="store_true")
    args = p.parse_args()

    quelle = Path(args.daten)
    gml_dateien = sorted(quelle.rglob("*.gml"))
    if not gml_dateien:
        print(f"!! Keine .gml-Dateien unter {quelle} gefunden.")
        sys.exit(1)

    member_tags = {f"{{{uri}}}cityObjectMember" for uri in NS_CORE}
    bins = [(0, 500), (500, 1000), (1000, 2500), (2500, 5000),
            (5000, 10000), (10000, 20000), (20000, float("inf"))]
    bin_counter: Counter = Counter()
    funk_counter: Counter = Counter()
    gemeinde_counter: Counter = Counter()

    gewerbe_gesamt = 0
    treffer: list[dict] = []

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
                gewerbe_gesamt += 1
                dach = surface_flaeche(elem, "RoofSurface", dreid=True)
                for lo, hi in bins:
                    if lo <= dach < hi:
                        bin_counter[(lo, hi)] += 1
                        break
                if args.min <= dach <= args.max:
                    grund = surface_flaeche(elem, "GroundSurface", dreid=False)
                    ags = gemeindeschluessel(elem)
                    funk_counter[code] += 1
                    if ags:
                        gemeinde_counter[ags] += 1
                    b = next(iter(_iter_ns(elem, "Building", NS_BLDG)), elem)
                    sp = schwerpunkt(elem)
                    treffer.append({
                        "gml_id": b.get(f"{{{NS_GML}}}id"),
                        "funktion_code": code,
                        "funktion": FUNKTION_LABELS.get(code, code),
                        "dachflaeche_qm": round(dach, 1),
                        "grundflaeche_qm": round(grund, 1),
                        "hoehe_m": _first_text(elem, "measuredHeight", NS_BLDG),
                        "adresse": adresse(elem),
                        "gemeindeschluessel": ags,
                        "kachel": pfad.name,
                        "x": sp[0] if sp else None,
                        "y": sp[1] if sp else None,
                    })
                elem.clear()
        except ET.ParseError as e:
            print(f"  !! Parse-Fehler in {pfad.name}: {e} — übersprungen")
        if n % 25 == 0 or n == len(gml_dateien):
            print(f"  ... {n}/{len(gml_dateien)} Kacheln, bisher {len(treffer)} Treffer")

    # ---- Bericht ----
    print("\n" + "=" * 62)
    print(f"Gewerbegebäude gesamt:                 {gewerbe_gesamt:>8}")
    print(f"Davon Dachfläche {args.min:.0f}–{args.max:.0f} m²:   {len(treffer):>8}")
    print("=" * 62)

    print("\nVerteilung nach Dachfläche (alle Gewerbegebäude):")
    for (lo, hi), c in sorted(bin_counter.items()):
        label = f"{lo:>6.0f} – {hi:.0f} m²" if hi != float("inf") else f"{lo:>6.0f} m² und mehr"
        print(f"  {label:<24} {c:>7}")

    print(f"\nTop-Gebäudefunktionen im Bereich {args.min:.0f}–{args.max:.0f} m²:")
    for code, c in funk_counter.most_common(15):
        print(f"  {code}  {FUNKTION_LABELS.get(code, '?'):<45} {c:>6}")

    if gemeinde_counter:
        print("\nTreffer nach Gemeindeschlüssel (Top 15):")
        for ags, c in gemeinde_counter.most_common(15):
            print(f"  {ags}   {c:>6}")

    # ---- Export ----
    if args.csv and treffer:
        with open(args.csv, "w", newline="", encoding="utf-8-sig") as fh:
            w = csv.DictWriter(fh, fieldnames=list(treffer[0].keys()), delimiter=";")
            w.writeheader()
            w.writerows(treffer)
        print(f"\n-> {args.csv} ({len(treffer)} Zeilen)")

    if args.geojson and treffer:
        fc = {
            "type": "FeatureCollection",
            "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::25832"}},
            "features": [
                {"type": "Feature",
                 "geometry": ({"type": "Point", "coordinates": [t["x"], t["y"]]}
                              if t["x"] is not None else None),
                 "properties": {k: v for k, v in t.items() if k not in ("x", "y")}}
                for t in treffer
            ],
        }
        Path(args.geojson).write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")
        print(f"-> {args.geojson}")


if __name__ == "__main__":
    main()