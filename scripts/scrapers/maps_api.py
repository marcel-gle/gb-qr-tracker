"""
Google Maps Eventlocation Scraper (Bayern / Baden-Württemberg / Hessen)
=======================================================================
Durchsucht Google Places API nach Eventlocations in konfigurierbaren Regionen
(PLZ-Bias-Kreise + Stadt-Booster + optionales 1km-Gitter). Export als CSV.

Nutzung (Projektroot):

    python scripts/scrapers/maps_api.py --api-key KEY --region baden-wuerttemberg
    python scripts/scrapers/maps_api.py --api-key KEY --region hessen --target-unique 1000

    python scripts/scrapers/maps_api.py --help

Kosten-Schätzung:
    - Text Search (New) inkl. Field Mask: siehe Google-Preisrechner
    - Pro Region/Query bis zu 3 Seiten (max. 60 Treffer)
"""

import argparse
import json
import math
import os
import time
import csv
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional

try:
    import requests
except ImportError:
    print("Installing requests...")
    os.system(f"{sys.executable} -m pip install requests --break-system-packages -q")
    import requests


# ============================================================
# KONFIGURATION
# ============================================================

# Volle Suchbegriffe (--queries-full); Standard ohne CLI sind 4 kompakte Begriffe.
_SEARCH_QUERIES_FULL = [
    "Eventlocation",
    "Veranstaltungsraum",
    "Tagungsraum",
    "Festsaal",
    "Hochzeitslocation",
    "Kongresshalle",
    "Eventhalle",
    "Seminarraum",
]

_SEARCH_QUERIES_COMPACT = [
    "Eventlocation",
    "Veranstaltungsraum",
    "Tagungsraum",
    "Hochzeitslocation",
]

_CFG: Optional["ScraperConfig"] = None


@dataclass
class ScraperConfig:
    api_key: str
    region_preset: str
    search_queries: List[str]
    target_unique: Optional[int]
    use_1km_grid: bool
    bw_booster_radius_m: int
    bw_plz_redundant_km: float
    output_dir: Path
    timestamp: str = field(init=False, default="")

    def __post_init__(self):
        self.bw_booster_radius_m = max(1000, min(int(self.bw_booster_radius_m), 50000))
        self.bw_plz_redundant_km = max(0.0, min(float(self.bw_plz_redundant_km), 50.0))
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    @property
    def progress_file(self) -> Path:
        return self.output_dir / f"scraper_progress_{self.region_preset}.json"

    @property
    def output_csv(self) -> Path:
        return self.output_dir / f"eventlocations_{self.region_preset}_{self.timestamp}.csv"


def _cfg() -> ScraperConfig:
    if _CFG is None:
        raise RuntimeError("Scraper not configured; run via main() with CLI arguments.")
    return _CFG


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Google Places (New) Text Search — Eventlocations nach Region.",
        epilog=(
            "Beispiel BW:  %(prog)s --api-key KEY --region bw --target-unique 0 --no-1km-grid\n"
            "Hinweis: API-Keys in der Shell können in der Prozessliste sichtbar sein."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--api-key",
        required=True,
        help="Google Maps API key (Places API New enabled)",
    )
    p.add_argument(
        "--region",
        default="bayern",
        metavar="PRESET",
        help="Region preset: bayern | baden-wuerttemberg | bw | hessen | he | test (default: bayern)",
    )
    p.add_argument(
        "--target-unique",
        type=int,
        default=6000,
        metavar="N",
        help="Stop after N unique places; use 0 for no limit (default: 6000)",
    )
    p.add_argument(
        "--no-1km-grid",
        action="store_true",
        help="Disable dense ~1 km grid regions (saves API calls)",
    )
    p.add_argument(
        "--bw-booster-radius-m",
        type=int,
        default=30000,
        metavar="M",
        help="Baden-Württemberg booster circle radius in meters (default: 30000)",
    )
    p.add_argument(
        "--bw-plz-redundant-km",
        type=float,
        default=5.0,
        metavar="KM",
        help="Skip BW boosters within this distance (km) of any BW PLZ anchor (0=off; default: 5)",
    )
    q = p.add_mutually_exclusive_group()
    q.add_argument(
        "--queries",
        default=None,
        metavar="Q1,Q2,...",
        help="Comma-separated text search queries (overrides default compact set)",
    )
    q.add_argument(
        "--queries-full",
        action="store_true",
        help="Use all 8 built-in query strings instead of the 4 compact defaults",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        metavar="DIR",
        help="Directory for CSV and progress JSON (default: output)",
    )
    return p.parse_args(argv)


def _build_config(args: argparse.Namespace) -> ScraperConfig:
    if args.queries_full:
        queries = list(_SEARCH_QUERIES_FULL)
    elif args.queries:
        queries = [q.strip() for q in args.queries.split(",") if q.strip()]
        if not queries:
            sys.exit("error: --queries produced an empty list")
    else:
        queries = list(_SEARCH_QUERIES_COMPACT)

    target = None if args.target_unique <= 0 else args.target_unique

    return ScraperConfig(
        api_key=args.api_key.strip(),
        region_preset=args.region.strip(),
        search_queries=queries,
        target_unique=target,
        use_1km_grid=not args.no_1km_grid,
        bw_booster_radius_m=args.bw_booster_radius_m,
        bw_plz_redundant_km=args.bw_plz_redundant_km,
        output_dir=args.output_dir,
    )

# Welche Felder von Google abgefragt werden (bestimmt den Preis!)
# Basic-Felder (im Text Search Preis enthalten):
#   displayName, formattedAddress, location, types, rating, userRatingCount
# Contact-Felder (Extra-Kosten ~$3/1000):
#   websiteUri, nationalPhoneNumber
FIELD_MASK = ",".join([
    "places.id",
    "places.displayName",
    "places.formattedAddress",
    "places.location",
    "places.types",
    "places.rating",
    "places.userRatingCount",
    "places.websiteUri",
    "places.nationalPhoneNumber",
    "places.googleMapsUri",
    "nextPageToken",
])

# Rate Limiting
REQUESTS_PER_SECOND = 5
DELAY_BETWEEN_REQUESTS = 1.0 / REQUESTS_PER_SECOND

# PLZ-weit 20 km; Großstädte 12 km; Kernstadt 1 km (sehr viele API-Calls)
DEFAULT_REGION_RADIUS_M = 20000
CITY_BOOSTER_RADIUS_M = 12000
DENSE_CITY_1KM_RADIUS_M = 1000

# ~1 km Abstand der Gitterpunkte (Breitengrad / Mittelfranken-Nähe)
_GRID_STEP_LAT = 0.009
_GRID_STEP_LON = 0.012

# ============================================================
# PLZ-GEBIETE DEFINIEREN
# ============================================================

# Bayern: PLZ 80–87 und 90–97 (ohne 88/89 — überwiegend BW). Format: (label, Anzeigename, lat, lon)
BAYERN_PLZ_REGIONS = [
    ("80", "PLZ 80 – München/Süd", 48.137, 11.576),
    ("81", "PLZ 81 – München-Ost", 48.122, 11.619),
    ("82", "PLZ 82 – Starnberg/Oberland", 47.868, 11.360),
    ("83", "PLZ 83 – Rosenheim", 47.856, 12.128),
    ("84", "PLZ 84 – Landshut", 48.537, 12.152),
    ("85", "PLZ 85 – Freising/Ingolstadt", 48.570, 11.600),
    ("86", "PLZ 86 – Augsburg", 48.366, 10.894),
    ("87", "PLZ 87 – Kempten/Allgäu", 47.727, 10.316),
    ("90", "PLZ 90 – Nürnberg", 49.454, 11.077),
    ("91", "PLZ 91 – Erlangen/Ansbach", 49.444, 10.963),
    ("92", "PLZ 92 – Amberg/Weiden", 49.443, 11.854),
    ("93", "PLZ 93 – Regensburg", 49.013, 12.102),
    ("94", "PLZ 94 – Passau", 48.567, 13.432),
    ("95", "PLZ 95 – Bayreuth/Hof", 50.022, 11.574),
    ("96", "PLZ 96 – Bamberg/Coburg", 50.095, 10.891),
    ("97", "PLZ 97 – Würzburg", 49.792, 9.932),
]

# Zusätzliche Kreise in dichten Städten (andere Zentren / kleinerer Radius → mehr unterhalb des 60er-Paging-Limits)
BAYERN_CITY_BOOSTERS = [
    ("MUC-N", "München Nord", 48.200, 11.575),
    ("MUC-S", "München Süd", 48.065, 11.548),
    ("MUC-O", "München Ost", 48.135, 11.720),
    ("MUC-W", "München West", 48.135, 11.420),
    ("NUE-NW", "Nürnberg NW", 49.480, 11.020),
    ("NUE-SE", "Nürnberg SE", 49.420, 11.130),
    ("NUE-O", "Nürnberg Ost", 49.455, 11.200),
    ("AUG-N", "Augsburg Nord", 48.400, 10.920),
    ("AUG-S", "Augsburg Süd", 48.330, 10.870),
    ("WUE-O", "Würzburg Ost", 49.795, 9.980),
    ("WUE-W", "Würzburg West", 49.785, 9.880),
    ("REG-N", "Regensburg Nord", 49.045, 12.090),
    ("REG-S", "Regensburg Süd", 48.985, 12.105),
    ("ING", "Ingolstadt", 48.763, 11.425),
    ("FUE", "Fürth", 49.478, 10.989),
]

# Baden-Württemberg: PLZ 70–79. Zentren = geografische Schwerpunkte pro PLZ-Bereich.
BADEN_WUERTTEMBERG_PLZ_REGIONS = [
    ("70", "PLZ 70 – Stuttgart", 48.776, 9.183),
    ("71", "PLZ 71 – Ludwigsburg/Rems-Murr", 48.897, 9.192),
    ("72", "PLZ 72 – Tübingen/Reutlingen", 48.523, 9.053),
    ("73", "PLZ 73 – Esslingen/Göppingen", 48.743, 9.307),
    ("74", "PLZ 74 – Heilbronn/Hohenlohe", 49.143, 9.210),
    ("75", "PLZ 75 – Karlsruhe Nord/Pforzheim", 48.893, 8.695),
    ("76", "PLZ 76 – Karlsruhe/Mittlerer Oberrhein", 49.007, 8.404),
    ("77", "PLZ 77 – Offenburg/Ortenau", 48.474, 7.949),
    ("78", "PLZ 78 – Schwarzwald-Baar/Bodensee-Süd", 47.955, 8.500),
    ("79", "PLZ 79 – Freiburg/Hochrhein", 47.999, 7.842),
]

# BW-Booster: kompakte Liste (größerer Radius s. MAPS_BW_BOOSTER_RADIUS_M). Stuttgart-Umfeld
# absichtlich weg — PLZ 70–73 + 20 km decken das ab. Einträge ≤ MAPS_BW_PLZ_REDUNDANT_KM zu einem
# PLZ-Zentrum werden zur Laufzeit noch ausgelassen (Doppelungen zu PLZ-Suchen).
BADEN_WUERTTEMBERG_CITY_BOOSTERS = [
    ("BW-BAC", "Backnang", 48.947, 9.402),
    ("BW-GOE", "Göppingen", 48.704, 9.652),
    ("BW-AIL", "Aalen", 48.837, 10.094),
    ("BW-GMÜ", "Schwäbisch Gmünd", 48.801, 9.798),
    ("BW-NAL", "Neckar-Alb Mitte", 48.507, 9.128),
    ("BW-ALB", "Albstadt", 48.213, 9.083),
    ("BW-ROT", "Rottweil", 48.168, 8.625),
    ("BW-SHA", "Schwäbisch Hall", 49.112, 9.737),
    ("BW-CRA", "Crailsheim", 49.135, 10.071),
    ("BW-MOS", "Mosbach", 49.354, 9.151),
    ("BW-SI", "Sinsheim", 49.251, 8.874),
    ("BW-PF", "Pforzheim", 48.892, 8.694),
    ("BW-BA", "Baden-Baden", 48.761, 8.241),
    ("BW-MHL", "Mühlacker", 48.946, 8.836),
    ("BW-OFF", "Offenburg", 48.474, 7.949),
    ("BW-FRE", "Freudenstadt", 48.463, 8.411),
    ("BW-CAL", "Calw", 48.714, 8.738),
    ("BW-MA", "Mannheim", 49.487, 8.466),
    ("BW-HD", "Heidelberg", 49.409, 8.694),
    ("BW-FDH", "Friedrichshafen", 47.657, 9.479),
    ("BW-RV", "Ravensburg", 47.782, 9.611),
    ("BW-KN", "Konstanz", 47.663, 9.175),
    ("BW-BIB", "Biberach", 48.098, 9.788),
    ("BW-VS", "Villingen-Schwenningen", 48.062, 8.494),
    ("BW-TUT", "Tuttlingen", 47.986, 8.818),
    ("BW-DON", "Donaueschingen", 47.954, 8.499),
    ("BW-LOR", "Lörrach", 47.614, 7.664),
    ("BW-WT", "Waldshut-Tiengen", 47.623, 8.214),
    ("BW-UL", "Ulm", 48.401, 9.988),
    ("BW-GÜ", "Geislingen", 48.624, 9.830),
]

# Hessen: PLZ 34–36 sowie 60–65 (inkl. Frankfurt/Rhein-Main und Nordhessen).
HESSEN_PLZ_REGIONS = [
    ("34", "PLZ 34 – Kassel/Nordhessen", 51.312, 9.479),
    ("35", "PLZ 35 – Marburg/Gießen/Wetzlar", 50.585, 8.678),
    ("36", "PLZ 36 – Fulda/Osthessen", 50.555, 9.680),
    ("60", "PLZ 60 – Frankfurt am Main", 50.110, 8.682),
    ("61", "PLZ 61 – Offenbach/Hanau", 50.105, 8.765),
    ("63", "PLZ 63 – Aschaffenburg/Rodgau Umland", 50.001, 9.042),
    ("64", "PLZ 64 – Darmstadt/Bergstraße", 49.872, 8.651),
    ("65", "PLZ 65 – Wiesbaden/Rheingau", 50.082, 8.241),
]

HESSEN_CITY_BOOSTERS = [
    ("HE-FRA-N", "Frankfurt Nord", 50.165, 8.682),
    ("HE-FRA-S", "Frankfurt Süd", 50.050, 8.682),
    ("HE-FRA-O", "Frankfurt Ost", 50.110, 8.790),
    ("HE-FRA-W", "Frankfurt West", 50.110, 8.560),
    ("HE-WI", "Wiesbaden", 50.082, 8.241),
    ("HE-MZK", "Mainz-Kastel", 50.010, 8.280),
    ("HE-DA", "Darmstadt", 49.872, 8.651),
    ("HE-OF", "Offenbach", 50.095, 8.776),
    ("HE-HU", "Hanau", 50.137, 8.916),
    ("HE-FD", "Fulda", 50.555, 9.680),
    ("HE-KS", "Kassel", 51.312, 9.479),
    ("HE-GI", "Gießen", 50.585, 8.678),
    ("HE-MR", "Marburg", 50.807, 8.770),
    ("HE-WE", "Wetzlar", 50.551, 8.499),
    ("HE-LM", "Limburg", 50.383, 8.050),
]


def _haversine_m(lat1, lon1, lat2, lon2):
    """Großkreis-Distanz in Metern (lat/lon in Dezimalgrad)."""
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def _baden_wuerttemberg_booster_radius_m():
    return _cfg().bw_booster_radius_m


def _bw_plz_redundant_km():
    return _cfg().bw_plz_redundant_km


def _filter_bw_boosters_against_plz(booster_tuples):
    """
    Entfernt Booster, deren Zentrum nahe an einem BW-PLZ-Anker liegt (gleiche Suche schon durch PLZ-20km).
    """
    threshold_m = _bw_plz_redundant_km() * 1000.0
    if threshold_m <= 0:
        return list(booster_tuples)
    anchors = [(lat, lon) for _, _, lat, lon in BADEN_WUERTTEMBERG_PLZ_REGIONS]
    kept = []
    for label, name, lat, lon in booster_tuples:
        if not anchors:
            kept.append((label, name, lat, lon))
            continue
        d_min = min(_haversine_m(lat, lon, alat, alon) for alat, alon in anchors)
        if d_min <= threshold_m:
            continue
        kept.append((label, name, lat, lon))
    return kept


# gesetzt in _regions_baden_wuerttemberg() für Ausgabe in main()
BW_LAST_BOOSTER_INFO = None  # (kandidaten, nach_filter, radius_m) oder None


def _grid_1km_labels(prefix, display_city, center_lat, center_lon, rows, cols):
    """
    Erzeugt (label, name, lat, lon) für ein rows×cols-Gitter um center_*.
    Abstände ~1 km (siehe _GRID_STEP_*). rows/cols >= 1.
    """
    out = []
    n = 0
    row_offsets = [(i - (rows - 1) / 2.0) for i in range(rows)]
    col_offsets = [(j - (cols - 1) / 2.0) for j in range(cols)]
    for i in row_offsets:
        for j in col_offsets:
            n += 1
            lat = center_lat + i * _GRID_STEP_LAT
            lon = center_lon + j * _GRID_STEP_LON
            label = f"1k-{prefix}-{n:02d}"
            name = f"{display_city} 1km Gitter {n:02d}"
            out.append((label, name, lat, lon))
    return out


def _bayer_1km_dense_regions():
    """1 km Kreise in Ballungsräumen (andere bias-Region → mehr unter 60er Cap)."""
    blocks = []
    # 5x5: ~1 km Gitterabstand, ~1 km Suchradius — gleiche Dichte wie München
    blocks.extend(_grid_1km_labels("MUC", "München", 48.137, 11.575, 5, 5))
    blocks.extend(_grid_1km_labels("NUE", "Nürnberg", 49.454, 11.077, 5, 5))
    blocks.extend(_grid_1km_labels("AUG", "Augsburg", 48.366, 10.894, 5, 5))
    blocks.extend(_grid_1km_labels("WUE", "Würzburg", 49.792, 9.932, 5, 5))
    blocks.extend(_grid_1km_labels("REG", "Regensburg", 49.013, 12.102, 5, 5))
    # Mittelgroß: 2x2
    blocks.extend(_grid_1km_labels("ING", "Ingolstadt", 48.763, 11.425, 2, 2))
    blocks.extend(_grid_1km_labels("FUE", "Fürth", 49.478, 10.989, 2, 2))
    blocks.extend(_grid_1km_labels("ROS", "Rosenheim", 47.856, 12.128, 2, 2))
    return [
        (label, name, lat, lon, DENSE_CITY_1KM_RADIUS_M)
        for label, name, lat, lon in blocks
    ]


def _baden_wuerttemberg_1km_dense_regions():
    """1 km Kreise in den größten BW-Ballungsräumen + kompakte 2x2-Gitter für Mittelstädte."""
    blocks = []
    blocks.extend(_grid_1km_labels("STR", "Stuttgart", 48.776, 9.183, 5, 5))
    blocks.extend(_grid_1km_labels("KA", "Karlsruhe", 49.007, 8.404, 5, 5))
    blocks.extend(_grid_1km_labels("MA", "Mannheim", 49.487, 8.466, 5, 5))
    blocks.extend(_grid_1km_labels("FR", "Freiburg", 47.999, 7.842, 5, 5))
    blocks.extend(_grid_1km_labels("HD", "Heidelberg", 49.409, 8.694, 5, 5))
    blocks.extend(_grid_1km_labels("HN", "Heilbronn", 49.143, 9.210, 2, 2))
    blocks.extend(_grid_1km_labels("PF", "Pforzheim", 48.892, 8.694, 2, 2))
    blocks.extend(_grid_1km_labels("REU", "Reutlingen", 48.491, 9.204, 2, 2))
    blocks.extend(_grid_1km_labels("UL", "Ulm", 48.401, 9.988, 2, 2))
    blocks.extend(_grid_1km_labels("KN", "Konstanz", 47.663, 9.175, 2, 2))
    blocks.extend(_grid_1km_labels("OFF", "Offenburg", 48.474, 7.949, 2, 2))
    blocks.extend(_grid_1km_labels("RV", "Ravensburg", 47.782, 9.611, 2, 2))
    return [
        (label, name, lat, lon, DENSE_CITY_1KM_RADIUS_M)
        for label, name, lat, lon in blocks
    ]


def _hessen_1km_dense_regions():
    """1 km Kreise in den größten hessischen Ballungsräumen + 2x2 für Mittelstädte."""
    blocks = []
    blocks.extend(_grid_1km_labels("FRA", "Frankfurt am Main", 50.110, 8.682, 5, 5))
    blocks.extend(_grid_1km_labels("WI", "Wiesbaden", 50.082, 8.241, 3, 3))
    blocks.extend(_grid_1km_labels("DA", "Darmstadt", 49.872, 8.651, 3, 3))
    blocks.extend(_grid_1km_labels("OF", "Offenbach", 50.095, 8.776, 2, 2))
    blocks.extend(_grid_1km_labels("KS", "Kassel", 51.312, 9.479, 2, 2))
    blocks.extend(_grid_1km_labels("FD", "Fulda", 50.555, 9.680, 2, 2))
    blocks.extend(_grid_1km_labels("GI", "Gießen", 50.585, 8.678, 2, 2))
    return [
        (label, name, lat, lon, DENSE_CITY_1KM_RADIUS_M)
        for label, name, lat, lon in blocks
    ]


def _regions_baden_wuerttemberg():
    global BW_LAST_BOOSTER_INFO
    booster_r = _baden_wuerttemberg_booster_radius_m()
    raw_boosters = BADEN_WUERTTEMBERG_CITY_BOOSTERS
    boosters = _filter_bw_boosters_against_plz(raw_boosters)
    BW_LAST_BOOSTER_INFO = (len(raw_boosters), len(boosters), booster_r)

    regions = [
        (label, name, lat, lon, DEFAULT_REGION_RADIUS_M)
        for label, name, lat, lon in BADEN_WUERTTEMBERG_PLZ_REGIONS
    ]
    regions.extend(
        (label, name, lat, lon, booster_r)
        for label, name, lat, lon in boosters
    )
    if _cfg().use_1km_grid:
        regions.extend(_baden_wuerttemberg_1km_dense_regions())
    return regions


def _regions_hessen():
    regions = [
        (label, name, lat, lon, DEFAULT_REGION_RADIUS_M)
        for label, name, lat, lon in HESSEN_PLZ_REGIONS
    ]
    regions.extend(
        (label, name, lat, lon, CITY_BOOSTER_RADIUS_M)
        for label, name, lat, lon in HESSEN_CITY_BOOSTERS
    )
    if _cfg().use_1km_grid:
        regions.extend(_hessen_1km_dense_regions())
    return regions


def _regions_bayern():
    regions = [
        (label, name, lat, lon, DEFAULT_REGION_RADIUS_M)
        for label, name, lat, lon in BAYERN_PLZ_REGIONS
    ]
    regions.extend(
        (label, name, lat, lon, CITY_BOOSTER_RADIUS_M)
        for label, name, lat, lon in BAYERN_CITY_BOOSTERS
    )
    if _cfg().use_1km_grid:
        regions.extend(_bayer_1km_dense_regions())
    return regions


def _normalize_region_preset(preset: str) -> str:
    p = preset.strip().lower().replace("_", "-")
    aliases = {"bw": "baden-wuerttemberg", "he": "hessen"}
    return aliases.get(p, p)


def get_plz_regions():
    """
    Liefert Liste von (label, name, lat, lon, radius_m).

    Preset aus CLI --region:
    - "bayern": PLZ + 12km-Booster + optionales 1km-Gitter (--no-1km-grid)
    - "baden-wuerttemberg" / "bw": PLZ 70–79 + Booster + optionales 1km-Gitter
    - "hessen" / "he": PLZ 34–36 & 60–65 + Booster + optionales 1km-Gitter
    - "test": nur ein Kreis München (günstig zum Testen)
    """
    global BW_LAST_BOOSTER_INFO
    BW_LAST_BOOSTER_INFO = None
    raw = _cfg().region_preset
    preset = _normalize_region_preset(raw)

    if preset == "test":
        return [("TEST-MUC", "München (Test)", 48.137, 11.576, DEFAULT_REGION_RADIUS_M)]

    if preset == "baden-wuerttemberg":
        return _regions_baden_wuerttemberg()

    if preset == "hessen":
        return _regions_hessen()

    if preset == "bayern":
        return _regions_bayern()

    print(f"⚠  Unbekanntes --region={raw!r}, nutze bayern.")
    return _regions_bayern()



# ============================================================
# GOOGLE PLACES API CLIENT
# ============================================================

class GooglePlacesClient:
    """Client für die Google Places API (New)."""
    
    BASE_URL = "https://places.googleapis.com/v1/places:searchText"
    
    def __init__(self, api_key):
        self.api_key = api_key
        self.session = requests.Session()
        self.request_count = 0
        self.total_cost_estimate = 0.0
    
    def text_search(self, query, lat, lon, radius_m, page_token=None):
        """
        Führt eine Text Search (New) Anfrage durch.
        Gibt (places_list, next_page_token) zurück.
        """
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": FIELD_MASK,
        }
        
        body = {
            "textQuery": query,
            "languageCode": "de",
            "regionCode": "DE",
            "locationBias": {
                "circle": {
                    "center": {"latitude": lat, "longitude": lon},
                    "radius": radius_m,
                }
            },
            "maxResultCount": 20,
        }
        
        if page_token:
            body["pageToken"] = page_token
        
        time.sleep(DELAY_BETWEEN_REQUESTS)
        
        try:
            resp = self.session.post(self.BASE_URL, headers=headers, json=body, timeout=30)
            self.request_count += 1
            
            # Kosten-Schätzung (Text Search Basic + Contact Fields)
            self.total_cost_estimate += 0.035  # ~$35/1000 requests
            
            if resp.status_code == 200:
                data = resp.json()
                places = data.get("places", [])
                next_token = data.get("nextPageToken")
                return places, next_token
            elif resp.status_code == 429:
                print("  ⚠ Rate limit erreicht, warte 60s...")
                time.sleep(60)
                return self.text_search(query, lat, lon, radius_m, page_token)
            else:
                print(f"  ✗ API Fehler {resp.status_code}: {resp.text[:200]}")
                return [], None
                
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Request Fehler: {e}")
            return [], None
    
    def search_all_pages(self, query, lat, lon, radius_m, max_pages=3):
        """Durchläuft alle Ergebnisseiten (max 60 Ergebnisse)."""
        all_places = []
        next_token = None
        
        for page in range(max_pages):
            places, next_token = self.text_search(query, lat, lon, radius_m, next_token)
            all_places.extend(places)
            
            if not next_token or not places:
                break
            
            # Google braucht kurz Zeit, bevor der pageToken gültig wird
            time.sleep(2)
        
        return all_places


# ============================================================
# DATENVERARBEITUNG
# ============================================================

def extract_place_data(place, region_label, search_query):
    """Extrahiert relevante Felder aus einem Place-Objekt."""
    return {
        "place_id": place.get("id", ""),
        "name": place.get("displayName", {}).get("text", ""),
        "address": place.get("formattedAddress", ""),
        "latitude": place.get("location", {}).get("latitude", ""),
        "longitude": place.get("location", {}).get("longitude", ""),
        "website": place.get("websiteUri", ""),
        "phone": place.get("nationalPhoneNumber", ""),
        "rating": place.get("rating", ""),
        "review_count": place.get("userRatingCount", ""),
        "types": ", ".join(place.get("types", [])),
        "google_maps_url": place.get("googleMapsUri", ""),
        "region_plz": region_label,
        "found_by_query": search_query,
    }


def extract_domain(website_url):
    """Extrahiert die Domain aus einer URL."""
    if not website_url:
        return ""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(website_url)
        domain = parsed.netloc or parsed.path
        domain = domain.replace("www.", "")
        return domain.split("/")[0]
    except Exception:
        return website_url


def deduplicate_places(places_list):
    """Entfernt Duplikate basierend auf place_id."""
    seen = set()
    unique = []
    for place in places_list:
        pid = place["place_id"]
        if pid and pid not in seen:
            seen.add(pid)
            unique.append(place)
    return unique


# ============================================================
# FORTSCHRITTS-MANAGEMENT
# ============================================================

def load_progress():
    """Lädt den Fortschritt aus einer vorherigen Session."""
    path = _cfg().progress_file
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {"completed_regions": [], "places": []}


def save_progress(progress):
    """Speichert den aktuellen Fortschritt."""
    with open(_cfg().progress_file, "w") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


# ============================================================
# CSV EXPORT
# ============================================================

CSV_HEADERS = [
    "name",
    "address",
    "website",
    "domain",
    "phone",
    "email_guess",
    "rating",
    "review_count",
    "latitude",
    "longitude",
    "types",
    "google_maps_url",
    "place_id",
    "region_plz",
    "found_by_query",
]


def export_csv(places, filepath):
    """Exportiert die Ergebnisse als CSV."""
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, delimiter=";")
        writer.writeheader()
        
        for place in places:
            domain = extract_domain(place.get("website", ""))
            place["domain"] = domain
            # E-Mail-Schätzung: info@domain ist der häufigste Catch-All
            place["email_guess"] = f"info@{domain}" if domain else ""
            writer.writerow({k: place.get(k, "") for k in CSV_HEADERS})
    
    print(f"\n✅ CSV exportiert: {filepath}")
    print(f"   {len(places)} Locations gespeichert")


# ============================================================
# HAUPTPROGRAMM
# ============================================================

def main(argv: Optional[List[str]] = None) -> List[dict]:
    global _CFG
    args = _parse_args(argv)
    _CFG = _build_config(args)
    cfg = _CFG

    print("=" * 60)
    print("🏛  Google Maps Eventlocation Scraper")
    print("=" * 60)

    if not cfg.api_key:
        print("\n⚠  --api-key fehlt oder ist leer.")
        print("   Places API (New) unter https://console.cloud.google.com aktivieren,")
        print("   API-Key unter „Anmeldedaten“ anlegen, Abrechnung einrichten.")
        sys.exit(1)

    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    client = GooglePlacesClient(cfg.api_key)
    regions = get_plz_regions()
    
    # Fortschritt laden
    progress = load_progress()
    all_places = progress.get("places", [])
    completed = set(progress.get("completed_regions", []))
    
    remaining = [(l, n, la, lo, r) for l, n, la, lo, r in regions if l not in completed]
    
    print(f"\n📍 Preset: {cfg.region_preset} — {len(regions)} Suchgebiete gesamt")
    print(f"   {len(completed)} bereits abgeschlossen")
    print(f"   {len(remaining)} noch zu scrapen")
    print(f"🔍 {len(cfg.search_queries)} Suchbegriffe pro Gebiet")
    if BW_LAST_BOOSTER_INFO is not None:
        n_raw, n_kept, br_m = BW_LAST_BOOSTER_INFO
        print(
            f"   BW-Booster: {n_kept} aktiv (von {n_raw} Kandidaten), Radius {br_m / 1000:.0f} km, "
            f"PLZ-Redundanz ≤{_bw_plz_redundant_km():g} km"
        )
    if cfg.target_unique is not None:
        print(f"🎯 Stop bei {cfg.target_unique} eindeutigen Orten (--target-unique 0 zum Deaktivieren)")
    max_pages = 3
    est_calls_max = len(remaining) * len(cfg.search_queries) * max_pages
    print(f"📊 Obere Schranke API-Calls: ~{est_calls_max} (wenn jede Suche 3 Seiten füllt)")
    print(f"💰 Grobe Kosten-Schätzung (Max): ~${est_calls_max * 0.035:.2f}")
    print()
    
    # Scraping starten
    try:
        for i, (plz, name, lat, lon, radius) in enumerate(remaining, 1):
            print(f"[{i}/{len(remaining)}] PLZ {plz} – {name}")
            region_places = []
            
            for query in cfg.search_queries:
                print(f"  🔍 '{query}'...", end=" ", flush=True)
                places = client.search_all_pages(query, lat, lon, radius)
                
                extracted = [extract_place_data(p, plz, query) for p in places]
                region_places.extend(extracted)
                print(f"{len(places)} gefunden")
            
            # Regionale Deduplizierung
            before = len(region_places)
            region_places = deduplicate_places(region_places)
            print(f"  📋 {len(region_places)} unique (von {before} total)")
            
            # Ergebnisse sammeln (global deduplizieren für Zielgröße)
            all_places.extend(region_places)
            all_places = deduplicate_places(all_places)
            completed.add(plz)

            if cfg.target_unique is not None and len(all_places) >= cfg.target_unique:
                all_places = all_places[: cfg.target_unique]
                progress["completed_regions"] = list(completed)
                progress["places"] = all_places
                save_progress(progress)
                print(
                    f"  🎯 Ziel {cfg.target_unique} eindeutige Orte erreicht — "
                    f"Stop (API-Calls bisher: {client.request_count})"
                )
                break

            # Fortschritt speichern
            progress["completed_regions"] = list(completed)
            progress["places"] = all_places
            save_progress(progress)

            print(f"  💾 Fortschritt gespeichert ({client.request_count} API-Calls bisher)")
            print(f"  📈 Eindeutige Orte gesamt: {len(all_places)}")
            print()
    
    except KeyboardInterrupt:
        print("\n\n⚠  Abgebrochen! Fortschritt wurde gespeichert.")
        print("   Erneut starten mit denselben CLI-Optionen (insb. --region), um fortzufahren.")
    
    # Globale Deduplizierung (nach Ziel-Stop ggf. schon gekürzt)
    before_dedup = len(all_places)
    all_places = deduplicate_places(all_places)
    if cfg.target_unique is not None and len(all_places) > cfg.target_unique:
        all_places = all_places[: cfg.target_unique]
    
    # Statistiken
    with_website = sum(1 for p in all_places if p.get("website"))
    with_phone = sum(1 for p in all_places if p.get("phone"))
    
    print("=" * 60)
    print("📊 ERGEBNIS")
    print("=" * 60)
    print(f"   Locations gefunden:  {before_dedup}")
    print(f"   Nach Deduplizierung: {len(all_places)}")
    print(f"   Mit Website:         {with_website} ({with_website/max(len(all_places),1)*100:.0f}%)")
    print(f"   Mit Telefon:         {with_phone} ({with_phone/max(len(all_places),1)*100:.0f}%)")
    print(f"   API-Calls gesamt:    {client.request_count}")
    print(f"   Geschätzte Kosten:   ~${client.total_cost_estimate:.2f}")
    print()
    
    # CSV Export
    if all_places:
        export_csv(all_places, cfg.output_csv)

    # Fortschrittsdatei aufräumen wenn fertig
    if len(completed) == len(regions):
        cfg.progress_file.unlink(missing_ok=True)
        print("🎉 Alle Regionen abgeschlossen!")

    return all_places


if __name__ == "__main__":
    main()