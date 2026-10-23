#!/usr/bin/env python3
"""
Pflegeheime aus OpenStreetMap (Overpass API) für SH, BE, BB, SL abfragen.

Aufruf:   python open_street_map_overpass.py
Ausgabe:  pflegeheime_osm.csv  (Quelle: © OpenStreetMap-Mitwirkende, ODbL)
Benötigt: pip install requests
"""
import csv
import sys
import threading
import time
import requests

# Bundesland -> Gebietsauswahl. Berlin zusätzlich über die feste Relations-ID,
# weil die Suche über ISO-Code/admin_level dort nicht zuverlässig greift.
BUNDESLAENDER = {
    "Schleswig-Holstein": 'area["ISO3166-2"="DE-SH"]["boundary"="administrative"]',
    "Berlin": "area(id:3600062422)",
    "Brandenburg": 'area["ISO3166-2"="DE-BB"]["boundary"="administrative"]',
    "Saarland": 'area["ISO3166-2"="DE-SL"]["boundary"="administrative"]',
}
MIN_PLAETZE = 80
ENDPOINTS = [
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
]
TAG_FILTER = ['["social_facility"="nursing_home"]', '["amenity"="nursing_home"]']
REQUEST_TIMEOUT = 150

# Zielgruppen, die nicht passen (Behindertenhilfe, Psychiatrie, Kinder usw.)
AUSSCHLUSS_FOR = {"disabled", "mental_health", "child", "juvenile", "orphan", "drug_addicted", "homeless"}
# Keine vollstationäre Pflege
AUSSCHLUSS_TYP = {"day_care", "assisted_living", "ambulatory_care", "group_home", "outreach"}
AUSSCHLUSS_NAME = ("tagespflege", "betreutes wohnen", "hospiz", "eingliederungshilfe", "intensivpflege", "wohngruppe")

OUTPUT = "pflegeheime_osm.csv"


# ---------- Fortschrittsanzeige ----------

class Ticker:
    """Zeigt während einer laufenden Anfrage die verstrichene Zeit an."""

    def __init__(self, label):
        self.label, self.stop_evt, self.start = label, threading.Event(), time.time()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        spinner = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        i = 0
        while not self.stop_evt.is_set():
            sec = int(time.time() - self.start)
            bar_len = 20
            filled = min(bar_len, int(bar_len * sec / REQUEST_TIMEOUT))
            bar = "█" * filled + "░" * (bar_len - filled)
            sys.stdout.write(f"\r    {spinner[i % len(spinner)]} {self.label} [{bar}] {sec:>3}s / max {REQUEST_TIMEOUT}s ")
            sys.stdout.flush()
            i += 1
            time.sleep(0.2)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop_evt.set()
        self.thread.join()
        sys.stdout.write("\r" + " " * 90 + "\r")
        sys.stdout.flush()


def fmt(sec):
    sec = int(sec)
    return f"{sec // 60}:{sec % 60:02d} min"


# ---------- Overpass ----------

def query(area, tag):
    return f"""
[out:json][timeout:120];
{area}->.a;
nwr{tag}(area.a);
out center tags;
"""


def fetch_one(area, tag):
    for runde in range(3):
        for url in ENDPOINTS:
            host = url.split("/")[2]
            try:
                with Ticker(host):
                    r = requests.post(url, data={"data": query(area, tag)}, timeout=REQUEST_TIMEOUT,
                                      headers={"User-Agent": "lead-research-script/1.0"})
                r.raise_for_status()
                data = r.json()
                # Overpass meldet Timeouts/Speicherfehler oft mit HTTP 200 + "remark"
                if "remark" in data and ("error" in data["remark"].lower() or "timed out" in data["remark"].lower()):
                    raise RuntimeError(f"Serverfehler: {data['remark'][:60]}")
                els = data.get("elements", [])
                print(f"    ✓ {tag}: {len(els)} Treffer über {host}")
                return els
            except Exception as e:
                print(f"    ✗ {host}: {str(e)[:70]}")
        warte = 60 * (runde + 1)
        print(f"    Alle Server belegt, warte {warte}s …")
        time.sleep(warte)
    raise RuntimeError(f"Keine Antwort für {area} {tag}")


# ---------- Auswertung ----------

def plaetze(tags):
    raw = tags.get("capacity") or tags.get("beds") or ""
    digits = "".join(ch for ch in raw.split(";")[0] if ch.isdigit())
    return int(digits) if digits else None


def passt(tags):
    if tags.get("social_facility") in AUSSCHLUSS_TYP:
        return False
    fuer = set(tags.get("social_facility:for", "").split(";"))
    if fuer & AUSSCHLUSS_FOR and "senior" not in fuer:
        return False
    name = tags.get("name", "").lower()
    return not any(w in name for w in AUSSCHLUSS_NAME)


def main():
    rows, seen = [], set()
    total = len(BUNDESLAENDER) * len(TAG_FILTER)
    step, t0 = 0, time.time()

    for land, area in BUNDESLAENDER.items():
        land_count = 0
        for tag in TAG_FILTER:
            step += 1
            elapsed = time.time() - t0
            eta = f" | Rest ca. {fmt(elapsed / (step - 1) * (total - step + 1))}" if step > 1 else ""
            done = int(30 * (step - 1) / total)
            print(f"\n[{'█' * done}{'░' * (30 - done)}] Abfrage {step}/{total}: {land} {tag} | {fmt(elapsed)}{eta}")

            for el in fetch_one(area, tag):
                key = (el["type"], el["id"])
                tags = el.get("tags", {})
                if key in seen or not passt(tags):
                    continue
                seen.add(key)
                land_count += 1
                p = plaetze(tags)
                rows.append({
                    "Bundesland": land,
                    "Name": tags.get("name", ""),
                    "Betreiber": tags.get("operator", ""),
                    "Straße": f'{tags.get("addr:street", "")} {tags.get("addr:housenumber", "")}'.strip(),
                    "PLZ": tags.get("addr:postcode", ""),
                    "Ort": tags.get("addr:city", ""),
                    "Telefon": tags.get("phone") or tags.get("contact:phone", ""),
                    "E-Mail": tags.get("email") or tags.get("contact:email", ""),
                    "Website": tags.get("website") or tags.get("contact:website", ""),
                    "Zielgruppe": tags.get("social_facility:for", ""),
                    "Plätze (OSM)": p if p is not None else "",
                    "Größe": "ab 80" if p and p >= MIN_PLAETZE else ("unter 80" if p else "unbekannt"),
                    "Lat": el.get("lat") or el.get("center", {}).get("lat"),
                    "Lon": el.get("lon") or el.get("center", {}).get("lon"),
                    "OSM-Link": f"https://www.openstreetmap.org/{el['type']}/{el['id']}",
                })
            time.sleep(5)  # Server schonen

        if land_count == 0:
            print(f"    ⚠ {land}: 0 Einrichtungen – Gebietsauswahl prüfen oder später erneut versuchen")

    print(f"\n[{'█' * 30}] fertig in {fmt(time.time() - t0)}")

    if not rows:
        print("Keine Einrichtungen gefunden.")
        return

    rows.sort(key=lambda r: (r["Bundesland"], r["Größe"] != "ab 80", r["Name"]))
    with open(OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys(), delimiter=";")
        w.writeheader()
        w.writerows(rows)

    print(f"\n{len(rows)} Einrichtungen gespeichert in {OUTPUT}")
    for land in BUNDESLAENDER:
        sub = [r for r in rows if r["Bundesland"] == land]
        print(f"  {land}: {len(sub)} gesamt | ab 80: {sum(r['Größe'] == 'ab 80' for r in sub)} "
              f"| ohne Platzzahl: {sum(r['Größe'] == 'unbekannt' for r in sub)} "
              f"| mit Website: {sum(bool(r['Website']) for r in sub)}")


if __name__ == "__main__":
    main()