#!/usr/bin/env python3
"""
app_pv_check.py — Gebäude für Gebäude durchklicken und auf dem aktuellen
Bayern-DOP20-Orthophoto (20 cm Auflösung) prüfen, ob PV auf dem Dach ist.

Datengrundlage: karte_daten.geojson aus prep_karte.py.
Orthophotos: WMS der Bayerischen Vermessungsverwaltung (OpenData, CC BY 4.0,
kein Token nötig). Bewertungen werden laufend in pv_bewertungen.csv gespeichert —
Abbrechen und später weitermachen ist jederzeit möglich.

Start:
  streamlit run app_pv_check.py
  streamlit run app_pv_check.py -- --daten karte_daten.geojson --bewertungen pv_bewertungen.csv
"""

from __future__ import annotations

import argparse
import io
import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from PIL import Image, ImageDraw

WMS_URL = "https://geoservices.bayern.de/od/wms/dop/v1/dop20"
WMS_LAYER = "by_dop20c"
ATTRIBUTION = "Datenquelle: Bayerische Vermessungsverwaltung – www.geodaten.bayern.de (CC BY 4.0)"

BEWERTUNGEN = ["PV vorhanden", "Kein PV", "Unsicher"]

# ---------------------------------------------------------------------------
# WGS84 -> UTM Zone 32N (EPSG:25832), Krüger-Reihen, ohne pyproj
# ---------------------------------------------------------------------------
_A = 6378137.0
_F = 1 / 298.257222101
_K0 = 0.9996
_E0 = 500000.0
_LON0 = math.radians(9.0)
_N_ = _F / (2 - _F)
_A_RECT = _A / (1 + _N_) * (1 + _N_**2 / 4 + _N_**4 / 64)
_E_ECC = math.sqrt(_F * (2 - _F))
_ALPHA = (
    _N_ / 2 - 2 * _N_**2 / 3 + 5 * _N_**3 / 16,
    13 * _N_**2 / 48 - 3 * _N_**3 / 5,
    61 * _N_**3 / 240,
)


def wgs84_to_utm32(lon: float, lat: float) -> tuple[float, float]:
    lam = math.radians(lon) - _LON0
    phi = math.radians(lat)
    t = math.sinh(math.atanh(math.sin(phi)) - _E_ECC * math.atanh(_E_ECC * math.sin(phi)))
    xi_p = math.atan2(t, math.cos(lam))
    eta_p = math.atanh(math.sin(lam) / math.sqrt(1 + t * t))
    xi, eta = xi_p, eta_p
    for j, a in enumerate(_ALPHA, start=1):
        xi += a * math.sin(2 * j * xi_p) * math.cosh(2 * j * eta_p)
        eta += a * math.cos(2 * j * xi_p) * math.sinh(2 * j * eta_p)
    return _E0 + _K0 * _A_RECT * eta, _K0 * _A_RECT * xi


# ---------------------------------------------------------------------------
# Daten laden
# ---------------------------------------------------------------------------

def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--daten", default="karte_daten.geojson")
    p.add_argument("--bewertungen", default="pv_bewertungen.csv")
    a, _ = p.parse_known_args()
    return a


@st.cache_data(show_spinner="Lade Gebäudedaten ...")
def lade_gebaeude(pfad: str) -> pd.DataFrame:
    raw = json.loads(Path(pfad).read_text(encoding="utf-8"))
    rows = []
    for f in raw["features"]:
        p = f["properties"]
        geom = f["geometry"]
        polys_ll = ([geom["coordinates"][0]] if geom["type"] == "Polygon"
                    else [poly[0] for poly in geom["coordinates"]])
        polys_utm = [[wgs84_to_utm32(lon, lat) for lon, lat in ring] for ring in polys_ll]
        alle = [pt for ring in polys_utm for pt in ring]
        cx = sum(p_[0] for p_ in alle) / len(alle)
        cy = sum(p_[1] for p_ in alle) / len(alle)
        rows.append({
            "gml_id": p.get("gml_id"),
            "funktion": p.get("funktion") or "",
            "dach_qm": p.get("dachflaeche_qm") or 0.0,
            "grund_qm": p.get("grundflaeche_qm") or 0.0,
            "hoehe_m": p.get("hoehe_m"),
            "adresse": p.get("adresse") or "—",
            "ags": p.get("ags") or "",
            "utm_polys": polys_utm,
            "e": cx, "n": cy,
        })
    df = pd.DataFrame(rows).sort_values("dach_qm", ascending=False).reset_index(drop=True)
    return df


def lade_bewertungen(pfad: str) -> dict[str, str]:
    if not Path(pfad).exists():
        return {}
    df = pd.read_csv(pfad, sep=";", dtype=str)
    return dict(zip(df["gml_id"], df["bewertung"]))


def speichere_bewertung(pfad: str, zeile: dict, bewertungen: dict[str, str]) -> None:
    bewertungen[zeile["gml_id"]] = zeile["bewertung"]
    neu = not Path(pfad).exists()
    # Vollständig neu schreiben, damit Änderungen an bestehenden IDs greifen
    alt: list[dict] = []
    if not neu:
        alt = pd.read_csv(pfad, sep=";", dtype=str).to_dict("records")
        alt = [r for r in alt if r["gml_id"] != zeile["gml_id"]]
    pd.DataFrame(alt + [zeile]).to_csv(pfad, sep=";", index=False, encoding="utf-8-sig")


# ---------------------------------------------------------------------------
# Orthophoto holen + Grundriss einzeichnen
# ---------------------------------------------------------------------------

@st.cache_data(show_spinner=False, max_entries=200)
def hole_dop(e: float, n: float, kante_m: float, px: int) -> bytes | None:
    half = kante_m / 2
    bbox = f"{e - half:.1f},{n - half:.1f},{e + half:.1f},{n + half:.1f}"
    params = {
        "SERVICE": "WMS", "VERSION": "1.1.1", "REQUEST": "GetMap",
        "LAYERS": WMS_LAYER, "STYLES": "", "SRS": "EPSG:25832",
        "BBOX": bbox, "WIDTH": px, "HEIGHT": px, "FORMAT": "image/jpeg",
    }
    try:
        r = requests.get(WMS_URL, params=params, timeout=30)
    except requests.RequestException:
        return None
    if r.status_code != 200 or not r.headers.get("Content-Type", "").startswith("image"):
        return None
    return r.content


def mit_umriss(img_bytes: bytes, polys_utm, e: float, n: float,
               kante_m: float, px: int) -> Image.Image:
    img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    draw = ImageDraw.Draw(img)
    half = kante_m / 2
    for ring in polys_utm:
        pts = [((x - (e - half)) / kante_m * px,
                ((n + half) - y) / kante_m * px) for x, y in ring]
        draw.line(pts + [pts[0]], fill=(255, 60, 60), width=3)
    return img


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

def main() -> None:
    st.set_page_config(page_title="PV-Check Gewerbedächer", layout="wide", page_icon="☀️")
    args = _args()

    if not Path(args.daten).exists():
        st.error(f"`{args.daten}` nicht gefunden — erst `python3 prep_karte.py` ausführen.")
        st.stop()

    df = lade_gebaeude(args.daten)
    if "bewertungen" not in st.session_state:
        st.session_state.bewertungen = lade_bewertungen(args.bewertungen)
    bew = st.session_state.bewertungen

    # ---------- Sidebar ----------
    st.sidebar.header("Auswahl")
    dmin, dmax = st.sidebar.slider(
        "Dachfläche (m²)", 0.0, float(min(df["dach_qm"].max(), 30000.0)),
        (500.0, 20000.0), step=50.0)
    nur_offen = st.sidebar.checkbox("Nur unbewertete anzeigen", value=False)
    kante = st.sidebar.select_slider(
        "Bildausschnitt (Kantenlänge)", options=[80, 120, 200, 300, 500], value=120)
    st.sidebar.caption(ATTRIBUTION)

    liste = df[(df["dach_qm"] >= dmin) & (df["dach_qm"] <= dmax)].reset_index(drop=True)
    if nur_offen:
        liste = liste[~liste["gml_id"].isin(bew)].reset_index(drop=True)
    if liste.empty:
        st.success("Keine (weiteren) Gebäude im Filter — alles bewertet 🎉")
        st.stop()

    # Index-Verwaltung
    if "idx" not in st.session_state:
        st.session_state.idx = 0
    st.session_state.idx = min(st.session_state.idx, len(liste) - 1)

    def springe(delta: int) -> None:
        st.session_state.idx = max(0, min(len(liste) - 1, st.session_state.idx + delta))

    g = liste.iloc[st.session_state.idx]

    # ---------- Kopf: Navigation ----------
    c_prev, c_pos, c_next = st.columns([1, 3, 1])
    c_prev.button("⬅️ Zurück", on_click=springe, args=(-1,),
                  disabled=st.session_state.idx == 0, width="stretch")
    c_next.button("Weiter ➡️", on_click=springe, args=(1,),
                  disabled=st.session_state.idx >= len(liste) - 1, width="stretch")
    bewertet_im_filter = int(liste["gml_id"].isin(bew).sum())
    c_pos.progress((st.session_state.idx + 1) / len(liste),
                   text=f"Gebäude {st.session_state.idx + 1} von {len(liste)} "
                        f"(sortiert nach Dachfläche) · {bewertet_im_filter} bewertet")

    ziel = c_pos.number_input("Springe zu Nr.", 1, len(liste),
                              st.session_state.idx + 1, label_visibility="collapsed")
    if ziel - 1 != st.session_state.idx:
        st.session_state.idx = ziel - 1
        st.rerun()

    # ---------- Hauptbereich ----------
    col_bild, col_info = st.columns([2, 1])

    with col_bild:
        img_bytes = hole_dop(g["e"], g["n"], float(kante), 900)
        if img_bytes:
            img = mit_umriss(img_bytes, g["utm_polys"], g["e"], g["n"], float(kante), 900)
            st.image(img, width="stretch",
                     caption=f"DOP20 · Ausschnitt {kante} m × {kante} m · roter Umriss = LoD2-Grundriss")
        else:
            st.warning("Orthophoto konnte nicht geladen werden (WMS nicht erreichbar?). "
                       "Nutze den BayernAtlas-Link rechts.")

    with col_info:
        status = bew.get(g["gml_id"])
        if status:
            st.info(f"Bereits bewertet: **{status}**")
        st.markdown(
            f"### {g['adresse']}\n"
            f"**{g['funktion']}**\n\n"
            f"| | |\n|---|---|\n"
            f"| Dachfläche | **{g['dach_qm']:,.0f} m²** |\n"
            f"| Grundfläche | {g['grund_qm']:,.0f} m² |\n"
            f"| Höhe | {g['hoehe_m'] or '—'} m |\n"
            f"| AGS | {g['ags']} |\n"
            f"| gml_id | `{g['gml_id']}` |".replace(",", "."))

        e_i, n_i = int(g["e"]), int(g["n"])
        st.link_button("Im BayernAtlas öffnen ↗",
                       f"https://atlas.bayern.de/?c={e_i},{n_i}&z=16&r=0&l=luftbild",
                       width="stretch")

        st.divider()
        st.markdown("**Bewertung** (springt automatisch weiter)")

        def bewerte(wert: str) -> None:
            speichere_bewertung(args.bewertungen, {
                "gml_id": g["gml_id"], "bewertung": wert, "adresse": g["adresse"],
                "funktion": g["funktion"], "dach_qm": g["dach_qm"],
                "e": round(g["e"], 1), "n": round(g["n"], 1),
                "zeit": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }, bew)
            springe(1)

        b1, b2, b3 = st.columns(3)
        b1.button("☀️ PV", on_click=bewerte, args=("PV vorhanden",),
                  width="stretch", type="primary")
        b2.button("❌ Kein PV", on_click=bewerte, args=("Kein PV",), width="stretch")
        b3.button("❓ Unsicher", on_click=bewerte, args=("Unsicher",), width="stretch")

        if bew:
            st.divider()
            zaehl = pd.Series(list(bew.values())).value_counts()
            st.caption("Gesamt: " + " · ".join(f"{k}: {v}" for k, v in zaehl.items()))
            if Path(args.bewertungen).exists():
                st.download_button(
                    "Bewertungen herunterladen (CSV)",
                    Path(args.bewertungen).read_bytes(),
                    file_name="pv_bewertungen.csv", mime="text/csv",
                    width="stretch")


if __name__ == "__main__":
    import sys
    import streamlit as _st
    if not _st.runtime.exists():
        print("Bitte mit Streamlit starten:\n  streamlit run app_pv_check.py")
        sys.exit(1)
    main()
else:
    main()