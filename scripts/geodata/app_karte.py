#!/usr/bin/env python3
"""
app_karte.py — Interaktive Karte der LoD2-Gewerbegebäude (Nürnberg + Umland).

Vorher einmalig die Daten aufbereiten:
  python3 prep_karte.py --daten lod2_daten --out karte_daten.geojson

Dann starten:
  streamlit run app_karte.py
  streamlit run app_karte.py -- --daten /pfad/zu/karte_daten.geojson

Benötigt: streamlit (bringt pydeck mit). Basiskarte: Carto/OpenStreetMap,
kein Mapbox-Token nötig.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st

AGS_NAMEN = {
    "09564000": "Nürnberg",
    "09563000": "Fürth (Stadt)",
    "09562000": "Erlangen",
    "09565000": "Schwabach",
    "09574": "Lkr. Nürnberger Land",
    "09573": "Lkr. Fürth",
    "09572": "Lkr. Erlangen-Höchstadt",
    "09576": "Lkr. Roth",
}

# Farbschema nach Funktionsgruppe (R, G, B)
FARBEN = {
    "Lager/Logistik": [230, 126, 34],     # 214x, 2150
    "Produktion/Industrie": [192, 57, 43],  # 21xx, 2200
    "Handel": [41, 128, 185],             # 2010, 205x
    "Büro/Verwaltung": [39, 174, 96],     # 2020-2040
    "Hotel/Gastro": [142, 68, 173],       # 207x, 208x
    "Sonstiges Gewerbe": [127, 140, 141],
    "Mischnutzung": [241, 196, 15],       # 1xxx
}


def gruppe(code: str) -> str:
    if code.startswith("1"):
        return "Mischnutzung"
    if code in ("2140", "2141", "2143", "2150"):
        return "Lager/Logistik"
    if code.startswith("21") or code in ("2200", "2210"):
        return "Produktion/Industrie"
    if code in ("2010", "2050", "2051", "2052", "2053", "2054", "2055", "2056"):
        return "Handel"
    if code in ("2020", "2030", "2040"):
        return "Büro/Verwaltung"
    if code.startswith("207") or code.startswith("208"):
        return "Hotel/Gastro"
    return "Sonstiges Gewerbe"


def _daten_pfad() -> str:
    p = argparse.ArgumentParser()
    p.add_argument("--daten", default="karte_daten.geojson")
    args, _ = p.parse_known_args()
    return args.daten


@st.cache_data(show_spinner="Lade Gebäudedaten ...")
def lade_daten(pfad: str):
    raw = json.loads(Path(pfad).read_text(encoding="utf-8"))
    feats = raw["features"]
    rows = []
    for f in feats:
        p = f["properties"]
        geom = f["geometry"]
        # Für pydeck PolygonLayer: Liste der Exterior-Ringe
        if geom["type"] == "Polygon":
            polys = [geom["coordinates"][0]]
        else:  # MultiPolygon
            polys = [poly[0] for poly in geom["coordinates"]]
        # Schwerpunkt für Zoom/Export
        pts = polys[0]
        lon = sum(pt[0] for pt in pts) / len(pts)
        lat = sum(pt[1] for pt in pts) / len(pts)
        rows.append({
            "gml_id": p.get("gml_id"),
            "funktion_code": p.get("funktion_code") or "",
            "funktion": p.get("funktion") or "",
            "gruppe": gruppe(p.get("funktion_code") or ""),
            "dach_qm": p.get("dachflaeche_qm") or 0.0,
            "grund_qm": p.get("grundflaeche_qm") or 0.0,
            "hoehe_m": p.get("hoehe_m") or 3.0,
            "adresse": p.get("adresse") or "—",
            "ags": p.get("ags") or "",
            "polygon": polys[0],   # größter/erster Ring fürs Rendering
            "lon": lon,
            "lat": lat,
        })
    return pd.DataFrame(rows)


def main() -> None:
    st.set_page_config(page_title="LoD2 Gewerbegebäude", layout="wide", page_icon="🏭")
    pfad = _daten_pfad()

    if not Path(pfad).exists():
        st.error(f"Datei `{pfad}` nicht gefunden. Erst `python3 prep_karte.py` ausführen "
                 f"oder Pfad angeben: `streamlit run app_karte.py -- --daten /pfad/zur.geojson`")
        st.stop()

    df = lade_daten(pfad)

    # ---------------- Sidebar: Filter ----------------
    st.sidebar.header("Filter")

    dach_max_daten = float(df["dach_qm"].max())
    slider_max = min(dach_max_daten, 30000.0)
    dach_min, dach_max = st.sidebar.slider(
        "Dachfläche (m²)",
        min_value=0.0, max_value=slider_max,
        value=(500.0, min(20000.0, slider_max)),
        step=50.0,
    )
    unbegrenzt = st.sidebar.checkbox(
        f"Obergrenze ignorieren (auch > {slider_max:,.0f} m²)", value=False)

    gruppen = st.sidebar.multiselect(
        "Funktionsgruppen", options=list(FARBEN.keys()),
        default=[g for g in FARBEN if g != "Mischnutzung"])

    ags_opts = sorted(df["ags"].unique())
    ags_sel = st.sidebar.multiselect(
        "Gebiet", options=ags_opts, default=ags_opts,
        format_func=lambda a: AGS_NAMEN.get(a, a) if a else "(ohne Schlüssel)")

    d3 = st.sidebar.toggle("3D-Ansicht (Gebäudehöhen)", value=True)
    st.sidebar.caption(f"Datenquelle: `{pfad}` · {len(df):,} Gewerbegebäude gesamt".replace(",", "."))

    # ---------------- Filterung ----------------
    m = (df["dach_qm"] >= dach_min) & df["gruppe"].isin(gruppen) & df["ags"].isin(ags_sel)
    if not unbegrenzt:
        m &= df["dach_qm"] <= dach_max
    sel = df[m].copy()

    # ---------------- Kopfzeile / KPIs ----------------
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Gebäude im Filter", f"{len(sel):,}".replace(",", "."))
    c2.metric("Summe Dachfläche", f"{sel['dach_qm'].sum() / 1e6:,.2f} km²".replace(",", "."))
    c3.metric("Ø Dachfläche", f"{sel['dach_qm'].mean():,.0f} m²".replace(",", ".") if len(sel) else "—")
    c4.metric("Median", f"{sel['dach_qm'].median():,.0f} m²".replace(",", ".") if len(sel) else "—")

    if sel.empty:
        st.warning("Keine Gebäude im gewählten Filter.")
        st.stop()

    # ---------------- Karte ----------------
    sel["farbe"] = sel["gruppe"].map(FARBEN)
    sel["elev"] = sel["hoehe_m"].clip(lower=3.0)

    layer = pdk.Layer(
        "PolygonLayer",
        data=sel[["polygon", "farbe", "elev", "funktion", "adresse",
                  "dach_qm", "grund_qm", "hoehe_m", "gml_id"]],
        get_polygon="polygon",
        get_fill_color="farbe",
        get_line_color=[60, 60, 60],
        line_width_min_pixels=1,
        extruded=d3,
        get_elevation="elev",
        pickable=True,
        auto_highlight=True,
        opacity=0.55,
    )
    view = pdk.ViewState(
        latitude=float(sel["lat"].mean()),
        longitude=float(sel["lon"].mean()),
        zoom=11,
        pitch=45 if d3 else 0,
    )
    tooltip = {
        "html": "<b>{funktion}</b><br/>{adresse}<br/>"
                "Dach: {dach_qm} m² · Grund: {grund_qm} m² · Höhe: {hoehe_m} m"
                "<br/><span style='color:#999'>{gml_id}</span>",
        "style": {"backgroundColor": "#1e1e1e", "color": "white", "fontSize": "12px"},
    }
    st.pydeck_chart(
        pdk.Deck(layers=[layer], initial_view_state=view, tooltip=tooltip,
                 map_provider="carto", map_style="light"),
        height=650,
    )

    # Legende
    leg = "  ".join(
        f"<span style='background:rgb({r},{g},{b});padding:1px 8px;border-radius:3px;"
        f"color:white;font-size:12px'>{name}</span>"
        for name, (r, g, b) in FARBEN.items() if name in gruppen)
    st.markdown(leg, unsafe_allow_html=True)

    # ---------------- Tabelle + Export ----------------
    with st.expander(f"Tabelle ({len(sel):,} Gebäude)".replace(",", ".")):
        st.dataframe(
            sel[["adresse", "funktion", "dach_qm", "grund_qm", "hoehe_m", "ags", "gml_id"]]
            .sort_values("dach_qm", ascending=False),
            width="stretch", height=400)

    buf = io.StringIO()
    sel.drop(columns=["polygon", "farbe", "elev"]).to_csv(
        buf, sep=";", index=False, encoding="utf-8")
    st.download_button(
        "Gefilterte Auswahl als CSV",
        buf.getvalue().encode("utf-8-sig"),
        file_name=f"gewerbe_{dach_min:.0f}-{dach_max:.0f}qm.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    if not st.runtime.exists():
        print("Bitte mit Streamlit starten:\n  streamlit run app_karte.py")
        sys.exit(1)
    main()
else:
    main()