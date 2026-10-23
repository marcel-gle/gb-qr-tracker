"""
Schritt 2: Filtern + Lead-Scoring.

Liest die lokale SQLite-DB (aus Schritt 1), wendet die harten Filter an,
berechnet für jede Anlage einen Lead-Score (0-100) und schreibt eine
priorisierte CSV.

Aufruf:  python step2_score.py
Läuft offline in Sekunden.
"""
import os
import sqlite3
import pandas as pd

import config as C


# ---------------------------------------------------------------------------
# DB-Zugriff
# ---------------------------------------------------------------------------
def db_pfad() -> str:
    """Standard-Pfad der open-mastr SQLite-DB."""
    return os.path.expanduser(
        "~/.open-MaStR/data/sqlite/open-mastr.db"
    )


def lade_rohdaten() -> pd.DataFrame:
    """
    Holt Solar-Einheiten inkl. Betreibername per SQL-Join.
    Filtert bereits in SQL grob vor (Status, Leistung, Land), damit wir
    nicht die komplette Solar-Tabelle (~4-5 Mio Zeilen) in den RAM ziehen.
    """
    pfad = db_pfad()
    if not os.path.exists(pfad):
        raise FileNotFoundError(
            f"DB nicht gefunden unter {pfad}. "
            f"Zuerst 'python step1_download.py' ausführen."
        )

    con = sqlite3.connect(pfad)

    query = f"""
        SELECT
            s.EinheitMastrNummer      AS mastr_nr,
            s.Bruttoleistung          AS kwp,
            s.Inbetriebnahmedatum     AS ibn,
            s.Lage                    AS lage,
            s.AnzahlModule            AS module,
            s.SpeicherAmGleichenOrt   AS speicher_flag,
            s.GemeinsamerWechselrichterMitSpeicher AS speicher_wr,
            s.Strasse                 AS strasse,
            s.Hausnummer              AS hausnummer,
            s.Postleitzahl            AS plz,
            s.Ort                     AS ort,
            s.Bundesland              AS bundesland,
            s.Landkreis               AS landkreis,
            s.Laengengrad             AS lon,
            s.Breitengrad             AS lat,
            s.AnlagenbetreiberMastrNummer AS betreiber_nr,
            b.Name                    AS betreiber_name,
            b.Einheitart              AS einheitart
        FROM solar_extended s
        LEFT JOIN basic_units b
               ON s.EinheitMastrNummer = b.EinheitMastrNummer
        WHERE s.EinheitBetriebsstatus = 'In Betrieb'
          AND s.Bruttoleistung >= {C.MIN_KWP}
          AND s.Bruttoleistung <= {C.MAX_KWP}
          AND s.Land = 'Deutschland'
    """
    df = pd.read_sql_query(query, con)
    con.close()
    return df


# ---------------------------------------------------------------------------
# Filter
# ---------------------------------------------------------------------------
def wende_filter_an(df: pd.DataFrame) -> pd.DataFrame:
    """Harte fachliche Filter, die über SQL hinausgehen."""
    df = df.copy()

    # Inbetriebnahmejahr ableiten
    df["ibn"] = pd.to_datetime(df["ibn"], errors="coerce")
    df["ibn_jahr"] = df["ibn"].dt.year
    df = df[df["ibn_jahr"].between(C.IBN_JAHR_MIN, C.IBN_JAHR_MAX)]

    # Nur Anlagen auf/an Gebäuden (schließt Freiflächenparks aus).
    # MaStR-Werte für Lage: "Bauliche Anlagen (Hausdach, Gebäude und Fassade)"
    # bzw. "Freifläche". Wir matchen tolerant per Substring.
    df = df[
        df["lage"].fillna("").str.contains("Bauliche", case=False)
    ]

    # Betreiber muss eine Organisation sein → Name ist dann öffentlich.
    # Natürliche Personen erscheinen als "Natürliche Person" / leer.
    df = df[
        df["betreiber_name"].notna()
        & (df["betreiber_name"].str.strip() != "")
        & (~df["betreiber_name"].str.contains(
            "Natürliche Person", case=False, na=False))
    ]

    # Adresse muss brauchbar sein (bei >30 kWp i.d.R. vorhanden).
    df = df[df["plz"].notna() & df["ort"].notna()]

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Scoring-Komponenten (jeweils 0..1, werden dann mit Gewichten skaliert)
# ---------------------------------------------------------------------------
def score_eeg_ablauf(ibn_jahr: int) -> tuple[float, int]:
    """
    Näher am / im Förderende = dringender.
    Gibt (score 0..1, ablaufjahr) zurück.
    """
    ablauf = int(ibn_jahr) + C.EEG_FOERDERDAUER_JAHRE
    jahre_bis = ablauf - C.AKTUELLES_JAHR

    if jahre_bis <= 0:
        s = 1.0                    # Förderung bereits ausgelaufen — heißester Lead
    elif jahre_bis == 1:
        s = 0.95
    elif jahre_bis == 2:
        s = 0.85
    elif jahre_bis <= 4:
        s = 0.65
    elif jahre_bis <= 6:
        s = 0.45
    else:
        s = 0.25
    return s, ablauf


def score_groesse(kwp: float) -> float:
    """Sweet Spot voll, außerhalb linear abfallend."""
    if C.GROESSE_SWEET_MIN <= kwp <= C.GROESSE_SWEET_MAX:
        return 1.0
    if kwp < C.GROESSE_SWEET_MIN:
        # 30..50 kWp → 0.5..1.0
        return 0.5 + 0.5 * (kwp - C.MIN_KWP) / (C.GROESSE_SWEET_MIN - C.MIN_KWP)
    # 300..750 kWp → 1.0..0.4
    frac = (kwp - C.GROESSE_SWEET_MAX) / (C.MAX_KWP - C.GROESSE_SWEET_MAX)
    return max(0.4, 1.0 - 0.6 * frac)


def score_kein_speicher(speicher_flag, speicher_wr) -> tuple[float, bool]:
    """
    Kein Speicher registriert = voller offener Bedarf.
    MaStR: SpeicherAmGleichenOrt ist 'Ja'/'Nein'/None.
    """
    hat_speicher = (
        str(speicher_flag).strip().lower() in ("ja", "true", "1")
        or str(speicher_wr).strip().lower() in ("ja", "true", "1")
    )
    return (0.0 if hat_speicher else 1.0), hat_speicher


def score_branche(name: str) -> tuple[float, str]:
    """Keyword-Match auf Betreibernamen → Verbrauchsprofil-Proxy."""
    n = (name or "").lower()
    best = C.BRANCHE_DEFAULT
    treffer = ""
    for kw, val in C.BRANCHE_KEYWORDS.items():
        if kw in n and val > best:
            best, treffer = val, kw
    return best, treffer


def score_betreiber_typ(name: str) -> tuple[float, bool]:
    """Investment-/Verpachtungsvehikel abwerten (nicht die Standortfirma)."""
    n = (name or "").lower()
    ist_investor = any(m in n for m in C.INVESTOR_MUSTER)
    return (0.3 if ist_investor else 1.0), ist_investor


# ---------------------------------------------------------------------------
# Gesamt-Scoring
# ---------------------------------------------------------------------------
def berechne_scores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    rows = []
    for _, r in df.iterrows():
        s_eeg, ablaufjahr = score_eeg_ablauf(r["ibn_jahr"])
        s_groesse = score_groesse(r["kwp"])
        s_speicher, hat_speicher = score_kein_speicher(
            r["speicher_flag"], r["speicher_wr"])
        s_branche, branche_kw = score_branche(r["betreiber_name"])
        s_betreiber, ist_investor = score_betreiber_typ(r["betreiber_name"])

        gesamt = (
            s_eeg       * C.GEWICHTE["eeg_ablauf"]
            + s_groesse * C.GEWICHTE["groesse"]
            + s_speicher* C.GEWICHTE["kein_speicher"]
            + s_branche * C.GEWICHTE["branche"]
            + s_betreiber* C.GEWICHTE["betreiber_typ"]
        )

        rows.append({
            "lead_score": round(gesamt, 1),
            "eeg_ablaufjahr": ablaufjahr,
            "jahre_bis_ablauf": ablaufjahr - C.AKTUELLES_JAHR,
            "hat_speicher": "ja" if hat_speicher else "nein",
            "branche_hint": branche_kw,
            "investor_verdacht": "ja" if ist_investor else "nein",
        })

    scored = pd.concat([df, pd.DataFrame(rows, index=df.index)], axis=1)
    return scored.sort_values("lead_score", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------
def exportiere(df: pd.DataFrame) -> str:
    spalten = [
        "lead_score", "betreiber_name", "kwp", "module", "ibn_jahr",
        "eeg_ablaufjahr", "jahre_bis_ablauf", "hat_speicher",
        "branche_hint", "investor_verdacht",
        "strasse", "hausnummer", "plz", "ort", "landkreis", "bundesland",
        "lat", "lon", "mastr_nr", "betreiber_nr",
    ]
    spalten = [s for s in spalten if s in df.columns]
    out = df[spalten]
    out.to_csv(
        C.OUTPUT_CSV,
        sep=C.CSV_DELIMITER,
        encoding=C.CSV_ENCODING,
        index=False,
    )
    return os.path.abspath(C.OUTPUT_CSV)


def main():
    print("Lade Rohdaten aus SQLite …")
    df = lade_rohdaten()
    print(f"  {len(df):,} Solar-Einheiten > {C.MIN_KWP:.0f} kWp (In Betrieb)")

    df = wende_filter_an(df)
    print(f"  {len(df):,} nach fachlichen Filtern "
          f"(Baudach, Organisation, IBN {C.IBN_JAHR_MIN}-{C.IBN_JAHR_MAX})")

    df = berechne_scores(df)
    pfad = exportiere(df)

    print(f"\n✓ {len(df):,} Leads geschrieben → {pfad}")
    print("\nTop 10 nach Lead-Score:")
    show = df.head(10)[
        ["lead_score", "betreiber_name", "kwp", "ibn_jahr",
         "eeg_ablaufjahr", "hat_speicher", "plz", "ort"]
    ]
    print(show.to_string(index=False))


if __name__ == "__main__":
    main()
