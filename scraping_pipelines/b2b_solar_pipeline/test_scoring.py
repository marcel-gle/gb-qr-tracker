"""Testet Filter + Scoring mit synthetischen Daten (ohne echten Download)."""
import pandas as pd
import config as C
import step2_score as S


def make_synthetic():
    return pd.DataFrame([
        # Idealer Lead: 2007er Metzgerei, 120 kWp, kein Speicher, Baudach
        dict(mastr_nr="SEE1", kwp=120.0, ibn="2007-05-01",
             lage="Bauliche Anlagen (Hausdach, Gebäude und Fassade)",
             module=550, speicher_flag="Nein", speicher_wr=None,
             strasse="Hauptstr.", hausnummer="5", plz="50667", ort="Köln",
             bundesland="Nordrhein-Westfalen", landkreis="Köln",
             lon=6.9, lat=50.9, betreiber_nr="ABR1",
             betreiber_name="Metzgerei Schmitz GmbH", einheitart="Solar"),
        # Investor-Vehikel, 2009, hat schon Speicher → soll runtergescored werden
        dict(mastr_nr="SEE2", kwp=400.0, ibn="2009-08-01",
             lage="Bauliche Anlagen (Hausdach, Gebäude und Fassade)",
             module=1800, speicher_flag="Ja", speicher_wr=None,
             strasse="Feldweg", hausnummer="1", plz="80331", ort="München",
             bundesland="Bayern", landkreis="München",
             lon=11.5, lat=48.1, betreiber_nr="ABR2",
             betreiber_name="Sonnen Solarpark GmbH & Co. KG", einheitart="Solar"),
        # Freifläche → soll rausgefiltert werden
        dict(mastr_nr="SEE3", kwp=200.0, ibn="2010-01-01",
             lage="Freifläche", module=900, speicher_flag="Nein", speicher_wr=None,
             strasse=None, hausnummer=None, plz="04109", ort="Leipzig",
             bundesland="Sachsen", landkreis="Leipzig",
             lon=12.3, lat=51.3, betreiber_nr="ABR3",
             betreiber_name="Acker Solar GmbH", einheitart="Solar"),
        # Natürliche Person → rausgefiltert (anonymisiert)
        dict(mastr_nr="SEE4", kwp=45.0, ibn="2008-03-01",
             lage="Bauliche Anlagen (Hausdach, Gebäude und Fassade)",
             module=200, speicher_flag="Nein", speicher_wr=None,
             strasse=None, hausnummer=None, plz="70173", ort="Stuttgart",
             bundesland="Baden-Württemberg", landkreis="Stuttgart",
             lon=9.1, lat=48.7, betreiber_nr="ABR4",
             betreiber_name="Natürliche Person", einheitart="Solar"),
        # Zu neu (2019) → rausgefiltert durch IBN-Fenster
        dict(mastr_nr="SEE5", kwp=90.0, ibn="2019-06-01",
             lage="Bauliche Anlagen (Hausdach, Gebäude und Fassade)",
             module=350, speicher_flag="Nein", speicher_wr=None,
             strasse="Ringstr.", hausnummer="9", plz="20095", ort="Hamburg",
             bundesland="Hamburg", landkreis="Hamburg",
             lon=10.0, lat=53.5, betreiber_nr="ABR5",
             betreiber_name="Logistik Nord GmbH", einheitart="Solar"),
        # Solider Lead: 2011 Maschinenbau, 60 kWp, kein Speicher
        dict(mastr_nr="SEE6", kwp=60.0, ibn="2011-11-01",
             lage="Bauliche Anlagen (Hausdach, Gebäude und Fassade)",
             module=270, speicher_flag=None, speicher_wr="Nein",
             strasse="Industriestr.", hausnummer="12", plz="42897", ort="Remscheid",
             bundesland="Nordrhein-Westfalen", landkreis="Remscheid",
             lon=7.2, lat=51.2, betreiber_nr="ABR6",
             betreiber_name="Weber Maschinenbau GmbH", einheitart="Solar"),
    ])


def main():
    df = make_synthetic()
    print(f"Synthetisch: {len(df)} Zeilen")
    df = S.wende_filter_an(df)
    print(f"Nach Filter: {len(df)} Zeilen (erwartet: 3 → SEE1, SEE2, SEE6)")
    df = S.berechne_scores(df)
    cols = ["lead_score", "betreiber_name", "kwp", "ibn_jahr",
            "eeg_ablaufjahr", "jahre_bis_ablauf", "hat_speicher",
            "branche_hint", "investor_verdacht"]
    print("\n" + df[cols].to_string(index=False))
    assert set(df["mastr_nr"]) == {"SEE1", "SEE2", "SEE6"}, "Filter falsch!"
    assert df.iloc[0]["mastr_nr"] == "SEE1", "SEE1 sollte Top-Lead sein!"
    print("\n✓ Alle Assertions bestanden.")


if __name__ == "__main__":
    main()
