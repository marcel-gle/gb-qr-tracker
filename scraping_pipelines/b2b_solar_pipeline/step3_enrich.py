"""
Schritt 3 (optional): Anreicherung der Top-Leads.

Das MaStR liefert Firmenname + Adresse + Koordinaten, aber keine
Ansprechpartner / Kontaktdaten. Diese Stufe reichert die höchstbewerteten
Leads über eine externe Quelle an (North Data, Handelsregister o.ä.) —
genau die Pipeline, die du aus deinen bestehenden Lead-Gen-Workflows kennst.

Hier bewusst als Stub mit klarer Schnittstelle: trag deinen North-Data-
API-Key ein bzw. hänge deinen bestehenden Enrichment-Call an.

Aufruf:  python step3_enrich.py --top 200
"""
import argparse
import pandas as pd

import config as C


def enrich_placeholder(row: pd.Series) -> dict:
    """
    Platzhalter. Ersetze den Inhalt durch deinen North-Data- /
    Handelsregister-Lookup. Input ist eine Lead-Zeile, Rückgabe die
    zusätzlichen Felder.
    """
    return {
        "geschaeftsfuehrer": "",
        "hrb_nummer": "",
        "email": "",
        "telefon": "",
        "website": "",
        "wz_code": "",
        "mitarbeiter": "",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=200,
                    help="Nur die N höchstbewerteten Leads anreichern")
    ap.add_argument("--infile", default=C.OUTPUT_CSV)
    ap.add_argument("--outfile", default="pv_upgrade_leads_enriched.csv")
    args = ap.parse_args()

    df = pd.read_csv(args.infile, sep=C.CSV_DELIMITER, encoding=C.CSV_ENCODING)
    top = df.head(args.top).copy()
    print(f"Reichere {len(top)} Top-Leads an …")

    enrich_rows = [enrich_placeholder(r) for _, r in top.iterrows()]
    top = pd.concat(
        [top.reset_index(drop=True), pd.DataFrame(enrich_rows)], axis=1)

    top.to_csv(args.outfile, sep=C.CSV_DELIMITER,
               encoding=C.CSV_ENCODING, index=False)
    print(f"✓ Geschrieben → {args.outfile}")
    print("  Hinweis: enrich_placeholder() noch mit deinem "
          "North-Data-Call ersetzen.")


if __name__ == "__main__":
    main()
