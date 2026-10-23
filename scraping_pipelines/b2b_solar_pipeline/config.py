"""
Zentrale Konfiguration für die PV-Upgrade-Lead-Pipeline.
Alle fachlichen Annahmen und Scoring-Gewichte an einem Ort — hier tunen.
"""
from datetime import date

# ---------------------------------------------------------------------------
# HARTE FILTER (bestimmen, wer überhaupt in die Liste kommt)
# ---------------------------------------------------------------------------

# Nur Anlagen > 30 kWp haben öffentliche Adresse + Koordinaten (MaStR-Verordnung).
MIN_KWP = 30.0

# Obergrenze: darüber sind es i.d.R. Investoren-/Freiflächenparks, nicht der
# klassische Mittelständler mit eigenem Dach und eigenem Stromverbrauch.
MAX_KWP = 750.0

# Inbetriebnahme-Fenster. Alte Hochvergütungs-Anlagen (Volleinspeisung),
# deren EEG-Förderung in absehbarer Zeit ausläuft.
IBN_JAHR_MIN = 2006
IBN_JAHR_MAX = 2014

# EEG-Förderdauer: Inbetriebnahmejahr + 20 volle Kalenderjahre.
EEG_FOERDERDAUER_JAHRE = 20

HEUTE = date.today()
AKTUELLES_JAHR = HEUTE.year

# ---------------------------------------------------------------------------
# SCORING-GEWICHTE (Summe der max. Beiträge = 100)
# ---------------------------------------------------------------------------
GEWICHTE = {
    "eeg_ablauf": 35,      # Dringlichkeit durch Förderende — stärkster Treiber
    "groesse": 25,         # Projektvolumen / Sweet Spot
    "kein_speicher": 20,   # noch kein Speicher registriert = offener Bedarf
    "branche": 15,         # Verbrauchsprofil passt zu Speicher
    "betreiber_typ": 5,    # echte Standortfirma vs. Investmentvehikel
}

# Sweet Spot für Anlagengröße (kWp) — hier max. Größen-Score.
GROESSE_SWEET_MIN = 50.0
GROESSE_SWEET_MAX = 300.0

# Branchen-Keywords → Score-Multiplikator (0..1). Hoher Tagesstromverbrauch =
# bester Speicher-/Eigenverbrauchs-Fit.
BRANCHE_KEYWORDS = {
    # sehr hoher, gut planbarer Tagesverbrauch
    "kühl": 1.0, "kälte": 1.0, "lebensmittel": 1.0, "molkerei": 1.0,
    "bäckerei": 1.0, "fleisch": 1.0, "metzger": 1.0,
    "galvanik": 1.0, "kunststoff": 1.0, "spritzguss": 1.0,
    # produzierendes Gewerbe / Maschinenbau
    "maschinen": 0.9, "produktion": 0.9, "fertigung": 0.9, "werk": 0.8,
    "metall": 0.9, "schreinerei": 0.8, "tischlerei": 0.8, "säge": 0.8,
    "druckerei": 0.9, "wäscherei": 1.0, "reinigung": 0.7,
    # Logistik / Lager (tagsüber Betrieb, oft Kühlung)
    "logistik": 0.8, "spedition": 0.7, "lager": 0.7,
    # Handel mit Tagesbetrieb
    "autohaus": 0.8, "kfz": 0.7, "getränke": 0.9, "supermarkt": 1.0,
    "hotel": 0.8, "gastro": 0.7, "hof": 0.6, "agrar": 0.6, "landwirt": 0.6,
}
BRANCHE_DEFAULT = 0.4  # unbekannte Branche: moderater Grundwert

# Betreiber-Namensmuster, die auf ein Investment-/Verpachtungsvehikel
# hindeuten (nicht die Firma im Gebäude → schlechterer Lead).
INVESTOR_MUSTER = [
    "solar gmbh & co", "solarpark", "solar & co", "photovoltaik gmbh & co",
    "pv gmbh & co", "sonnen", "energie & co", "bürgerenergie",
    "solarfonds", "beteiligungs", "vermögensverwaltung", "grundstücks",
]

# ---------------------------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------------------------
OUTPUT_CSV = "pv_upgrade_leads.csv"
CSV_DELIMITER = ";"      # Excel-DE-freundlich
CSV_ENCODING = "utf-8-sig"  # UTF-8-BOM, damit Umlaute in Excel stimmen
