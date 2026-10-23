"""
config/coaching.py — Beispiel-Zielgruppe: ShineUp-Coaching-Kundschaft.

So liest du diese Datei:
- Jede GROSSGESCHRIEBENE Variable ist ein "Regler". Die Namen sind fest —
  analyze.py und run.py suchen nach genau diesen Namen.
- Strings stehen in "Anführungszeichen", Listen in [eckigen Klammern],
  zwischen Einträgen steht ein Komma. Das ist die ganze Syntax.
- Was mit # beginnt oder zwischen dreifachen Anführungszeichen steht,
  ist Kommentar und wird ignoriert.
"""

# ── ANALYSE-REGLER ──────────────────────────────────────────────

# Kurzname ohne Leerzeichen — landet im Report-Dateinamen (analyse_coaching.md)
SLUG = "coaching"

# Wer ist die Zielgruppe? Prosa. Claude sieht das in BEIDEN Pässen.
# Je konkreter, desto schärfer die Analyse.
AUDIENCE = ("Menschen im DACH-Raum, die ein Coaching zur beruflichen "
            "Neuausrichtung suchen: angestellt, Mitte 30 bis 50, unzufrieden "
            "im Job, aber unsicher, ob und wie ein Wechsel machbar ist.")

# Deine Arbeitsthese — sieht NUR Pass 2 (Bias-Schutz, dazu unten mehr).
# Wenn du KEINE These hast: auf None setzen (siehe Erklärung im Chat).
THESIS = ("Das eigentliche Hindernis ist nicht fehlende Fähigkeit, sondern "
          "die Angst vor dem finanziellen Risiko und dem Urteil des Umfelds.")

# Womit zieht der Synthesiser die relevanten Aussagen aus dem Store?
# Dank Präfix-Suche findet "Angst" auch Ängste/ängstlich.
# NICHT dasselbe wie die Sammel-Keywords weiter unten!
SEARCH_TERMS = [
    "Job", "Wechsel", "Angst", "Risiko", "Gehalt", "kündigen",
    "unzufrieden", "Sinn", "Neuanfang", "Umschulung", "traue mich nicht",
    "career change", "quit job", "fear", "burnout",
]

# ── SAMMEL-REGLER ───────────────────────────────────────────────

# Apify-Actor-IDs. Vor dem ersten bezahlten Lauf in der Apify-Console prüfen.
ACTORS = {
    "reddit": "trudax/reddit-scraper",
    "forum": "apify/website-content-crawler",
    "linkedin": None,   # bewusst leer bis du einen Actor ausgewählt hast
}

# Reddit: WO wird gesucht ...
SUBREDDITS = [
    "cscareerquestions", "careerguidance", "jobs",
    "antiwork", "findapath", "ExperiencedDevs",
]
# ... und WONACH. (Das füllt den Store — nicht zu verwechseln mit SEARCH_TERMS.)
REDDIT_KEYWORDS = [
    "career change fear", "quit stable job", "afraid to leave job",
    "career change 40", "hate my job but scared",
]

# Deutsche Foren: konkrete Bereiche, wo über Jobwechsel geredet wird.
# URLs vor dem Lauf im Browser prüfen.
FORUM_URLS = [
    "https://www.gutefrage.net/tag/berufswechsel",
    "https://www.wiwi-treff.de/Forum/",
]
FORUM_KEYWORDS = [
    "Jobwechsel", "kündigen", "unzufrieden", "Angst", "Risiko",
    "Neuanfang", "Umschulung", "traue mich nicht", "Gehalt",
]

# LinkedIn (DE)
LINKEDIN_KEYWORDS = [
    "berufliche Neuorientierung", "Jobwechsel Angst",
    "Mut zur Veränderung Karriere",
]
