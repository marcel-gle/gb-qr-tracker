"""
Quellen-Register — welche Apify-Scraper es gibt und wie man ihre Ausgabe
in eine Aussage übersetzt.

Idee: Statt fester Collector wählt jede Zielgruppen-Config aus diesem Register
die passenden Quellen aus. Neue Quelle = hier EIN Eintrag, dann in jeder
Config nutzbar. So deckst du für jede Zielgruppe genau das Sinnvolle ab.

Jeder Eintrag beschreibt:
- actor:     Apify-Actor-ID (vor erstem Lauf Input-Schema in der Console prüfen!)
- source:    Label, unter dem die Aussagen im Store landen
- build_input(cfg_block): baut das Actor-Input aus dem Config-Block
- parse(item): zieht (text, context, engagement, origin) aus einem Ergebnis
- ehrlichkeit: grobe Einordnung (hoch/mittel/niedrig) — nur zur Orientierung
- typisch_fuer: Stichworte, wann diese Quelle sinnvoll ist
"""

def _first(item, *keys, default=None):
    for k in keys:
        v = item.get(k)
        if v not in (None, ""):
            return v
    return default


REGISTRY = {

    # ---- Bewertungen: sehr dicht an echtem Schmerz & Einwänden ----
    "google_maps_reviews": {
        "actor": "compass/google-maps-reviews-scraper",
        "source": "gmaps",
        "ehrlichkeit": "hoch",
        "typisch_fuer": "lokale Dienstleister, Gastro, Handwerk, Agenturen — "
                        "Kunden schreiben, was sie nervt (Einwand-Material)",
        "build_input": lambda b: {
            "searchStringsArray": b.get("queries", []),
            "maxReviews": b.get("max_reviews", 100),
            "language": b.get("language", "de"),
            "reviewsSort": "newest",
        },
        "parse": lambda it: (
            _first(it, "text", "reviewText", default=""),
            _first(it, "title", "placeName"),
            int(_first(it, "likesCount", "stars", default=0) or 0),
            "google-maps",
        ),
    },

    "trustpilot_reviews": {
        "actor": "nikita-sviridenko/trustpilot-reviews-scraper",
        "source": "trustpilot",
        "ehrlichkeit": "hoch",
        "typisch_fuer": "SaaS, Online-Shops, Dienstleister mit Trustpilot-Profil",
        "build_input": lambda b: {
            "companyDomain": b.get("domains", []),
            "count": b.get("max_reviews", 100),
        },
        "parse": lambda it: (
            _first(it, "text", "reviewBody", default=""),
            _first(it, "title", "reviewHeadline"),
            int(_first(it, "likes", default=0) or 0),
            "trustpilot",
        ),
    },

    "amazon_reviews": {
        "actor": "junglee/amazon-reviews-scraper",
        "source": "amazon",
        "ehrlichkeit": "hoch",
        "typisch_fuer": "B2C-Produkte — was Käufer lieben/hassen, wörtlich",
        "build_input": lambda b: {
            "productUrls": [{"url": u} for u in b.get("urls", [])],
            "maxReviews": b.get("max_reviews", 100),
        },
        "parse": lambda it: (
            _first(it, "reviewDescription", "text", default=""),
            _first(it, "reviewTitle"),
            int(_first(it, "helpfulCount", "ratingScore", default=0) or 0),
            "amazon",
        ),
    },

    # ---- Arbeitgeber-Bewertungen: Gold für B2B-Zielgruppen ----
    "kununu": {
        "actor": "apify/website-content-crawler",
        "source": "kununu",
        "ehrlichkeit": "hoch",
        "typisch_fuer": "B2B — was Mitarbeiter/Insider einer Branche wirklich denken",
        "build_input": lambda b: {
            "startUrls": [{"url": u} for u in b.get("urls", [])],
            "maxCrawlPages": b.get("max_pages", 40),
            "crawlerType": "cheerio",
        },
        "parse": lambda it: (
            (it.get("text") or ""),
            (it.get("metadata") or {}).get("title") if isinstance(it.get("metadata"), dict) else None,
            0, "kununu",
        ),
    },

    # ---- Social: Trends, Ästhetik, B2C-Sprache ----
    "tiktok": {
        "actor": "clockworks/tiktok-scraper",
        "source": "tiktok",
        "ehrlichkeit": "mittel",
        "typisch_fuer": "B2C, jüngere Zielgruppen, Beauty/Fitness/Lifestyle-Trends",
        "build_input": lambda b: {
            "searchQueries": b.get("keywords", []),
            "resultsPerPage": b.get("max_items", 50),
            "shouldDownloadVideos": False,
        },
        "parse": lambda it: (
            _first(it, "text", "desc", default=""),
            None,
            int(_first(it, "diggCount", "playCount", default=0) or 0),
            "tiktok",
        ),
    },

    "instagram": {
        "actor": "apify/instagram-scraper",
        "source": "instagram",
        "ehrlichkeit": "mittel",
        "typisch_fuer": "B2C, visuelle Marken, Lifestyle — Hashtag-Recherche",
        "build_input": lambda b: {
            "search": b.get("keywords", [""])[0] if b.get("keywords") else "",
            "resultsLimit": b.get("max_items", 50),
        },
        "parse": lambda it: (
            _first(it, "caption", "text", default=""),
            None,
            int(_first(it, "likesCount", default=0) or 0),
            "instagram",
        ),
    },

    "youtube_comments": {
        "actor": "streamers/youtube-comments-scraper",
        "source": "youtube",
        "ehrlichkeit": "hoch",
        "typisch_fuer": "B2C & B2B — Kommentare unter How-to/Problem-Videos sind ehrlich",
        "build_input": lambda b: {
            "startUrls": [{"url": u} for u in b.get("urls", [])],
            "maxComments": b.get("max_items", 100),
        },
        "parse": lambda it: (
            _first(it, "text", "comment", default=""),
            None,
            int(_first(it, "likes", "voteCount", default=0) or 0),
            "youtube",
        ),
    },
}


def describe():
    """Übersicht aller Quellen — für die Quellen-Empfehlung."""
    out = []
    for key, r in REGISTRY.items():
        out.append(f"- {key} [{r['ehrlichkeit']}]: {r['typisch_fuer']}")
    return "\n".join(out)


if __name__ == "__main__":
    print("Verfügbare Quellen im Register:\n")
    print(describe())
