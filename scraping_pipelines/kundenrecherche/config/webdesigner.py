"""
Zielgruppen-Config: Webdesigner & Agenturen (2-10 MA), DACH.

ALLES Zielgruppen-Spezifische lebt hier: Zielgruppe, These, Quellen,
Suchbegriffe, Actor-IDs. Für eine neue Zielgruppe: Datei kopieren,
anpassen, mit `python run.py collect --config <name>` nutzen.
"""

SLUG = "webdesigner"

AUDIENCE = ("Webdesigner und Agenturen mit 2-10 Mitarbeitern im DACH-Raum, "
            "die bereits Leads bekommen und erfolgreich verkaufen. "
            "Thema: Neukundengewinnung.")

# Die Arbeitsthese wird der Analyse erst in PASS 2 gezeigt (Bias-Schutz).
THESIS = ("Das eigentliche Problem ist eine Preissenkungs-Abwärtsspirale "
          "(Preise senken, um Aufträge zu gewinnen) kombiniert mit kaltem "
          "Erstkontakt: Der Erstkontakt ist kalt und undifferenziert, dadurch "
          "startet jede Verhandlung aus einer schwachen Position.")

# --- APIFY ACTOR-IDs ---
# Vor dem ersten Lauf: Input-Schema in der Apify-Console gegenprüfen!
# Actors ändern ihre Schemas; die Collector parsen defensiv, aber das
# INPUT-Schema muss stimmen.
ACTORS = {
    "reddit": "spry_wholemeal/reddit-scraper",  # KOSTENLOS, kein Rental, kein Key. Alt (Miete!): trudax/reddit-scraper
    "forum": "apify/website-content-crawler",   # offizieller Apify-Actor
    # Input-Schema geprüft 07/2026: Feld heißt 'keyword' (Einzahl), Menge
    # 'total_posts'. Preis 5 $ / 1.000 Ergebnisse. Kein Login/Cookie nötig.
    # Hier reden deutsche Agenturinhaber tatsächlich öffentlich — die einzige
    # zugängliche Quelle für diese Zielgruppe (Foren sind tot, Facebook und
    # Slack geschlossen).
    "linkedin": "apimaestro/linkedin-posts-search-scraper-no-cookies",
}

# --- REDDIT ---
# Zwei Ebenen: DEUTSCHE Subs/Keywords für echte DACH-Sprache (dünn besiedelt,
# aber die Original-Formulierungen sind Copy-Gold) + ENGLISCHE für Volumen und
# internationale Problemstruktur (die Schmerzpunkte sind grenzüberschreitend
# ähnlich). Der Store markiert jede Aussage mit Sprache, die Analyse trennt.
SUBREDDITS = [
    # deutschsprachig (DACH) — hier liegt die Copy-Sprache
    "de", "Finanzen", "selbststaendig", "Unternehmer", "Freelance_de",
    "de_EDV", "Ratschlag", "kmu", "Gruendung", "Handwerker",
    # international (Volumen)
    "webdev", "web_design", "freelance", "Entrepreneur",
    "agency", "digital_marketing", "smallbusiness",
]
# HINWEIS: Reddit blockt die öffentliche JSON-API von hier aus (403), die
# Existenz der neuen Subs ließ sich also nicht vorab prüfen. Nicht existierende
# Subs lässt der Actor einfach aus (wie bei r/Freelance_de im ersten Lauf) —
# das kostet nichts. Nach dem Lauf im Log prüfen, welche geliefert haben.

# WELCHE SUBS SIND THEMATISCH, WELCHE NICHT?
# Bei r/webdev ist das Subreddit selbst die Vorauswahl — alles dort hat
# Branchenbezug, ungefiltert übernehmen ist richtig. r/de und r/Finanzen sind
# Allzweck-Foren: dort geht es um Steuern, Politik und Gastronomie. Messung am
# ersten Lauf: nur 2,0 % der deutschen Aussagen hatten Branchenbezug, gegenüber
# 9,7 % der englischen aus den thematischen Subs. Deshalb brauchen die
# Allzweck-Subs einen Themenfilter, sonst füllt Rauschen den Store.
ALLZWECK_SUBREDDITS = {
    "de", "Finanzen", "selbststaendig", "Unternehmer", "Ratschlag",
    "Gruendung", "Handwerker", "kmu", "de_EDV",
}
BRANCHEN_KEYWORDS = [
    "webdesign", "web design", "website", "webseite", "homepage", "wordpress",
    "seo", "agentur", "grafik", "designer", "frontend", "webentwickl",
    "onlineshop", "online-shop", "landingpage", "webflow", "typo3", "shopify",
    "screendesign", "ux", "ui", "relaunch", "cms", "wix", "jimdo",
]
REDDIT_KEYWORDS = [
    # deutsch — für die echte DACH-Sprache
    "Neukunden gewinnen", "Kunden akquise", "Preise Webdesign",
    "unter Wert verkaufen", "Preiskampf Agentur", "Kaltakquise",
    "Stundensatz Freelancer",
    # englisch — für Volumen und Problemstruktur
    "client acquisition", "underpricing", "race to the bottom",
    "web design pricing", "getting clients",
]

# --- FACHFOREN ---
# Konkrete Such-/Bereichs-URLs VOR dem Lauf im Browser prüfen (Foren ändern
# Strukturen). Trage hier die Bereiche ein, wo über Akquise/Preise geredet wird.
# ALLE URLS EINZELN GEPRÜFT (Juli 2026). Befund vorweg: Die klassischen
# deutschen Webmaster-Foren sind größtenteils tot. Verworfen wurden:
#   psd-tutorials.de/forum  — lebt, aber KEIN Unterforum zu Preisen/Akquise
#   gutefrage.net/tag/...   — 403 + Laien-Q&A ohne Agenturbezug
#   webmasterpro.de/forum   — HTTP 410 Gone, abgeschaltet
#   ayom.com                — Domain existiert nicht mehr (DNS)
#   SELFHTML-Forum          — rein technisch (HTML/CSS/PHP), kein Geschäft
#   freelancermap / GULP    — Jobbörsen und Studien, kein Diskussionsforum
#
# 1) ABAKUS — das größte deutschsprachige SEO-/Online-Marketing-Forum und die
#    beste Fundstelle: Publikum sind Agenturen, Freelancer und Webmaster, die
#    über Preise und Kundenarbeit streiten ("Online Marketing Angebot ->
#    Welcher Preis?", "SEO Freelancer Einkommen"). Es gibt keinen eigenen
#    Business-Bereich, die Diskussionen stehen im allgemeinen Forum f-1.
_ABAKUS = "https://forum.abakus-internet-marketing.de/"
FORUM_URLS = [
    _ABAKUS + "viewforum/f-1.html",    # "Ich hab' da mal 'ne Frage" (34.410 Beiträge)
    _ABAKUS + "viewforum/f-75.html",   # Marktplatz: Dienstleistungen (aktiv 06/2026)
    _ABAKUS + "viewforum/f-59.html",   # Internet-Recht & -Gesetz
    # Threads, die die Suche als hochrelevant ausgewiesen hat
    _ABAKUS + "viewtopic/t-134903.html",   # Online Marketing Angebot -> Welcher Preis?
    _ABAKUS + "viewtopic/t-143382.html",   # SEO Freelancer Einkommen
    _ABAKUS + "viewtopic/t-129056.html",   # Stundensatz für Adwords-Einrichtung
    _ABAKUS + "viewtopic/t-138217.html",   # Kostenschätzung Adwords-Kampagne

    # 2) dasauge — DACH-Kreativbranche. Schwerpunkt ist Grafik-/Kommunikations-
    #    design, NICHT Webdesign; die Honorar- und Akquise-Diskussionen sind
    #    trotzdem dieselbe Debatte (Freelancer vs. Agenturpreise, Kunden, die
    #    drücken). Deshalb nur über die Forensuche mit Geschäftsbegriffen
    #    angesteuert, nie über die Indizes — deren Seite 1 ist SEO-Spam.
    "https://dasauge.de/forum/s6?begriff=stundensatz",
    "https://dasauge.de/forum/s6?begriff=honorar",
    "https://dasauge.de/forum/s6?begriff=akquise",
    "https://dasauge.de/forum/s6?begriff=neukunden",
    "https://dasauge.de/forum/tipps_tricks/e15203",   # Stundensatz Freelancer
    "https://dasauge.de/forum/designforum/e12924",    # Konditionen Freiberufler

    # 3) homepage-forum.de — UNGEPRÜFT. Blockt direkte Abrufe mit 403, könnte
    #    über Apifys Proxies erreichbar sein. Nach dem Lauf im Dataset schauen,
    #    ob überhaupt Seiten dieser Domain drin sind; wenn nicht, hier streichen.
    "https://www.homepage-forum.de/",
]
FORUM_KEYWORDS = [
    "Neukunden", "Akquise", "Kunden gewinnen", "Preis", "zu teuer",
    "Rabatt", "Angebot", "Auftrag", "Konkurrenz", "Erstgespräch",
    "Kaltakquise", "Stundensatz",
]

# --- LINKEDIN (DE) ---
LINKEDIN_KEYWORDS = [
    "Webdesign Neukunden", "Agentur Akquise", "Kundengewinnung Agentur",
    "Webdesigner Preise", "Neukundengewinnung Webdesign",
]

# --- SYNTHESE ---
# Steuert, welche Aussagen aus dem Store in die Analyse wandern.
# Dank Präfix-Matching findet "Preis" auch Preise/Preisen/Preiskampf.
SEARCH_TERMS = [
    "Neukunden", "Kunden", "Preis", "teuer", "Rabatt", "Kaltakquise",
    "Erstgespräch", "Angebot", "Auftrag", "Konkurrenz", "Stundensatz",
    "client", "pricing", "cold outreach", "underpriced", "lowball",
    "leads", "agency",
]

# --- REGISTER-QUELLEN (Bewertungen, Social) ---
# Pro Zielgruppe an-/abwählen. Nur eintragen, was für DIESE Zielgruppe Sinn
# ergibt. Schlüssel = Register-Key (siehe collectors/registry.py).
# Für Webdesigner sinnvoll:
#   - google_maps_reviews: Kunden von Agenturen schreiben, was sie nervt
#     -> Einwand-Material aus Kundensicht (die andere Seite des Tisches)
#   - kununu: Insider-Sicht auf Agentur-Alltag
# NICHT sinnvoll hier: tiktok/instagram/amazon (B2C-Konsumgüter) -> weggelassen.
SOURCES = {
    "google_maps_reviews": {
        "queries": [
            "Webdesign Agentur München", "Webdesign Agentur Berlin",
            "Internetagentur Hamburg",
        ],
        "max_reviews": 80,
        "language": "de",
    },
    # kununu erst aktivieren, wenn du konkrete Profil-URLs hast:
    # "kununu": {"urls": ["https://www.kununu.com/de/<agentur>"]},
}
