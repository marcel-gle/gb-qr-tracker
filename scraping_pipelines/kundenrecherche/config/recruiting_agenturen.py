"""
Zielgruppen-Config: Recruiting-Agenturen (2-50 MA), DACH.

Fokus:
- Wie Recruiting-Agenturen Neukunden gewinnen
- Welche Probleme bei Akquise, Positionierung und Sales-Prozess auftreten
"""

SLUG = "recruiting_agenturen"

AUDIENCE = (
    "Recruiting-Agenturen und Personalberatungen im DACH-Raum (2-20 Mitarbeitende), "
    "die aktiv Neukunden (Unternehmen mit offenen Stellen) gewinnen wollen."
)

# Arbeitsthese wird erst in Pass 2 verwendet (Bias-Schutz).
THESIS = (
    "Das Kernproblem ist nicht nur zu wenig Outreach, sondern fehlende Differenzierung: "
    "Agenturen klingen austauschbar, verkaufen Prozesse statt Resultate und landen daher "
    "im Preisvergleich statt als strategischer Partner."
)

# --- APIFY ACTOR-IDs ---
# Vor erstem Lauf in Apify Input-Schema kurz gegenpruefen.
ACTORS = {
    "reddit": "spry_wholemeal/reddit-scraper",
    "forum": "apify/website-content-crawler",
    "linkedin": "apimaestro/linkedin-posts-search-scraper-no-cookies",
    "google": "apify/google-search-scraper",
}

# --- REDDIT ---
SUBREDDITS = [
    # DE / DACH
    "de", "selbststaendig", "Unternehmer", "kmu", "jobs",
    # international / Fachbezug Recruiting und Sales
    "recruiting", "recruitinghell", "sales", "Entrepreneur", "smallbusiness",
    "HumanResources", "startups",
]

ALLZWECK_SUBREDDITS = {"de", "selbststaendig", "Unternehmer", "kmu", "jobs"}

BRANCHEN_KEYWORDS = [
    "recruiting", "recruiter", "personalberatung", "headhunter", "talent acquisition",
    "kandidat", "candidate", "hiring", "vakanz", "besetzung", "active sourcing",
    "hr dienstleister", "personaldienstleister", "employer branding",
]

REDDIT_KEYWORDS = [
    # DE
    "Recruiting Neukunden gewinnen", "Personalberatung Akquise", "Kundengewinnung Recruiting",
    "Recruiting Agentur Preise", "Retainer Personalberatung", "Kaltakquise Recruiting",
    # EN
    "recruitment agency client acquisition", "how recruiting agencies get clients",
    "staffing agency sales", "recruitment business development", "retained search pricing",
    "contingency recruiting problems",
]

# --- FACHFOREN / WEB ---
FORUM_URLS = [
    "https://www.wiwi-treff.de/Forum/",
    "https://forum.personalwirtschaft.de/",
    "https://www.recruiting-treff.de/",
]

FORUM_KEYWORDS = [
    "Neukunden", "Akquise", "Kundengewinnung", "Lead", "Pitch",
    "Retainer", "Provision", "Preis", "Abschluss", "Einwand", "Kaltakquise",
    "Hiring Freeze", "Budgetstopp", "Auftragsflaute",
]

# --- LINKEDIN ---
LINKEDIN_KEYWORDS = [
    "Recruiting Agentur Neukunden",
    "Personalberatung Kundengewinnung",
    "Recruitment Agency Business Development",
    "Active Sourcing Agentur",
    "Retained Search Vertrieb",
]

# --- GOOGLE: BLOGS / FACHARTIKEL ---
# SERP findet URLs, danach crawlt der Forum-Actor die Seiten.
# GOOGLE_KEYWORDS filtert Absätze (Fallback: FORUM_KEYWORDS).
GOOGLE_QUERIES = [
    "Recruiting Agentur Neukunden gewinnen",
    "Personalberatung Kundengewinnung Probleme",
    "Personalberatung Kaltakquise",
    "Warum Recruiting Agenturen keine Kunden finden",
]
GOOGLE_KEYWORDS = [
    "Neukunden", "Akquise", "Kundengewinnung", "Lead", "Pipeline",
    "Retainer", "Provision", "Preis", "Einwand", "Kaltakquise",
    "Positionierung", "Differenzierung", "Hiring Freeze", "Budget",
    "client acquisition", "business development", "staffing", "outreach",
]

# --- SYNTHESE ---
SEARCH_TERMS = [
    "Neukunden", "Akquise", "Kundengewinnung", "Lead", "Pipeline",
    "Preis", "Retainer", "Provision", "Abschluss", "Einwand",
    "Kaltakquise", "Inbound", "Outbound", "Positionierung",
    "austauschbar", "Differenzierung", "Hiring Freeze", "Budget",
    "client acquisition", "agency sales", "staffing", "retained search",
]

# --- REGISTER-QUELLEN ---
SOURCES = {
    "google_maps_reviews": {
        "queries": [
            "Personalberatung München",
            "Recruiting Agentur Berlin",
            "Headhunter Hamburg",
            "Executive Search Frankfurt",
        ],
        "max_reviews": 100,
        "language": "de",
    },
    # Optional aktivieren, wenn konkrete Profil-URLs vorliegen:
    # "kununu": {"urls": ["https://www.kununu.com/de/<agentur>"]},
    # "trustpilot_reviews": {"domains": ["example.com"], "max_reviews": 100},
}
