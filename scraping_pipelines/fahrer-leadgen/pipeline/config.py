"""
Central configuration for the Fahrer-Leadgen pipeline.
Edit keyword lists and thresholds here rather than in the stage code.
"""
from __future__ import annotations

import os

# --- Stage 1: sourcing ------------------------------------------------------
# WZ 2008 codes that map to "Logistik und Transport"
TARGET_WZ_CODES = {
    "49.41": "Güterbeförderung im Straßenverkehr (road freight – bullseye)",
    "52.29": "Erbringung sonstiger Dienstleistungen für den Verkehr (Spedition)",
    "53.20": "Sonstige Post-, Kurier- und Expressdienste",
    "52.10": "Lagerei",
}

# No hard employee-count filter -- North Data's figure is unreliable, so we
# don't gate stage 1 on it. employee_estimate (used only for the scoring
# bonus below) is instead derived in stage 3 from an explicit headcount
# mention on the company's own website, when there is one.
EMPLOYEE_SWEETSPOT = (35, 65)  # scoring bonus band, not a hard filter

# --- Stage 2: hiring signal -------------------------------------------------
# Job titles / terms that indicate "sucht Fahrer".
DRIVER_KEYWORDS = [
    "kraftfahrer", "berufskraftfahrer", "lkw-fahrer", "lkw fahrer",
    "lkwfahrer", "auslieferungsfahrer", "fahrer c", "fahrer ce",
    "c/ce", "ce-führerschein", "klasse ce", "nahverkehr", "fernverkehr",
    "kraftfahrer/in", "berufskraftfahrer/in", "fahrmischerfahrer",
]

# Career-page URL fragments to probe on a company website.
CAREER_PATH_HINTS = [
    "karriere", "jobs", "stellenangebote", "stellen", "stellenanzeigen",
    "job", "career", "careers", "team-werden", "mitarbeiter-werden",
    "offene-stellen", "wir-suchen", "arbeiten-bei",
]

# Bundesagentur Jobsuche (free, no registration; header key below).
BA_JOBS_URL = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/jobs"
BA_DETAILS_URL = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/jobdetails"
BA_API_KEY = "jobboerse-jobsuche"  # public client id used as X-API-Key
BA_DAYS_LOOKBACK = 60  # veroeffentlichtseit

# Google Jobs (optional, needs a paid key). Google has no public API for the
# "Google for Jobs" search carousel; scraping Google's result HTML directly
# violates their ToS and gets blocked fast, so this goes through SerpApi
# (https://serpapi.com/google-jobs-api), a compliant third-party wrapper.
# Set the key via env var, never hardcode it here. If unset, Method C simply
# returns no hits and the pipeline still runs on Methods A + B alone.
GOOGLE_JOBS_URL = "https://serpapi.com/search"
GOOGLE_JOBS_API_KEY = os.environ.get("SERPAPI_KEY", "")
GOOGLE_JOBS_LOCATION = "Germany"  # fallback when a row has no `ort`

# --- Stage 3: HR exclusion --------------------------------------------------
# Pages to scrape when looking for an HR function.
HR_PROBE_PATHS = [
    "impressum", "team", "ueber-uns", "über-uns", "unternehmen",
    "kontakt", "karriere", "jobs", "ansprechpartner",
]

# Strings that (if found near a person/role) suggest a dedicated HR function.
# Kept deliberately German and Mittelstand-realistic: a 30-80 MA Spedition/
# Logistik-Betrieb uses these titles, not startup anglicisms like "Head of HR"
# or "People & Culture". Matching is case-insensitive substring (see
# keyword_hr_hits below), so masculine roots already catch feminine forms
# (e.g. "personalleiter" is a substring of "personalleiterin") -- explicit
# feminine entries below are for readability, not strictly required.
HR_ROLE_SIGNALS = [
    "personalleiter", "personalleiterin", "personalleitung",
    "personalreferent", "personalreferentin",
    "personalsachbearbeiter", "personalsachbearbeiterin",
    "personalabteilung", "personalwesen", "personalmanagement",
    "leiter personal", "leiterin personal", "leitung personal",
    "personalchef", "personalchefin",
]

# Mailbox prefixes that imply a routed HR inbox (weaker signal on their own).
HR_MAILBOX_HINTS = ["bewerbung@", "personal@", "hr@", "karriere@", "jobs@"]

# Local LLM (LM Studio / Ollama / vLLM - any OpenAI-compatible /v1 server).
# LM Studio default: start the local server in the "Developer" tab; the URL
# below matches its default host:port. LLM_MODEL must match the "API Model
# Identifier" shown in that tab (e.g. "openai/gpt-oss-20b") -- LM Studio only
# routes to whichever model is currently loaded, but some versions still
# validate this field, so keep it in sync with what you actually loaded.
LLM_API_URL = os.environ.get("LLM_API_URL", "http://localhost:1234/v1/chat/completions")
LLM_MODEL = os.environ.get("LLM_MODEL", "openai/gpt-oss-20b")
LLM_TIMEOUT = 120

# --- Stage 4: scoring -------------------------------------------------------
WEIGHTS = {
    "active_driver_posting": 40,
    "multiple_or_recent": 15,
    "employee_sweetspot": 15,
    "no_hr_high_conf": 25,
    "direct_to_gf": 5,
}
HR_DISQUALIFY = -100  # any confident HR detection tanks the score
HR_CONF_KEEP = 0.70   # min confidence to treat "no HR" as a keep
SCORE_QUALIFY_MIN = 60  # rows at/above this go to outreach

# --- Politeness -------------------------------------------------------------
USER_AGENT = "RocketLetter-LeadResearch/1.0 (+contact: your-email@example.com)"
REQUEST_TIMEOUT = 20
REQUEST_DELAY_SECONDS = 1.5  # per-host courtesy delay
