"""
Collector-Schicht — holt öffentliche Aussagen aus Reddit, Fachforen und
LinkedIn und schreibt sie GDPR-safe in den gemeinsamen Store.

Zwei Wege:
- Apify-Actors (skalierbar, kostet Credits). Actor-IDs kommen aus der Config.
  WICHTIG: Vor dem ersten Lauf das Input-Schema des Actors in der Apify-
  Console prüfen — Actors ändern ihre Schemas gelegentlich. Die Parser hier
  sind defensiv gebaut (mehrere Feldnamen), aber das Input-Schema musst du
  einmal verifizieren.
- Reddit-Fallback ohne Apify: Reddits öffentliche JSON-Endpoints. Gratis,
  gut für Testläufe und kleine Mengen. Rate-Limits beachten (~1 req/s).

Recht/Ethik: Nur öffentliche Inhalte. Keine Logins, keine privaten Gruppen.
Gespeichert wird die Aussage + grobe Herkunft, NICHT der Autor.
"""

import os
import time
import json
import urllib.request
import urllib.parse
import urllib.error
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from storage import store

TOKEN = os.environ.get("APIFY_TOKEN")
USER_AGENT = "kundenrecherche-pipeline/1.0 (marktforschung, kontakt via apify)"


def _client():
    from apify_client import ApifyClient
    if not TOKEN:
        raise RuntimeError("APIFY_TOKEN nicht gesetzt. export APIFY_TOKEN=apify_api_...")
    return ApifyClient(TOKEN)


def _first(item: dict, *keys, default=None):
    """Defensiver Feldzugriff: Actors benennen Felder unterschiedlich."""
    for k in keys:
        v = item.get(k)
        if v not in (None, ""):
            return v
    return default


def _as_text(v) -> str:
    """
    Holt einen String aus dem, was ein Actor liefert. Manche verschachteln den
    Text (z.B. {"text": "..."} oder {"value": ...}), manche liefern Listen von
    Fragmenten. Ohne diese Normalisierung stirbt der Store an einem dict.
    """
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        for k in ("text", "content", "value", "body", "commentary"):
            got = _as_text(v.get(k))
            if got:
                return got
        return ""
    if isinstance(v, (list, tuple)):
        parts = [_as_text(x) for x in v]
        return " ".join(p for p in parts if p)
    return str(v)


def _dataset_id(run) -> str:
    """
    Dataset-ID aus einem Actor-Lauf holen — versionsunabhängig.

    apify-client >= 3 gibt aus .call() ein Run-Modell zurück (Attribut
    default_dataset_id), ältere Versionen ein Dict (Key defaultDatasetId).
    .call() kann außerdem None liefern (z.B. Timeout beim Warten).
    """
    if run is None:
        raise RuntimeError("Actor-Lauf lieferte kein Run-Objekt (None) — "
                           "Lauf in der Apify-Console prüfen.")
    ds = (run.get("defaultDatasetId") if isinstance(run, dict)
          else getattr(run, "default_dataset_id", None))
    if not ds:
        raise RuntimeError(f"Keine Dataset-ID im Run gefunden: {run!r}")
    return ds


# ---------------------------------------------------------------------------
# 1a) REDDIT via öffentliche JSON-API (gratis, für Tests & kleine Läufe)
# ---------------------------------------------------------------------------
# Reddit blockt seit 2023/24 generische User-Agents (HTTP 403). Abhilfe:
# - realistischer Browser-User-Agent
# - old.reddit.com (weniger streng als www.reddit.com)
# - Retry mit Backoff bei 403/429, längere Grundpause
BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/125.0.0.0 Safari/537.36")


def _reddit_get(url: str, retries: int = 3):
    """Eine GET-Anfrage an Reddit mit Browser-Headern und Backoff."""
    headers = {
        "User-Agent": BROWSER_UA,
        "Accept": "application/json,text/html",
        "Accept-Language": "de,en;q=0.8",
    }
    for attempt in range(retries):
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code in (403, 429) and attempt < retries - 1:
                wait = 3 * (attempt + 1)  # 3s, 6s, ...
                print(f"[reddit-free] {e.code} — warte {wait}s und versuche erneut")
                time.sleep(wait)
                continue
            raise
    return None


def collect_reddit_free(keywords: list[str], subreddits: list[str] = None,
                        limit: int = 50, delay: float = 2.5):
    """
    Nutzt old.reddit.com Such-JSON — keine Keys nötig, aber Rate-Limits.
    Holt Posts (Titel + Selftext). Kommentare holt dieser Weg nicht;
    dafür den Apify-Weg nutzen. Bei anhaltendem 403: Apify-Weg verwenden.
    """
    saved = 0
    blocked = 0
    targets = subreddits or [None]
    for kw in keywords:
        for sub in targets:
            base = (f"https://old.reddit.com/r/{sub}/search.json"
                    if sub else "https://old.reddit.com/search.json")
            params = {"q": kw, "limit": min(limit, 100), "sort": "relevance",
                      "restrict_sr": "1" if sub else "0", "t": "year"}
            url = base + "?" + urllib.parse.urlencode(params)
            try:
                data = _reddit_get(url)
            except Exception as e:
                print(f"[reddit-free] {sub or 'all'}/{kw!r} fehlgeschlagen: {e}")
                blocked += 1
                time.sleep(delay)
                continue
            if not data:
                continue
            for child in data.get("data", {}).get("children", []):
                d = child.get("data", {})
                text = (d.get("selftext") or "").strip() or d.get("title", "")
                if store.save_statement(
                    source="reddit", text=text,
                    origin=d.get("subreddit"),
                    context=d.get("title"),
                    engagement=int(d.get("score") or 0),
                    query=kw,
                ):
                    saved += 1
            time.sleep(delay)  # höflich bleiben
    print(f"[reddit-free] {saved} neue Aussagen gespeichert"
          + (f" ({blocked} Anfragen geblockt)" if blocked else ""))
    if saved == 0 and blocked:
        print("[reddit-free] Reddit blockt anhaltend. Optionen: (1) später "
              "erneut versuchen, (2) den Apify-Weg nutzen: python run.py collect reddit")
    return saved


# ---------------------------------------------------------------------------
# 1b) REDDIT via Apify (skalierbar, inkl. Kommentare — das eigentliche Gold)
# ---------------------------------------------------------------------------
def collect_reddit_apify(keywords: list[str], actor_id: str,
                         subreddits: list[str] = None,
                         max_items: int = 100, max_comments: int = 30):
    """
    Angepasst an 'spry_wholemeal/reddit-scraper' (kostenlos): Dieser Actor
    arbeitet mit mode='scrape' + listings=[{subreddit: ...}], nicht mit freier
    Keyword-Suche. Wir grasen die Subreddits ab und filtern danach LOKAL auf
    die Keywords — das passt gut zum DACH-Fokus (gezielt deutsche Subs).

    subreddits: Liste ohne 'r/'. keywords: zum lokalen Filtern der Ausgabe.
    """
    client = _client()
    subreddits = subreddits or []
    if not subreddits:
        print("[reddit-apify] Keine Subreddits konfiguriert — übersprungen.")
        return 0

    kws_lower = [k.lower() for k in keywords]
    run_input = {
        "mode": "scrape",
        "sort": "top",
        "timeframe": "year",
        "maxPosts": max_items,
        "includeCommentsMode": "all",     # Kommentare = ungefilterte Sprache
        "maxTopLevelComments": max_comments,
        "listings": [{"subreddit": s} for s in subreddits],
        "proxyConfiguration": {"useApifyProxy": True,
                               "apifyProxyGroups": ["RESIDENTIAL"]},
    }
    try:
        run = client.actor(actor_id).call(run_input=run_input)
        dataset_id = _dataset_id(run)
    except Exception as e:
        print(f"[reddit-apify] Actor-Lauf fehlgeschlagen: {e}")
        return 0

    print(f"[reddit-apify] Dataset {dataset_id} — Nachträglich erneut "
          f"einlesbar mit: python run.py ingest reddit {dataset_id}")
    return ingest_reddit_dataset(dataset_id, keywords, client=client)


def ingest_reddit_dataset(dataset_id: str, keywords: list[str] = None,
                          client=None, require_keywords: bool = False,
                          allzweck_subs: set = None, branchen_keywords: list = None):
    """
    Liest ein bereits vorhandenes Apify-Dataset in den Store — ohne den Actor
    (und damit die Credits) erneut zu verbrauchen. Nimmt eine Dataset-ID ODER
    eine Run-ID; bei einer Run-ID wird das Default-Dataset des Laufs aufgelöst.

    FILTER-PHILOSOPHIE: standardmäßig wird NICHT auf Keywords gefiltert.
    Die Subreddits sind bereits die Vorauswahl; die Relevanz entsteht später
    über die FTS-Suche mit SEARCH_TERMS in der Synthese. Spät filtern statt
    früh — ein erneuter Ingest ist gratis, ein erneuter Scrape kostet.
    require_keywords=True schaltet den alten, engen Phrasen-Filter wieder ein.
    """
    client = client or _client()
    kws_lower = [k.lower() for k in (keywords or [])] if require_keywords else []
    # Themenfilter NUR für Allzweck-Subreddits (r/de, r/Finanzen, ...). In
    # thematischen Subs wie r/webdev ist das Sub selbst schon die Vorauswahl.
    allzweck = {s.lower() for s in (allzweck_subs or set())}
    branchen = [b.lower() for b in (branchen_keywords or [])]
    gefiltert_allzweck = 0

    # Run-ID? Dann Default-Dataset des Laufs nehmen. Sonst als Dataset-ID lesen.
    try:
        run = client.run(dataset_id).get()
        if run is not None:
            dataset_id = _dataset_id(run)
    except Exception:
        pass

    saved = 0
    seen = 0
    for item in client.dataset(dataset_id).iterate_items():
        seen += 1
        text = _first(item, "body", "text", "selftext", "comment",
                      "postText", default="")
        title = _first(item, "title", "postTitle")
        # Engagement defensiv über mehrere Feldnamen. Dieser Actor liefert
        # aktuell durchgehend 0 — die Alternativen stehen hier, damit ein
        # späterer Lauf mit echten Werten ohne Codeänderung durchkommt.
        engagement = _first(item, "score", "upVotes", "numberOfVotes",
                            "estimated_upvotes", "total_votes", default=0)
        origin = _first(item, "subreddit", "communityName", "display_name")
        posted_at = _first(item, "created_utc_iso", "createdAt", "created_at")
        haystack = f"{title or ''} {text or ''}".lower()
        # Keyword-Filter nur wenn ausdrücklich verlangt (require_keywords).
        if kws_lower and not any(k in haystack for k in kws_lower):
            continue
        # Themenfilter für Allzweck-Subs: ohne Branchenbezug fliegt es raus.
        if branchen and str(origin or "").lower() in allzweck:
            if not any(b in haystack for b in branchen):
                gefiltert_allzweck += 1
                continue
        if store.save_statement(
            source="reddit", text=text, origin=origin,
            context=title, engagement=int(engagement or 0),
            posted_at=posted_at,
            query="|".join(keywords)[:80] if kws_lower else "reddit-subreddits",
        ):
            saved += 1
    modus = "auf Keywords gefiltert" if kws_lower else "ungefiltert (Subreddit-Vorauswahl)"
    print(f"[reddit-apify] {saved} neue Aussagen gespeichert "
          f"(aus {seen} Dataset-Items, {modus})")
    if gefiltert_allzweck:
        print(f"[reddit-apify] {gefiltert_allzweck} Items aus Allzweck-Subs ohne "
              f"Branchenbezug verworfen")
    if seen and not saved:
        print("[reddit-apify] Nichts gespeichert — Feldnamen des Actors prüfen: "
              "python explore.py fields <datasetId> zeigt, was wirklich ankommt.")
    return saved


# ---------------------------------------------------------------------------
# 2) FACHFOREN (generischer Crawler, weil keine APIs)
# ---------------------------------------------------------------------------
def collect_forums(forum_urls: list[str], keywords: list[str], actor_id: str,
                   max_pages: int = 50):
    client = _client()
    saved = 0
    run_input = {
        "startUrls": [{"url": u} for u in forum_urls],
        "maxCrawlPages": max_pages,
        "crawlerType": "cheerio",
        "proxyConfiguration": {"useApifyProxy": True},
        "removeElementsCssSelector": "nav, header, footer, aside, .signature, .ad, .cookie",
    }
    run = client.actor(actor_id).call(run_input=run_input)

    kws = [k.lower() for k in keywords]
    for item in client.dataset(_dataset_id(run)).iterate_items():
        text = item.get("text") or ""
        meta = item.get("metadata") or {}
        title = meta.get("title") if isinstance(meta, dict) else None
        origin = item.get("url", "").split("/")[2] if item.get("url") else "forum"
        # FIX: Filter PRO ABSATZ, nicht pro Seite — sonst landet bei einem
        # einzigen Keyword-Treffer die halbe Seite (Navigation etc.) im Store.
        for para in text.split("\n"):
            p = para.strip()
            if p and any(k in p.lower() for k in kws):
                if store.save_statement(
                    source="forum", text=p, origin=origin,
                    context=title, query="|".join(keywords),
                ):
                    saved += 1
    print(f"[forum] {saved} neue Aussagen gespeichert")
    return saved


# ---------------------------------------------------------------------------
# 3) LINKEDIN via Apify
# ---------------------------------------------------------------------------
def collect_linkedin(keywords: list[str], actor_id: str, max_items: int = 80):
    """
    Angepasst an 'apimaestro/linkedin-posts-search-scraper-no-cookies'
    (Schema geprüft 07/2026): Suchbegriff heißt 'keyword' (Einzahl!), die
    Gesamtmenge 'total_posts'. Die früheren Feldnamen (keywords/maxItems)
    kannte der Actor nicht — er hätte stillschweigend nichts geliefert.

    Preis: 5 $ pro 1.000 Ergebnisse. Ein Lauf über N Keywords à max_items
    kostet also N * max_items / 1000 * 5 $.

    GDPR: Der Actor liefert auch Autordaten mit. Wir speichern ausschließlich
    den Text — kein Name, kein Profil-Link. Namen INNERHALB eines Posts kann
    der Store nicht erkennen; bei LinkedIn ist das wahrscheinlicher als bei
    Reddit, also die Ausgabe stichprobenhaft prüfen.
    """
    client = _client()
    saved = 0
    for kw in keywords:
        run_input = {
            "keyword": kw,
            "total_posts": max_items,
            "sort_type": "date_posted",   # neueste zuerst
        }
        try:
            run = client.actor(actor_id).call(run_input=run_input)
            dataset_id = _dataset_id(run)
        except Exception as e:
            print(f"[linkedin] '{kw}' fehlgeschlagen: {e}")
            continue

        print(f"[linkedin] '{kw}' -> Dataset {dataset_id} "
              f"(nachlesbar mit: python run.py ingest linkedin {dataset_id})")
        saved += ingest_linkedin_dataset(dataset_id, kontext=kw, client=client)
    print(f"[linkedin] {saved} neue Aussagen gespeichert")
    return saved


def ingest_linkedin_dataset(dataset_id: str, kontext: str = None, client=None):
    """
    Liest ein bereits bezahltes LinkedIn-Dataset ein — ohne den Actor erneut
    laufen zu lassen. Nimmt Dataset-ID ODER Run-ID.
    """
    client = client or _client()
    try:
        run = client.run(dataset_id).get()
        if run is not None:
            dataset_id = _dataset_id(run)
    except Exception:
        pass

    saved = seen = ohne_text = 0
    for item in client.dataset(dataset_id).iterate_items():
        seen += 1
        text = _as_text(_first(item, "text", "content", "postText", "commentary",
                               "post_text", "description", default=""))
        if not text:
            ohne_text += 1
            continue
        engagement = _first(item, "numLikes", "likes", "reactionsCount",
                            "reactions_count", "total_reactions",
                            "num_likes", default=0)
        if isinstance(engagement, dict):
            engagement = _first(engagement, "total", "count", "likes", default=0)
        posted_at = _as_text(_first(item, "postedAt", "posted_at", "publishedAt",
                                    "date", "time", "posted_at_iso"))
        if store.save_statement(
            source="linkedin", text=text, origin="linkedin",
            context=kontext, engagement=int(engagement or 0),
            posted_at=posted_at or None, query=kontext or "linkedin",
        ):
            saved += 1
    print(f"[linkedin] {saved} neue Aussagen aus {seen} Dataset-Items"
          + (f" ({ohne_text} ohne erkennbaren Text)" if ohne_text else ""))
    if seen and ohne_text == seen:
        print("[linkedin] Kein Item hatte ein bekanntes Textfeld — Feldnamen "
              f"prüfen: python explore.py dump {dataset_id} && "
              f"python explore.py fields {dataset_id}")
    return saved


# ---------------------------------------------------------------------------
# 4) GOOGLE → BLOGS / FACHARTIKEL
# ---------------------------------------------------------------------------
# Zwei Schritte: (1) Google-SERP findet URLs, (2) Website-Crawler holt den
# Inhalt. Snippets aus der SERP landen zusätzlich im Store — oft schon
# brauchbare Problemformulierungen, ohne den zweiten Crawl abzuwarten.
#
# Actor: apify/google-search-scraper (Input: queries als Multiline-String,
# maxPagesPerQuery). Content-Crawl: gleicher Actor wie bei Foren.

_GOOGLE_SKIP_HOSTS = (
    "google.", "youtube.com", "youtu.be", "linkedin.com", "facebook.com",
    "instagram.com", "tiktok.com", "twitter.com", "x.com", "reddit.com",
    "maps.google.", "play.google.", "apps.apple.com", "amazon.",
    "indeed.", "stepstone.", "xing.com", "kununu.com", "glassdoor.",
)


def _host_of(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""


def _is_crawlable_url(url: str) -> bool:
    if not url or not url.startswith("http"):
        return False
    host = _host_of(url)
    return not any(skip in host for skip in _GOOGLE_SKIP_HOSTS)


def collect_google_blogs(
    queries: list[str],
    keywords: list[str],
    search_actor_id: str = "apify/google-search-scraper",
    crawl_actor_id: str = "apify/website-content-crawler",
    max_pages_per_query: int = 1,
    max_crawl_pages: int = 40,
    country_code: str = "de",
    language_code: str = "de",
):
    """
    Sucht per Google nach Beiträgen/Blogs und crawlt die Treffer-Seiten.

    queries: Suchbegriffe (z.B. "Personalberatung Neukunden gewinnen")
    keywords: Filter für Absätze beim Content-Crawl (wie FORUM_KEYWORDS)
    """
    if not queries:
        print("[google] Keine GOOGLE_QUERIES in der Config — übersprungen.")
        return 0

    client = _client()
    kws = [k.lower() for k in keywords]
    saved_snippets = 0
    urls: list[str] = []
    seen_urls: set[str] = set()

    # --- Schritt 1: SERP ---
    run_input = {
        "queries": "\n".join(queries),
        "maxPagesPerQuery": max_pages_per_query,
        "resultsPerPage": 10,
        "countryCode": country_code,
        "languageCode": language_code,
        "mobileResults": False,
        "includeUnfilteredResults": False,
    }
    try:
        run = client.actor(search_actor_id).call(run_input=run_input)
        dataset_id = _dataset_id(run)
    except Exception as e:
        print(f"[google] SERP-Lauf fehlgeschlagen: {e}")
        return 0

    print(f"[google] SERP-Dataset {dataset_id}")
    for item in client.dataset(dataset_id).iterate_items():
        # Ein Item = eine Suchseite; organische Treffer sind verschachtelt.
        organic = item.get("organicResults") or item.get("organic") or []
        query = _first(item, "searchQuery", "query", default="")
        if isinstance(query, dict):
            query = _first(query, "term", "query", default="")
        for hit in organic:
            if not isinstance(hit, dict):
                continue
            url = _first(hit, "url", "link", default="") or ""
            title = _as_text(_first(hit, "title", default=""))
            desc = _as_text(_first(hit, "description", "snippet", default=""))
            # Snippet speichern (Titel + Beschreibung) — günstige Vorstufe.
            # Query filtert Relevanz schon; kein zweiter Keyword-Check nötig.
            snippet = " — ".join(p for p in (title, desc) if p).strip()
            if snippet:
                origin = _host_of(url) or "google"
                if store.save_statement(
                    source="blog", text=snippet, origin=origin,
                    context=title or query, query=str(query)[:80] or "google",
                ):
                    saved_snippets += 1
            if _is_crawlable_url(url) and url not in seen_urls:
                seen_urls.add(url)
                urls.append(url)

    print(f"[google] {saved_snippets} SERP-Snippets, {len(urls)} URLs zum Crawlen")
    if not urls:
        return saved_snippets

    # --- Schritt 2: Seiteninhalte ---
    # Cap, damit ein breiter SERP-Lauf nicht unbegrenzt Credits frisst.
    urls = urls[:max_crawl_pages]
    crawl_input = {
        "startUrls": [{"url": u} for u in urls],
        "maxCrawlPages": max_crawl_pages,
        "crawlerType": "cheerio",
        "proxyConfiguration": {"useApifyProxy": True},
        "removeElementsCssSelector": (
            "nav, header, footer, aside, .signature, .ad, .cookie, "
            ".sidebar, .comments, .related-posts, script, style"
        ),
        # Nur die Seed-URLs, nicht dem Blog hinterherlaufen.
        "maxCrawlDepth": 0,
    }
    try:
        crawl_run = client.actor(crawl_actor_id).call(run_input=crawl_input)
        crawl_ds = _dataset_id(crawl_run)
    except Exception as e:
        print(f"[google] Content-Crawl fehlgeschlagen: {e}")
        return saved_snippets

    saved_paras = 0
    for item in client.dataset(crawl_ds).iterate_items():
        text = item.get("text") or ""
        meta = item.get("metadata") or {}
        title = meta.get("title") if isinstance(meta, dict) else None
        url = item.get("url") or ""
        origin = _host_of(url) or "blog"
        for para in text.split("\n"):
            p = para.strip()
            if not p:
                continue
            # Ohne Keywords: alles durchlassen (Store filtert Junk/Länge).
            # Mit Keywords: Absatz muss mindestens eins treffen.
            if kws and not any(k in p.lower() for k in kws):
                continue
            if store.save_statement(
                source="blog", text=p, origin=origin,
                context=title, query="google-blog",
            ):
                saved_paras += 1

    total = saved_snippets + saved_paras
    print(f"[google] {saved_paras} Absätze aus Artikeln, "
          f"gesamt {total} neue Aussagen")
    return total


# ---------------------------------------------------------------------------
# 5) GENERISCHER REGISTER-COLLECTOR
# ---------------------------------------------------------------------------
def collect_from_registry(sources: dict):
    """
    Läuft alle in der Config aktivierten Register-Quellen ab.

    sources: dict aus der Config, z.B.
        SOURCES = {
            "google_maps_reviews": {"queries": ["Webdesign Agentur München"], "max_reviews": 80},
            "kununu": {"urls": ["https://www.kununu.com/de/..."]},
        }
    Schlüssel = Register-Key, Wert = Config-Block für diese Quelle.
    """
    from collectors.registry import REGISTRY
    total = 0
    for key, block in sources.items():
        if key not in REGISTRY:
            print(f"[registry] '{key}' unbekannt — übersprungen. "
                  f"Verfügbar: {', '.join(REGISTRY)}")
            continue
        spec = REGISTRY[key]
        client = _client()
        try:
            run_input = spec["build_input"](block)
            run = client.actor(spec["actor"]).call(run_input=run_input)
            dataset_id = _dataset_id(run)
        except Exception as e:
            print(f"[{key}] Lauf fehlgeschlagen: {e}")
            continue

        saved = 0
        for item in client.dataset(dataset_id).iterate_items():
            text, context, engagement, origin = spec["parse"](item)
            if store.save_statement(
                source=spec["source"], text=text, origin=origin,
                context=context, engagement=engagement, query=key,
            ):
                saved += 1
        print(f"[{key}] {saved} neue Aussagen gespeichert")
        total += saved
    print(f"[registry] gesamt {total} neue Aussagen")
    return total
