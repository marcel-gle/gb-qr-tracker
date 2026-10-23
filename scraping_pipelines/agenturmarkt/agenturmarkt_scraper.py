#!/usr/bin/env python3
"""
Agenturmarkt.de Scraper
=======================
Crawls all agency listings + detail pages from agenturmarkt.de into a local
SQLite database, resumable at any time, and exports to CSV/XLSX.

Usage:
    python agenturmarkt_scraper.py scrape              # full crawl (resumable)
    python agenturmarkt_scraper.py scrape --limit 100  # sample run
    python agenturmarkt_scraper.py scrape --category seo-agentur
    python agenturmarkt_scraper.py scrape --city berlin
    python agenturmarkt_scraper.py export              # write CSV (+XLSX if openpyxl installed)
    python agenturmarkt_scraper.py status              # progress overview

Options:
    --delay 1.0     seconds between requests (default 1.0 — be polite)
    --db PATH       SQLite file (default agenturmarkt.db)
    --no-details    only collect slugs + listing data, skip detail pages

Requirements:
    pip install requests beautifulsoup4 lxml
    pip install openpyxl   # optional, for XLSX export

Notes:
    - Detail pages still serve placeholder phone/e-mail. Real business e-mails
      appear on listing cards (masked in HTML) and are decoded from there.
    - Check the site's AGB / legal constraints before large-scale use.
"""

import argparse
import html as html_lib
import json
import random
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from urllib.parse import unquote

import requests
from bs4 import BeautifulSoup

try:
    from tqdm import tqdm
    HAVE_TQDM = True
except ImportError:  # progress bar is optional — fall back to plain prints
    HAVE_TQDM = False

BASE = "https://www.agenturmarkt.de"

# --- browser fingerprints -------------------------------------------------
# Each profile is internally consistent: the User-Agent matches its client
# hints (sec-ch-ua*) and platform, so the header set looks like a real
# browser rather than a random mix that fingerprinters flag instantly.
BROWSER_PROFILES = [
    {
        "ua": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"),
        "ch_ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        "ch_mobile": "?0",
        "ch_platform": '"Windows"',
        "engine": "blink",
    },
    {
        "ua": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
               "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"),
        "ch_ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        "ch_mobile": "?0",
        "ch_platform": '"macOS"',
        "engine": "blink",
    },
    {
        "ua": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"),
        "ch_ua": '"Microsoft Edge";v="124", "Chromium";v="124", "Not-A.Brand";v="99"',
        "ch_mobile": "?0",
        "ch_platform": '"Windows"',
        "engine": "blink",
    },
    {
        # Firefox does not send sec-ch-ua / sec-fetch client hints
        "ua": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) "
               "Gecko/20100101 Firefox/127.0"),
        "engine": "gecko",
    },
    {
        "ua": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7; rv:126.0) "
               "Gecko/20100101 Firefox/126.0"),
        "engine": "gecko",
    },
    {
        # Safari on macOS — no client hints
        "ua": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
               "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15"),
        "engine": "webkit",
    },
]

ACCEPT_LANGS = [
    "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "de-DE,de;q=0.9,en;q=0.8",
    "de,en-US;q=0.9,en;q=0.8",
    "de-DE,de;q=0.8",
]

ACCEPT_HTML = ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,image/apng,*/*;q=0.8,"
               "application/signed-exchange;v=b3;q=0.7")


def build_headers(profile, referer=None):
    """Assemble a realistic, self-consistent header set for a profile."""
    headers = {
        "User-Agent": profile["ua"],
        "Accept": ACCEPT_HTML,
        "Accept-Language": random.choice(ACCEPT_LANGS),
        "Accept-Encoding": "gzip, deflate, br",
        "Upgrade-Insecure-Requests": "1",
        "Connection": "keep-alive",
    }
    # Client hints + fetch metadata only for Chromium-based engines
    if profile["engine"] == "blink":
        headers.update({
            "sec-ch-ua": profile["ch_ua"],
            "sec-ch-ua-mobile": profile["ch_mobile"],
            "sec-ch-ua-platform": profile["ch_platform"],
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin" if referer else "none",
            "Sec-Fetch-User": "?1",
        })
    if referer:
        headers["Referer"] = referer
    elif random.random() < 0.4:
        # sometimes arrive "from Google" on the first hit
        headers["Referer"] = "https://www.google.com/"
    return headers

SLUG_RE = re.compile(r"^/agentur/([a-z0-9\-]+)$")
TOTAL_RE = re.compile(r"von\s+([\d.,]+)")
FOUNDED_RE = re.compile(r"Gegründet\s+(\d{4})")
EMPLOYEES_RE = re.compile(r"([\d.]+)\s+Mitarbeiter")
# Listing cards: .de<!ATstammgast-personal#.info> → info@stammgast-personal.de
EMAIL_MASK_RE = re.compile(r"\.([^<]+)<!AT([^#>]+)#\.?([^>]+)>")
PLACEHOLDER_EMAIL_RE = re.compile(
    r"(mustermann|example\.|aaaaaaaa|placeholder|noreply)", re.I
)


# ---------------------------------------------------------------- database

SCHEMA = """
CREATE TABLE IF NOT EXISTS agencies (
    slug            TEXT PRIMARY KEY,
    name            TEXT,
    slogan          TEXT,
    score           REAL,
    review_count    INTEGER,
    city            TEXT,
    state           TEXT,
    categories      TEXT,   -- JSON list
    services        TEXT,   -- JSON list
    industries      TEXT,   -- JSON list
    website         TEXT,
    email           TEXT,
    founded         INTEGER,
    employees       TEXT,
    address         TEXT,
    verified        INTEGER,
    description     TEXT,
    detail_status   TEXT DEFAULT 'pending',   -- pending | done | error
    detail_error    TEXT,
    scraped_at      TEXT
);
CREATE TABLE IF NOT EXISTS crawl_state (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


def db_connect(path):
    con = sqlite3.connect(path)
    con.executescript(SCHEMA)
    cols = {r[1] for r in con.execute("PRAGMA table_info(agencies)")}
    if "email" not in cols:
        con.execute("ALTER TABLE agencies ADD COLUMN email TEXT")
        con.commit()
    return con


def state_get(con, key, default=None):
    row = con.execute("SELECT value FROM crawl_state WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def state_set(con, key, value):
    con.execute(
        "INSERT INTO crawl_state(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )
    con.commit()


# ---------------------------------------------------------------- fetching

# A "browsing session" keeps one fingerprint + cookie jar for a run of
# requests, then rotates — this mimics different visitors instead of one
# client that suspiciously changes its User-Agent on every hit.
_session = None
_profile = None
_requests_on_session = 0
_rotate_after = 0
_last_url = None


def _new_session():
    """Spin up a fresh session with a new random browser fingerprint."""
    global _session, _profile, _requests_on_session, _rotate_after
    _profile = random.choice(BROWSER_PROFILES)
    _session = requests.Session()
    _session.headers.update(build_headers(_profile))
    _requests_on_session = 0
    _rotate_after = random.randint(15, 40)  # requests before next rotation


def _polite_sleep(delay):
    """Randomised, human-ish pause between requests."""
    # jitter around the base delay (never below ~0.4s to stay polite)
    base = max(0.4, random.uniform(delay * 0.6, delay * 1.8))
    # occasional longer "reading" pause, as a real user would take
    if random.random() < 0.08:
        base += random.uniform(2.0, 6.0)
    time.sleep(base)


def fetch(url, delay, retries=4):
    """GET with rotating fingerprints, jittered timing, retry + backoff."""
    global _requests_on_session, _last_url

    if _session is None or _requests_on_session >= _rotate_after:
        _new_session()

    for attempt in range(retries):
        try:
            # per-request headers: rebuild referer/fetch-metadata for realism
            headers = build_headers(_profile, referer=_last_url)
            r = _session.get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                _requests_on_session += 1
                _last_url = url
                _polite_sleep(delay)
                return r.text
            if r.status_code == 404:
                raise LookupError(f"404: {url}")
            if r.status_code in (403, 429, 503):
                # possible soft-block: back off AND rotate fingerprint
                wait = 2 ** (attempt + 2) + random.uniform(0, 3)  # ~4-35 s
                print(f"  blocked/limited ({r.status_code}), waiting "
                      f"{wait:.1f}s and rotating identity…")
                time.sleep(wait)
                _new_session()
                continue
            r.raise_for_status()
        except (requests.ConnectionError, requests.Timeout) as e:
            wait = 2 ** (attempt + 1) + random.uniform(0, 2)
            print(f"  network error ({e.__class__.__name__}), retry in "
                  f"{wait:.1f}s…")
            time.sleep(wait)
    raise RuntimeError(f"giving up on {url}")


# ---------------------------------------------------------------- parsing

def parse_german_number(s):
    """'40.480' -> 40480 ; '4,9' -> 4.9"""
    s = s.strip()
    if "," in s:
        return float(s.replace(".", "").replace(",", "."))
    return int(s.replace(".", ""))


def extract_slugs(html):
    """All agency detail slugs linked on a page (listing, category, city …)."""
    soup = BeautifulSoup(html, "lxml")
    slugs = []
    for a in soup.find_all("a", href=True):
        href = a["href"].replace(BASE, "")
        m = SLUG_RE.match(href.split("?")[0])
        if m:
            slugs.append(m.group(1))
    # de-dupe, keep order
    return list(dict.fromkeys(slugs))


def decode_masked_email(masked):
    """'.de<!ATstammgast-personal#.info>' → 'info@stammgast-personal.de'"""
    if not masked:
        return None
    m = EMAIL_MASK_RE.search(html_lib.unescape(masked))
    if not m:
        return None
    tld, domain, local = (g.strip(". ") for g in m.groups())
    if not tld or not domain or not local:
        return None
    return f"{local}@{domain}.{tld}".lower()


def _clean_email(raw):
    if not raw:
        return None
    email = unquote(raw).split("?")[0].strip().strip("'\"")
    if email.lower().startswith("mailto:"):
        email = email[7:]
    email = email.lower()
    if "@" not in email or PLACEHOLDER_EMAIL_RE.search(email):
        return None
    return email


def extract_listing_email(item):
    """Real e-mails live on listing cards; detail pages only have placeholders."""
    for a in item.find_all("a", href=True):
        href = a["href"]
        if href.lower().startswith("mailto:"):
            email = _clean_email(href)
            if email:
                return email
    return decode_masked_email(str(item))


def parse_listing_cards(html):
    """Parse .company-list-item cards (name, email, website, score, city)."""
    soup = BeautifulSoup(html, "lxml")
    cards = []
    for item in soup.select(".company-list-item"):
        slug = None
        for a in item.find_all("a", href=True):
            href = a["href"].replace(BASE, "").split("?")[0]
            m = SLUG_RE.match(href)
            if m:
                slug = m.group(1)
                break
        if not slug:
            continue
        d = {"slug": slug}
        h3 = item.select_one("h3.name")
        if h3:
            d["name"] = h3.get_text(" ", strip=True)
        catch = item.select_one("p.catchphrase")
        if catch:
            slogan = catch.get_text(" ", strip=True)
            if slogan:
                d["slogan"] = slogan
        rating = item.select_one("[data-rating]")
        if rating and rating.get("data-rating"):
            try:
                d["score"] = round(float(str(rating["data-rating"]).replace(",", ".")), 2)
            except (TypeError, ValueError):
                pass
            try:
                d["review_count"] = int(rating.get("data-reviews") or 0)
            except (TypeError, ValueError):
                pass
        loc = item.select_one('[wire\\:key^="location-"] span')
        if loc:
            city = loc.get_text(" ", strip=True).split(",")[0].strip()
            if city:
                d["city"] = city
        fy = item.select_one('[wire\\:key^="founding-year-"] span')
        if fy:
            year = fy.get_text(strip=True)
            if year.isdigit():
                d["founded"] = int(year)
        for a in item.find_all("a", href=True):
            if "utm_campaign=list" in a["href"]:
                d["website"] = a["href"].split("?")[0]
                break
        email = extract_listing_email(item)
        if email:
            d["email"] = email
        cats = [s.get_text(" ", strip=True) for s in item.select(".item-category span")]
        cats = list(dict.fromkeys(c for c in cats if c))
        if cats:
            d["categories"] = json.dumps(cats, ensure_ascii=False)
        cards.append(d)
    return cards


def upsert_listing_card(con, card):
    """Insert slug and fill listing fields without wiping later detail data."""
    slug = card["slug"]
    con.execute("INSERT OR IGNORE INTO agencies(slug) VALUES (?)", (slug,))
    sets, vals = [], []
    for k in ("name", "slogan", "email", "website", "city", "score",
              "review_count", "founded", "categories"):
        v = card.get(k)
        if v in (None, "", "[]"):
            continue
        sets.append(f"{k}=COALESCE({k}, ?)")
        vals.append(v)
    if sets:
        con.execute(
            f"UPDATE agencies SET {', '.join(sets)} WHERE slug=?",
            vals + [slug],
        )


def extract_total(html):
    """Total result count from 'Ergebnisse X - Y von 40.480'."""
    m = re.search(r"Ergebnisse\s+[\d.]+\s*[-–]\s*[\d.]+\s+von\s+([\d.]+)", html)
    if m:
        return int(parse_german_number(m.group(1)))
    m = re.search(r"([\d.]+)\s+Agenturen", html)
    return int(parse_german_number(m.group(1))) if m else None


def parse_jsonld(soup):
    """Return merged JSON-LD dicts if the page ships schema.org data."""
    out = {}
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except (json.JSONDecodeError, TypeError):
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            if isinstance(item, dict):
                t = item.get("@type", "")
                if t in ("LocalBusiness", "Organization", "ProfessionalService"):
                    out.update(item)
    return out


def section_list_items(soup, heading_text):
    """Find <h2>/<h3> whose text contains heading_text, return following <li> texts."""
    for h in soup.find_all(["h2", "h3", "h4"]):
        if heading_text.lower() in h.get_text(" ", strip=True).lower():
            ul = h.find_next(["ul", "ol"])
            if ul:
                return [li.get_text(" ", strip=True) for li in ul.find_all("li")]
    return []


def parse_detail(html, slug):
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    d = {"slug": slug}

    # --- JSON-LD first (most reliable if present)
    ld = parse_jsonld(soup)
    if ld:
        d["name"] = ld.get("name")
        addr = ld.get("address") or {}
        if isinstance(addr, dict):
            parts = [addr.get("streetAddress"), addr.get("postalCode"),
                     addr.get("addressLocality")]
            d["address"] = ", ".join(p for p in parts if p) or None
            d["city"] = d.get("city") or addr.get("addressLocality")
        agg = ld.get("aggregateRating") or {}
        if isinstance(agg, dict):
            try:
                d["score"] = float(str(agg.get("ratingValue")).replace(",", "."))
                d["review_count"] = int(agg.get("reviewCount") or agg.get("ratingCount"))
            except (TypeError, ValueError):
                pass

    # --- HTML heuristics (fill gaps / no JSON-LD case)
    if not d.get("name"):
        h1 = soup.find("h1")
        d["name"] = h1.get_text(" ", strip=True) if h1 else slug

    # score: "4,9" appearing right after "Agentur-Score"
    if not d.get("score"):
        m = re.search(r"Agentur-Score:?\s*([\d],[\d])", text)
        if m:
            d["score"] = float(m.group(1).replace(",", "."))
    if not d.get("review_count"):
        m = re.search(r"([\d.]+)\s+Bewertung(?:en)?", text)
        if m:
            d["review_count"] = int(parse_german_number(m.group(1)))

    # description: paragraphs after "Wer ist …?"
    for h in soup.find_all("h2"):
        if h.get_text(strip=True).startswith("Wer ist"):
            paras = []
            for sib in h.find_next_siblings():
                if sib.name and sib.name.startswith("h"):
                    break
                if sib.name == "p":
                    paras.append(sib.get_text(" ", strip=True))
                else:
                    paras.extend(p.get_text(" ", strip=True)
                                 for p in sib.find_all("p"))
            d["description"] = "\n\n".join(p for p in paras if p) or None
            break

    d["services"] = json.dumps(section_list_items(soup, "Dienstleistungen"),
                               ensure_ascii=False)
    d["industries"] = json.dumps(section_list_items(soup, "Branchen"),
                                 ensure_ascii=False)

    # categories: links to /agenturen/<category-slug> within main content
    cats = []
    for a in soup.find_all("a", href=True):
        m = re.match(r"^(?:https?://www\.agenturmarkt\.de)?/agenturen/([a-z0-9\-]+)$",
                     a["href"])
        if m and not re.match(r"^(berlin|hamburg|munchen|koln)", m.group(1)):
            label = a.get_text(" ", strip=True)
            if label and ("agentur" in m.group(1) or "agentur" in label.lower()):
                cats.append(label)
    d["categories"] = json.dumps(list(dict.fromkeys(cats)), ensure_ascii=False)

    # website: outbound link with utm_source=agenturmarkt
    for a in soup.find_all("a", href=True):
        if "utm_source=agenturmarkt" in a["href"] and "utm_campaign=profile" in a["href"]:
            d["website"] = a["href"].split("?")[0]
            break

    m = FOUNDED_RE.search(text)
    if m:
        d["founded"] = int(m.group(1))
    m = EMPLOYEES_RE.search(text)
    if m:
        d["employees"] = m.group(1)

    # city, state: a text node reading exactly "Passau, Bayern"
    city_re = re.compile(
        r"^([A-ZÄÖÜ][\w\s.\-()äöüß]{1,40}?),\s*"
        r"(Baden-Württemberg|Bayern|Berlin|Brandenburg|Bremen|Hamburg|Hessen|"
        r"Mecklenburg-Vorpommern|Niedersachsen|Nordrhein-Westfalen|"
        r"Rheinland-Pfalz|Saarland|Sachsen-Anhalt|Sachsen|Schleswig-Holstein|"
        r"Thüringen|Wien|Zürich)$")
    for s in soup.stripped_strings:
        m = city_re.match(s)
        if m:
            if not d.get("city"):
                d["city"] = m.group(1).strip()
            d["state"] = m.group(2)
            break

    # address: text block after "Adresse:"
    if not d.get("address"):
        m = re.search(r"Adresse:\s*(.{10,120}?)\s+(?:Deutschland|Österreich|Schweiz)",
                      text)
        if m:
            d["address"] = m.group(1).strip()

    d["verified"] = 1 if "Verifiziertes Profil" in text else 0

    # slogan: og:description often repeats; use first non-name heading sibling
    if not d.get("slogan"):
        h1 = soup.find("h1")
        if h1:
            nxt = h1.find_next("p")
            if nxt:
                t = nxt.get_text(" ", strip=True)
                if 0 < len(t) < 120:
                    d["slogan"] = t

    d["scraped_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    d["detail_status"] = "done"
    return d


# ---------------------------------------------------------------- stages

def stage_listing(con, args):
    """Collect all slugs by paging through the listing."""
    base_path = "/agenturen"
    if args.category:
        base_path = f"/agenturen/{args.category}"
    elif args.city:
        base_path = f"/agenturen/{args.city}"

    page = int(state_get(con, f"listing_page:{base_path}", 0)) + 1
    total_pages = None
    empty_streak = 0
    bar = None

    while True:
        url = f"{BASE}{base_path}?page={page}"
        try:
            html = fetch(url, args.delay)
        except Exception as e:
            print(f"listing page {page} failed: {e}")
            break

        cards = parse_listing_cards(html)
        # Prefer listing cards so "Kürzlich bewertet" (other categories) is ignored.
        slugs = [c["slug"] for c in cards] or extract_slugs(html)
        if total_pages is None:
            total = extract_total(html)
            if total:
                total_pages = -(-total // 10)  # ceil, 10 per page
                print(f"{total} agencies ≈ {total_pages} listing pages")
                if HAVE_TQDM:
                    bar = tqdm(total=total_pages, desc="listing", unit="page",
                               initial=page - 1)

        new = 0
        if cards:
            for card in cards:
                before = con.execute(
                    "SELECT 1 FROM agencies WHERE slug=?", (card["slug"],)
                ).fetchone()
                upsert_listing_card(con, card)
                if before is None:
                    new += 1
        else:
            for slug in slugs:
                cur = con.execute(
                    "INSERT OR IGNORE INTO agencies(slug) VALUES (?)", (slug,))
                new += cur.rowcount
        con.commit()
        state_set(con, f"listing_page:{base_path}", page)

        known = con.execute("SELECT COUNT(*) FROM agencies").fetchone()[0]
        if bar is not None:
            bar.update(1)
            bar.set_postfix(known=known, new=new)
        else:
            print(f"listing page {page}: {len(slugs)} links, {new} new "
                  f"(total known: {known})")

        empty_streak = empty_streak + 1 if not slugs else 0
        if empty_streak >= 3:
            print("3 empty pages in a row — done with listings.")
            break
        if total_pages and page >= total_pages:
            print("reached last listing page.")
            break
        if args.limit and known >= args.limit:
            if bar is None:
                print(f"--limit {args.limit} reached, stopping listing crawl.")
            break
        page += 1

    if bar is not None:
        bar.close()


def stage_details(con, args):
    rows = con.execute(
        "SELECT slug FROM agencies WHERE detail_status='pending' ORDER BY slug"
    ).fetchall()
    todo = [r[0] for r in rows]
    if args.limit:
        todo = todo[: args.limit]
    print(f"{len(todo)} detail pages to fetch")

    bar = tqdm(todo, desc="details", unit="agency") if HAVE_TQDM else None
    for i, slug in enumerate(todo, 1):
        url = f"{BASE}/agentur/{slug}"
        try:
            html = fetch(url, args.delay)
            d = parse_detail(html, slug)
        except LookupError:
            d = {"slug": slug, "detail_status": "error", "detail_error": "404"}
        except Exception as e:
            d = {"slug": slug, "detail_status": "error", "detail_error": str(e)[:200]}

        cols = ", ".join(f"{k}=?" for k in d if k != "slug")
        con.execute(f"UPDATE agencies SET {cols} WHERE slug=?",
                    [v for k, v in d.items() if k != "slug"] + [slug])
        con.commit()

        if bar is not None:
            bar.update(1)
        elif i % 25 == 0 or i == len(todo):
            done = con.execute(
                "SELECT COUNT(*) FROM agencies WHERE detail_status='done'"
            ).fetchone()[0]
            print(f"  {i}/{len(todo)} this run — {done} done overall")

    if bar is not None:
        bar.close()


# ---------------------------------------------------------------- export

EXPORT_COLS = ["slug", "name", "slogan", "score", "review_count", "city", "state",
               "categories", "services", "industries", "website", "email", "founded",
               "employees", "address", "verified", "description", "scraped_at"]


def cmd_export(con, args):
    import csv

    rows = con.execute(
        f"SELECT {', '.join(EXPORT_COLS)} FROM agencies "
        "WHERE detail_status='done' OR name IS NOT NULL ORDER BY slug"
    ).fetchall()

    def prettify(row):
        out = list(row)
        for idx in (7, 8, 9):  # JSON list columns -> "a; b; c"
            try:
                out[idx] = "; ".join(json.loads(out[idx] or "[]"))
            except (json.JSONDecodeError, TypeError):
                pass
        return out

    csv_path = args.out or "agenturmarkt_export.csv"
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(EXPORT_COLS)
        w.writerows(prettify(r) for r in rows)
    print(f"wrote {len(rows)} rows -> {csv_path}")

    try:
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Agenturen"
        ws.append(EXPORT_COLS)
        for r in rows:
            ws.append(prettify(r))
        xlsx_path = csv_path.rsplit(".", 1)[0] + ".xlsx"
        wb.save(xlsx_path)
        print(f"wrote {xlsx_path}")
    except ImportError:
        print("openpyxl not installed — skipped XLSX (pip install openpyxl)")


def cmd_status(con):
    total = con.execute("SELECT COUNT(*) FROM agencies").fetchone()[0]
    done = con.execute(
        "SELECT COUNT(*) FROM agencies WHERE detail_status='done'").fetchone()[0]
    err = con.execute(
        "SELECT COUNT(*) FROM agencies WHERE detail_status='error'").fetchone()[0]
    print(f"slugs collected : {total}")
    print(f"details done    : {done}")
    print(f"details errored : {err}")
    print(f"details pending : {total - done - err}")
    with_email = con.execute(
        "SELECT COUNT(*) FROM agencies WHERE email IS NOT NULL AND email != ''"
    ).fetchone()[0]
    print(f"with e-mail     : {with_email}")


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description="agenturmarkt.de scraper")
    ap.add_argument("command", choices=["scrape", "export", "status"])
    ap.add_argument("--db", default="agenturmarkt.db")
    ap.add_argument("--delay", type=float, default=1.0)
    ap.add_argument("--limit", type=int, default=None,
                    help="max agencies (sample run)")
    ap.add_argument("--category", help="category slug, e.g. seo-agentur")
    ap.add_argument("--city", help="city slug, e.g. berlin")
    ap.add_argument("--no-details", action="store_true")
    ap.add_argument("--out", help="export CSV path")
    args = ap.parse_args()

    con = db_connect(args.db)

    if args.command == "status":
        cmd_status(con)
    elif args.command == "export":
        cmd_export(con, args)
    else:
        try:
            stage_listing(con, args)
            if not args.no_details:
                stage_details(con, args)
        except KeyboardInterrupt:
            print("\ninterrupted — progress saved, rerun to resume.")
            sys.exit(1)
        cmd_status(con)
        cmd_export(con, args)


if __name__ == "__main__":
    main()
