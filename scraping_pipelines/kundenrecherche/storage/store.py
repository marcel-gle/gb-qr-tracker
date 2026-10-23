"""
Storage-Schicht der Kundenrecherche-Pipeline.

Design-Prinzipien:
- GDPR-safe: Wir speichern AUSSAGEN, nicht Personen. Kein Username, kein
  Profil-Link wird persistiert.
- Ein gemeinsamer Speicher für alle Collector.
- FTS5-Volltextsuche mit Sanitization + Präfix-Matching (wichtig für
  Deutsch: 'Preis*' findet auch 'Preise', 'Preisen', 'Preiskampf').
- Stratifizierte Auswahl für die Synthese, damit keine Quelle dominiert.
"""

import sqlite3
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from contextlib import contextmanager

DB_PATH = Path(__file__).parent / "recherche.db"

# Textgrenzen: zu kurz = Rauschen, zu lang = ganze Seite statt Aussage.
# Die 2000 stammen aus der Reddit-/Foren-Welt (Median dort 156 bzw. 122 Zeichen).
# LinkedIn-Posts sind Langtext — Median 1051 — und 15 % lagen über der Grenze.
# Ausgerechnet die ausführlichsten Beiträge fielen also weg, deshalb pro Quelle.
MIN_LEN, MAX_LEN = 25, 2000
MAX_LEN_BY_SOURCE = {
    "linkedin": 4000,
    "forum": 3000,
    "blog": 3000,
}

# Heuristik für Foren-/Seitenmüll (Navigation, Cookie-Banner, Signaturen)
JUNK_PATTERNS = re.compile(
    r"(cookie|impressum|datenschutz|newsletter|anmelden|registrieren|"
    r"alle rechte vorbehalten|zum inhalt springen|advertisement)",
    re.IGNORECASE,
)

# Plattform-Boilerplate ohne Aussagewert. "submitted by /u/..." trägt zusätzlich
# einen Usernamen — das verletzt die GDPR-Regel des Projekts (nur Aussagen,
# keine Autoren) und muss deshalb zwingend aussortiert werden.
BOILERPLATE_PATTERNS = re.compile(
    r"(submitted by\s*/?u/|\[link\]\s*\[comments\]|^\[(removed|deleted)\]$"
    # Foren-Byline: "Beitrag imwebsein » 10.02.2015, 11:09" — enthält den
    # Usernamen des Autors, gleiche GDPR-Lage wie "submitted by /u/...".
    r"|^Beitrag\s+\S+\s*»"
    # Metatext der dasauge-Suchseiten, die wir als Einstieg crawlen
    r"|Gesucht wurde nach|dasauge® Foren"
    r")",
    re.IGNORECASE,
)

# Autoren-Handles, die in einer sonst brauchbaren Aussage vorkommen (z.B. eine
# Antwort, die jemanden erwähnt). Die Aussage bleibt, das Handle wird ersetzt.
AUTHOR_MENTION = re.compile(r"(?<!\w)/?u/[A-Za-z0-9_-]{3,}", re.IGNORECASE)


def redact_authors(text: str) -> str:
    """Ersetzt Reddit-Handles durch einen Platzhalter — GDPR: keine Autoren."""
    return AUTHOR_MENTION.sub("/u/[entfernt]", text)


# WER SPRICHT? Entscheidend für die Synthese: Eine Google-Maps-Bewertung stammt
# von einem KUNDEN einer Agentur, nicht von der Zielgruppe selbst. Ohne diese
# Unterscheidung hält das Modell Kundenbeschwerden für Aussagen der Zielgruppe
# und leitet daraus deren "Hauptproblem" ab.
PERSPECTIVE_BY_SOURCE = {
    "reddit": "zielgruppe",
    "forum": "zielgruppe",
    # LinkedIn ist KEINE Schmerzäußerung. Messung an 80 Posts: 59 trugen offene
    # Werbemuster, genau 1 war eine echte Ich-Aussage über ein eigenes Problem.
    # Was dort steht, ist die Verkaufsstimme der Zielgruppe — oft adressiert an
    # deren Kunden. Als Schmerz gelesen kehrt es die Analyse um.
    "linkedin": "marketing",
    # Google-Blogs/Fachartikel: redaktionell oder salesy, selten rohe Ich-Aussagen.
    # Nützlich für Problemstruktur und Markt-Narrative, nicht als Copy-Rohstoff.
    "blog": "fachpresse",
    "gmaps": "kunde",
    "trustpilot": "kunde",
    "amazon": "kunde",
    "youtube": "kunde",
    "kununu": "mitarbeiter",
    "tiktok": "unbekannt",
    "instagram": "unbekannt",
}


def perspective_for(source: str) -> str:
    return PERSPECTIVE_BY_SOURCE.get(source, "unbekannt")


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.strip().lower().encode("utf-8")).hexdigest()[:16]


def guess_lang(text: str) -> str:
    """Einfache DE/EN-Heuristik über Funktionswörter — reicht für Filtern/Statistik."""
    de = len(re.findall(r"\b(und|nicht|aber|ich|das|die|der|ein|mit|für|auch)\b", text.lower()))
    en = len(re.findall(r"\b(and|not|but|the|with|for|that|this|have|just)\b", text.lower()))
    return "de" if de >= en else "en"


@contextmanager
def get_conn(db_path: Path = DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(db_path: Path = DB_PATH):
    with get_conn(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS statements (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                content_hash TEXT UNIQUE NOT NULL,
                source       TEXT NOT NULL,
                origin       TEXT,
                lang         TEXT,
                text         TEXT NOT NULL,
                context      TEXT,
                engagement   INTEGER DEFAULT 0,
                collected_at TEXT NOT NULL,
                posted_at    TEXT,
                perspektive  TEXT,
                query        TEXT
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS statements_fts
                USING fts5(text, context, content='statements', content_rowid='id');

            CREATE TRIGGER IF NOT EXISTS statements_ai AFTER INSERT ON statements BEGIN
                INSERT INTO statements_fts(rowid, text, context)
                VALUES (new.id, new.text, new.context);
            END;
            """
        )
        # Migration für DBs, die vor posted_at angelegt wurden. ADD COLUMN ist
        # in SQLite verlustfrei — kein Neuaufbau, bestehende Zeilen bekommen NULL.
        cols = {r[1] for r in conn.execute("PRAGMA table_info(statements)")}
        if "posted_at" not in cols:
            conn.execute("ALTER TABLE statements ADD COLUMN posted_at TEXT")
        if "perspektive" not in cols:
            conn.execute("ALTER TABLE statements ADD COLUMN perspektive TEXT")
            # Bestand nachziehen: Perspektive ergibt sich aus der Quelle.
            for src, persp in PERSPECTIVE_BY_SOURCE.items():
                conn.execute("UPDATE statements SET perspektive=? WHERE source=?",
                             (persp, src))
            conn.execute("UPDATE statements SET perspektive='unbekannt' "
                         "WHERE perspektive IS NULL")


def junk_reason(text: str, source: str = None) -> str | None:
    """
    Warum eine Aussage aussortiert wird — oder None, wenn sie durchkommt.
    Einzige Wahrheit für den Filter; is_junk() ist nur die Ja/Nein-Sicht.
    Der Grund wird von explore.py für den Trichter ausgewertet.
    """
    max_len = MAX_LEN_BY_SOURCE.get(source, MAX_LEN)
    if len(text) < MIN_LEN:
        return f"zu kurz (<{MIN_LEN} Zeichen)"
    if len(text) > max_len:
        return f"zu lang (>{max_len} Zeichen)"
    m = BOILERPLATE_PATTERNS.search(text)
    if m:
        return f"Plattform-Boilerplate {m.group(0).lower()!r}"
    m = JUNK_PATTERNS.search(text)
    if m:
        return f"Junk-Muster {m.group(0).lower()!r}"
    # Zeilen die überwiegend aus Links/Sonderzeichen bestehen
    alpha_ratio = sum(c.isalpha() or c.isspace() for c in text) / max(len(text), 1)
    if alpha_ratio < 0.7:
        return f"Buchstabenanteil {alpha_ratio:.0%} (<70%)"
    return None


def is_junk(text: str, source: str = None) -> bool:
    return junk_reason(text, source) is not None


def save_statement(
    source: str,
    text: str,
    origin: str = None,
    lang: str = None,
    context: str = None,
    engagement: int = 0,
    query: str = None,
    posted_at: str = None,
    db_path: Path = DB_PATH,
) -> bool:
    """Speichert eine Aussage. True wenn neu, False bei Duplikat/Junk."""
    # Scraper liefern gelegentlich verschachtelte Strukturen statt Strings.
    # Nicht abstürzen — das würde einen bezahlten Actor-Lauf komplett verlieren.
    # Der Collector ist für die Extraktion zuständig; hier nur absichern.
    if text is not None and not isinstance(text, str):
        return False
    text = re.sub(r"\s+", " ", (text or "")).strip()
    if is_junk(text, source):
        return False
    # Redaktion VOR dem Hash, damit die Dedup-Erkennung konsistent bleibt.
    text = redact_authors(text)

    lang = lang or guess_lang(text)
    h = _content_hash(text)
    with get_conn(db_path) as conn:
        try:
            conn.execute(
                """INSERT INTO statements
                   (content_hash, source, origin, lang, text, context, engagement,
                    collected_at, posted_at, perspektive, query)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (h, source, origin, lang, text, context, engagement,
                 datetime.now(timezone.utc).isoformat(), posted_at,
                 perspective_for(source), query),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def _fts_query(term: str) -> str:
    """
    Baut aus einem freien Suchbegriff eine sichere FTS5-Query:
    - Tokens werden gequotet (keine Syntax-Crashes durch -, ', ?)
    - Tokens >= 4 Zeichen bekommen Präfix-Matching: "Preis"* findet
      Preise/Preisen/Preiskampf. Entscheidend für deutsche Flexion/Komposita.
    """
    tokens = re.findall(r"\w+", term, re.UNICODE)
    if not tokens:
        return '""'
    parts = []
    for t in tokens:
        if len(t) >= 4:
            parts.append(f'"{t}"*')
        else:
            parts.append(f'"{t}"')
    return " ".join(parts)  # implizites AND


def search(term: str, limit: int = 50, db_path: Path = DB_PATH) -> list[dict]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """
            SELECT s.text, s.source, s.origin, s.context, s.engagement, s.lang,
                   s.posted_at, s.perspektive
            FROM statements_fts f
            JOIN statements s ON s.id = f.rowid
            WHERE statements_fts MATCH ?
            ORDER BY rank, s.engagement DESC, s.posted_at DESC
            LIMIT ?
            """,
            (_fts_query(term), limit),
        ).fetchall()
        return [dict(r) for r in rows]


def stats(db_path: Path = DB_PATH) -> dict:
    with get_conn(db_path) as conn:
        total = conn.execute("SELECT COUNT(*) FROM statements").fetchone()[0]
        by_source = conn.execute(
            "SELECT source, COUNT(*) c FROM statements GROUP BY source"
        ).fetchall()
        by_lang = conn.execute(
            "SELECT lang, COUNT(*) c FROM statements GROUP BY lang"
        ).fetchall()
        return {
            "total": total,
            "by_source": {r["source"]: r["c"] for r in by_source},
            "by_lang": {r["lang"]: r["c"] for r in by_lang},
        }


def export_for_synthesis(
    terms: list[str],
    per_term: int = 30,
    max_per_bucket: int = 75,
    db_path: Path = DB_PATH,
) -> list[dict]:
    """
    Zieht die relevantesten Aussagen — STRATIFIZIERT pro (Quelle, Sprache).

    Warum auch nach Sprache: Das Endprodukt ist deutschsprachige Copy. Eine
    Stratifizierung nur nach Quelle deckelt bei einer einzigen Quelle das
    Gesamtergebnis und überlässt die Sprachmischung dem Zufall — bei einem
    Korpus, der zu 55% englisch ist, kann Deutsch dabei untergehen.
    """
    seen = set()
    by_bucket: dict[tuple[str, str], list] = {}
    for term in terms:
        for row in search(term, limit=per_term, db_path=db_path):
            key = _content_hash(row["text"])
            if key in seen:
                continue
            seen.add(key)
            bucket = (row["source"], row.get("lang") or "?")
            by_bucket.setdefault(bucket, []).append(row)

    # Sortierung: Engagement zuerst, dann Aktualität. Wenn eine Quelle gar kein
    # Engagement liefert (Reddit-Actor gibt aktuell überall 0 zurück), entscheidet
    # allein das Datum — sonst wäre die Reihenfolge willkürlich.
    for rows in by_bucket.values():
        rows.sort(key=lambda r: (r.get("engagement") or 0, r.get("posted_at") or ""),
                  reverse=True)
        del rows[max_per_bucket:]

    # Reihum aus allen Töpfen, damit ein Abschneiden weiter oben (max_statements)
    # nicht eine ganze Sprache oder Quelle wegschneidet.
    out = []
    if by_bucket:
        for i in range(max(len(r) for r in by_bucket.values())):
            for rows in by_bucket.values():
                if i < len(rows):
                    out.append(rows[i])
    return out


if __name__ == "__main__":
    init_db()
    print("DB initialisiert:", DB_PATH)
    print(json.dumps(stats(), indent=2, ensure_ascii=False))
