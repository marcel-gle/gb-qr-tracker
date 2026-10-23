"""
Orchestrator der Kundenrecherche-Pipeline.

Nutzung:
    python run.py init
    python run.py collect [reddit|reddit-free|forum|linkedin|google|registry] [--config NAME]
    python run.py ingest reddit <datasetId|runId> [--filter] [--config NAME]
    python run.py stats
    python run.py reindex                      # Suchindex neu bauen
    python run.py analyze [--config NAME]

'ingest' liest ein bereits gelaufenes Apify-Dataset nach — nützlich, wenn der
Actor durchgelaufen ist, aber das Einlesen danach abbrach (kein zweiter Lauf,
keine zusätzlichen Credits).

--config wählt die Zielgruppen-Config aus config/ (Default: webdesigner).
Neue Zielgruppe = config/webdesigner.py kopieren & anpassen.

Empfohlener erster Lauf (gratis, ohne Apify):
    python run.py init
    python run.py collect reddit-free
    python run.py stats
    python run.py analyze
Dann skalieren mit Apify (Kommentare, Foren, LinkedIn):
    export APIFY_TOKEN=apify_api_...
    python run.py collect
"""

import sys
import importlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from storage import store


def load_config(name: str):
    return importlib.import_module(f"config.{name}")


def cmd_collect(cfg, only: str = None):
    from collectors import collect
    if only == "reddit-free":
        collect.collect_reddit_free(cfg.REDDIT_KEYWORDS, subreddits=cfg.SUBREDDITS)
    elif only == "registry":
        collect.collect_from_registry(getattr(cfg, "SOURCES", {}))
    elif only == "google":
        queries = getattr(cfg, "GOOGLE_QUERIES", None) or []
        keywords = getattr(cfg, "GOOGLE_KEYWORDS", None) or getattr(cfg, "FORUM_KEYWORDS", [])
        collect.collect_google_blogs(
            queries, keywords,
            search_actor_id=cfg.ACTORS.get("google", "apify/google-search-scraper"),
            crawl_actor_id=cfg.ACTORS.get("forum", "apify/website-content-crawler"),
        )
    else:
        if only in (None, "reddit"):
            collect.collect_reddit_apify(cfg.REDDIT_KEYWORDS, actor_id=cfg.ACTORS["reddit"],
                                         subreddits=cfg.SUBREDDITS)
        if only in (None, "forum"):
            # max_pages ist die GESAMTZAHL gecrawlter Seiten, nicht pro Start-URL.
            # Bei 13 Startpunkten kommt der Crawler mit dem Default (50) kaum
            # über die Seeds hinaus — dann bezahlst du Übersichtsseiten statt
            # Threads. 300 lässt ihn in die Diskussionen laufen.
            collect.collect_forums(cfg.FORUM_URLS, cfg.FORUM_KEYWORDS,
                                   actor_id=cfg.ACTORS["forum"], max_pages=300)
        if only in (None, "linkedin"):
            if cfg.ACTORS.get("linkedin"):
                collect.collect_linkedin(cfg.LINKEDIN_KEYWORDS,
                                         actor_id=cfg.ACTORS["linkedin"])
            else:
                print("[linkedin] übersprungen — kein Actor in der Config "
                      "eingetragen (siehe config, ACTORS['linkedin']).")
        if only is None and getattr(cfg, "GOOGLE_QUERIES", None):
            keywords = getattr(cfg, "GOOGLE_KEYWORDS", None) or getattr(cfg, "FORUM_KEYWORDS", [])
            collect.collect_google_blogs(
                cfg.GOOGLE_QUERIES, keywords,
                search_actor_id=cfg.ACTORS.get("google", "apify/google-search-scraper"),
                crawl_actor_id=cfg.ACTORS.get("forum", "apify/website-content-crawler"),
            )
        # Register-Quellen (Bewertungen, Social) laufen im vollen collect mit
        if only is None and getattr(cfg, "SOURCES", None):
            collect.collect_from_registry(cfg.SOURCES)
    print("\nStore-Stand:", store.stats())


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return

    config_name = "webdesigner"
    if "--config" in args:
        i = args.index("--config")
        config_name = args[i + 1]
        args = args[:i] + args[i + 2:]

    command = args[0]
    rest = args[1:]

    # Schema sicherstellen — idempotent (CREATE TABLE IF NOT EXISTS). Spart das
    # separate 'init' als Stolperfalle: ohne DB brach jeder Lauf erst NACH dem
    # (bezahlten) Actor-Lauf ab.
    if command in ("collect", "ingest", "stats", "analyze"):
        store.init_db()

    if command == "init":
        store.init_db()
        print("DB initialisiert.")
    elif command == "collect":
        cmd_collect(load_config(config_name), rest[0] if rest else None)
    elif command == "ingest":
        # python run.py ingest reddit <datasetId|runId> [--filter]
        from collectors import collect
        require_kw = "--filter" in rest
        rest = [a for a in rest if a != "--filter"]
        if len(rest) < 2 or rest[0] not in ("reddit", "linkedin"):
            print("Nutzung: python run.py ingest reddit|linkedin "
                  "<datasetId|runId> [--filter]")
            return
        cfg = load_config(config_name)
        if rest[0] == "reddit":
            collect.ingest_reddit_dataset(
                rest[1], cfg.REDDIT_KEYWORDS, require_keywords=require_kw,
                allzweck_subs=getattr(cfg, "ALLZWECK_SUBREDDITS", None),
                branchen_keywords=getattr(cfg, "BRANCHEN_KEYWORDS", None))
        else:
            collect.ingest_linkedin_dataset(rest[1])
        print("\nStore-Stand:", store.stats())
    elif command == "reindex":
        # Nötig, wenn von Hand Zeilen gelöscht/geändert wurden: der FTS-Index
        # hat nur einen INSERT-Trigger und läuft dann auseinander.
        with store.get_conn() as conn:
            conn.execute("INSERT INTO statements_fts(statements_fts) VALUES('rebuild')")
        print("Suchindex neu aufgebaut.")
    elif command == "stats":
        import json
        print(json.dumps(store.stats(), indent=2, ensure_ascii=False))
    elif command == "analyze":
        from synthesis import analyze as synth
        synth.analyze(load_config(config_name))
    elif command == "sources":
        # Quellen-Empfehlung: python run.py sources b2b lokal
        from collectors import suggest as sug
        flags = set(rest)
        base, picks = sug.suggest(
            b2b="b2b" in flags, lokal="lokal" in flags,
            jung="jung" in flags, hat_produkt="produkt" in flags)
        print("Flags:", ", ".join(flags) or "(keine — B2C, nicht lokal)")
        print("Basis (immer):", ", ".join(base))
        print("Empfohlene Register-Quellen:")
        print(sug.explain(picks))
        print("\n-> In die Config als SOURCES-Block übernehmen (Keys + Parameter).")
    else:
        print(f"Unbekannter Befehl: {command}")
        print(__doc__)


if __name__ == "__main__":
    main()
