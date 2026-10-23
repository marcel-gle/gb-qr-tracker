"""
Diagnose-Werkzeug: zeigt, was gescraped wurde und was unterwegs wegfällt.

Warum: zwischen Apify-Dataset und Store liegen mehrere Filter. Wenn am Ende
wenig ankommt, muss sichtbar sein WO es hängt — Feldnamen, Keyword-Filter,
Junk-Heuristik oder Dedup.

Wo liegt was:
  - Apify-Lauf (Rohdaten, Cloud): console.apify.com -> Actor runs -> Dataset
  - Lokale Kopie der Rohdaten:    data/raw/<datasetId>.jsonl   (via 'dump')
  - Store (gefiltert, SQLite):    storage/recherche.db

Nutzung:
    python explore.py dump <datasetId|runId>   # Rohdaten einmal lokal ziehen
    python explore.py peek   <file> [n]        # n Roh-Items ansehen
    python explore.py fields <file>            # welche Feldnamen kommen vor
    python explore.py funnel <file> [--config NAME]   # Filter-Trichter
    python explore.py rejected <file> keyword|junk [n] # Beispiele fürs Aussortierte
    python explore.py db [n]                   # was tatsächlich im Store liegt

Ohne <file> nach 'dump' genügt der Dateiname aus data/raw/.
"""

import sys
import json
import random
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent))
from storage import store
from collectors.collect import _first

RAW_DIR = Path(__file__).parent / "data" / "raw"


def _resolve(path_or_id: str) -> Path:
    """Nimmt einen Pfad ODER nur die Dataset-ID (dann aus data/raw/)."""
    p = Path(path_or_id)
    if p.exists():
        return p
    cand = RAW_DIR / f"{path_or_id}.jsonl"
    if cand.exists():
        return cand
    raise SystemExit(f"Datei nicht gefunden: {path_or_id}\n"
                     f"Vorhanden in {RAW_DIR}: "
                     f"{', '.join(f.name for f in RAW_DIR.glob('*.jsonl')) or '(nichts)'}\n"
                     f"Erst ziehen mit: python explore.py dump <datasetId>")


def _load(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _extract(item: dict) -> tuple[str, str, str, int]:
    """Genau die Felder, die auch der Collector nimmt — sonst lügt der Trichter."""
    text = _first(item, "body", "text", "selftext", "comment", "postText", default="") or ""
    title = _first(item, "title", "postTitle") or ""
    origin = _first(item, "subreddit", "communityName", "display_name") or "?"
    engagement = _first(item, "score", "upVotes", "numberOfVotes", default=0) or 0
    return text, title, origin, int(engagement)


# ---------------------------------------------------------------------------
def cmd_dump(dataset_id: str):
    """Zieht ein Apify-Dataset einmalig lokal — danach ist alles offline."""
    from collectors.collect import _client, _dataset_id
    client = _client()
    try:
        run = client.run(dataset_id).get()
        if run is not None:
            print(f"[dump] {dataset_id} ist eine Run-ID -> Dataset "
                  f"{_dataset_id(run)}")
            dataset_id = _dataset_id(run)
    except Exception:
        pass

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out = RAW_DIR / f"{dataset_id}.jsonl"
    n = 0
    with out.open("w", encoding="utf-8") as f:
        for item in client.dataset(dataset_id).iterate_items():
            f.write(json.dumps(item, ensure_ascii=False, default=str) + "\n")
            n += 1
            if n % 2000 == 0:
                print(f"[dump] {n} Items ...")
    size_mb = out.stat().st_size / 1_000_000
    print(f"[dump] {n} Items -> {out} ({size_mb:.1f} MB)")
    print(f"[dump] Weiter mit: python explore.py funnel {dataset_id}")


def cmd_peek(path: Path, n: int = 3):
    items = _load(path)
    print(f"{len(items)} Items in {path.name} — {n} zufällige komplett:\n")
    for item in random.sample(items, min(n, len(items))):
        print(json.dumps(item, indent=2, ensure_ascii=False)[:3000])
        print("-" * 70)


def cmd_fields(path: Path):
    """
    Welche Feldnamen liefert der Actor wirklich — und wie oft sind sie befüllt?
    Damit prüfst du die _first(...)-Annahmen im Collector gegen die Realität.
    """
    items = _load(path)
    present, filled = Counter(), Counter()
    for item in items:
        for k, v in item.items():
            present[k] += 1
            if v not in (None, "", [], {}):
                filled[k] += 1
    print(f"{len(items)} Items, {len(present)} verschiedene Felder\n")
    print(f"{'Feld':<28} {'vorhanden':>10} {'befüllt':>10}")
    for k, c in present.most_common():
        print(f"{k:<28} {c:>10} {filled[k]:>10}")

    used = ["body", "text", "selftext", "comment", "postText",
            "title", "postTitle", "subreddit", "communityName", "display_name",
            "score", "upVotes", "numberOfVotes"]
    missing = [k for k in used if k not in present]
    print("\nVom Collector gesucht, im Dataset NICHT vorhanden:",
          ", ".join(missing) or "(alle vorhanden)")


def cmd_funnel(path: Path, config_name: str = "webdesigner"):
    """Der Kern: wo genau bleiben die Items hängen."""
    import importlib
    cfg = importlib.import_module(f"config.{config_name}")
    keywords = cfg.REDDIT_KEYWORDS
    kws_lower = [k.lower() for k in keywords]

    items = _load(path)
    total = len(items)
    no_text = 0
    kw_hits = Counter()
    kw_pass = 0
    junk_reasons = Counter()
    passed = []
    hashes = set()
    dupes = 0

    # Vergleichsmodi für den Keyword-Filter (nur gezählt, nicht angewendet):
    # phrase = ganze Phrase als Substring (das macht der Collector aktuell)
    # alle_woerter = alle Wörter der Phrase kommen vor, Reihenfolge egal
    # ein_wort = irgendein Wort der Phrase kommt vor
    kw_words = [[w for w in k.lower().split()] for k in kws_lower]
    mode_counts = Counter()

    for item in items:
        text, title, origin, _eng = _extract(item)
        norm = " ".join((text or "").split())
        if not norm:
            no_text += 1
            continue
        haystack = f"{title} {text}".lower()

        hit = [k for k in kws_lower if k in haystack]
        if hit:
            mode_counts["phrase"] += 1
        if any(all(w in haystack for w in ws) for ws in kw_words):
            mode_counts["alle_woerter"] += 1
        if any(any(w in haystack for w in ws) for ws in kw_words):
            mode_counts["ein_wort"] += 1

        for k in hit:
            kw_hits[k] += 1
        if hit:
            kw_pass += 1

        # Der Ingest filtert standardmäßig NICHT auf Keywords — der Trichter
        # bildet denselben Pfad ab. Die Keyword-Zahlen bleiben als Diagnose
        # stehen (sie zeigen, was der alte --filter-Modus durchgelassen hätte).
        reason = store.junk_reason(norm)
        if reason:
            junk_reasons[reason] += 1
            continue
        h = store._content_hash(norm)
        if h in hashes:
            dupes += 1
            continue
        hashes.add(h)
        passed.append((origin, norm))

    def pct(x):
        return f"{x / total * 100:5.1f}%" if total else "    -"

    print(f"\nFILTER-TRICHTER — {path.name}, Config '{config_name}'\n" + "=" * 62)
    with_text = total - no_text
    print(f"{'Dataset-Items gesamt':<40}{total:>8}  {pct(total)}")
    print(f"{'  davon ohne Text (nur Metadaten)':<40}{no_text:>8}  {pct(no_text)}")
    print(f"{'  -> mit Text':<40}{with_text:>8}  {pct(with_text)}")
    print(f"{'  -> Junk-Filter bestanden':<40}"
          f"{with_text - sum(junk_reasons.values()):>8}  "
          f"{pct(with_text - sum(junk_reasons.values()))}")
    print(f"{'  -> nach Dedup (= im Store)':<40}{len(passed):>8}  {pct(len(passed))}")

    print(f"\nZum Vergleich: mit --filter (Keyword-Modus) kämen {kw_pass} Items "
          f"überhaupt bis zum Junk-Filter. Treffer je Keyword:")
    for k in kws_lower:
        print(f"  {kw_hits.get(k, 0):>6}  {k}")

    print("\nWie viele Items je Matching-Modus durchkämen (nur mit --filter relevant):")
    for mode in ("phrase", "alle_woerter", "ein_wort"):
        c = mode_counts[mode]
        marker = "  <- was --filter benutzt" if mode == "phrase" else ""
        print(f"  {c:>6}  {pct(c)}  {mode}{marker}")

    if junk_reasons:
        print("\nWarum der JUNK-Filter aussortiert hat:")
        for reason, c in junk_reasons.most_common():
            print(f"  {c:>6}  {reason}")
    if dupes:
        print(f"\nDuplikate (gleicher Text) im Dataset: {dupes}")

    if passed:
        print("\nQuellen der durchgekommenen Aussagen:")
        for origin, c in Counter(o for o, _ in passed).most_common():
            print(f"  {c:>6}  r/{origin}")


def cmd_rejected(path: Path, stage: str, n: int = 5, config_name: str = "webdesigner"):
    """Beispiele für das, was rausfliegt — die wichtigste Plausibilitätsprüfung."""
    import importlib
    cfg = importlib.import_module(f"config.{config_name}")
    kws_lower = [k.lower() for k in cfg.REDDIT_KEYWORDS]

    items = _load(path)
    out = []
    for item in items:
        text, title, origin, eng = _extract(item)
        norm = " ".join((text or "").split())
        if not norm:
            continue
        haystack = f"{title} {text}".lower()
        hit = any(k in haystack for k in kws_lower)
        if stage == "keyword" and not hit:
            out.append((f"kein Keyword-Treffer", origin, title, norm))
        elif stage == "junk" and hit:
            reason = store.junk_reason(norm)
            if reason:
                out.append((reason, origin, title, norm))

    print(f"{len(out)} Items in Stufe '{stage}' aussortiert — {n} Beispiele:\n")
    for reason, origin, title, text in random.sample(out, min(n, len(out))):
        print(f"[{reason}] r/{origin}")
        if title:
            print(f"  Titel: {title[:120]}")
        print(f"  Text:  {text[:400]}")
        print("-" * 70)


def cmd_db(n: int = 10):
    """Was tatsächlich im Store gelandet ist."""
    print(f"DB-Datei: {store.DB_PATH}")
    print(f"Größe:    {store.DB_PATH.stat().st_size / 1024:.0f} KB\n")
    print(json.dumps(store.stats(), indent=2, ensure_ascii=False))
    with store.get_conn() as conn:
        rows = conn.execute(
            "SELECT source, origin, lang, engagement, context, text "
            "FROM statements ORDER BY engagement DESC LIMIT ?", (n,)
        ).fetchall()
    print(f"\nTop {len(rows)} nach Engagement:\n")
    for r in rows:
        print(f"[{r['source']}/{r['origin']} {r['lang']} ↑{r['engagement']}] "
              f"{r['context'] or ''}"[:110])
        print(f"  {r['text'][:300]}")
        print("-" * 70)


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

    cmd, rest = args[0], args[1:]
    if cmd == "dump":
        if not rest:
            raise SystemExit("Nutzung: python explore.py dump <datasetId|runId>")
        cmd_dump(rest[0])
    elif cmd == "peek":
        cmd_peek(_resolve(rest[0]), int(rest[1]) if len(rest) > 1 else 3)
    elif cmd == "fields":
        cmd_fields(_resolve(rest[0]))
    elif cmd == "funnel":
        cmd_funnel(_resolve(rest[0]), config_name)
    elif cmd == "rejected":
        stage = rest[1] if len(rest) > 1 else "keyword"
        if stage not in ("keyword", "junk"):
            raise SystemExit("stage muss 'keyword' oder 'junk' sein")
        cmd_rejected(_resolve(rest[0]), stage,
                     int(rest[2]) if len(rest) > 2 else 5, config_name)
    elif cmd == "db":
        store.init_db()
        cmd_db(int(rest[0]) if rest else 10)
    else:
        print(f"Unbekannter Befehl: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
