"""
Quellen-Empfehlung für eine neue Zielgruppe.

Beantwortet ein paar Fragen und schlägt aus dem Register die sinnvollsten
Quellen vor — als Startpunkt für den SOURCES-Block einer neuen Config.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from collectors.registry import REGISTRY


def suggest(b2b: bool, lokal: bool, jung: bool, hat_produkt: bool):
    """
    b2b:         B2B (True) oder B2C (False)?
    lokal:       lokale/ortsgebundene Dienstleistung (Gastro, Handwerk, Agentur)?
    jung:        junge Zielgruppe / Lifestyle / visuell?
    hat_produkt: physisches Produkt oder SaaS mit Bewertungen?
    Gibt (basis, register_quellen) zurück.
    """
    picks = []
    base = ["reddit-free / reddit", "forum", "google"]   # fast immer Basis

    if lokal:
        picks.append("google_maps_reviews")
    if b2b:
        picks += ["kununu", "youtube_comments"]
    else:
        picks.append("youtube_comments")
        if jung:
            picks += ["tiktok", "instagram"]
    if hat_produkt:
        picks += ["amazon_reviews", "trustpilot_reviews"]

    seen, out = set(), []
    for p in picks:
        if p not in seen:
            seen.add(p); out.append(p)
    return base, out


def explain(keys):
    lines = []
    for k in keys:
        if k in REGISTRY:
            r = REGISTRY[k]
            lines.append(f"  {k} [{r['ehrlichkeit']}] — {r['typisch_fuer']}")
    return "\n".join(lines)


if __name__ == "__main__":
    print("=== Beispiel: Webdesigner/Agenturen (B2B, lokal-nah) ===")
    base, picks = suggest(b2b=True, lokal=True, jung=False, hat_produkt=False)
    print("Basis (immer):", ", ".join(base))
    print("Register-Quellen:")
    print(explain(picks))
    print("\n=== Beispiel: Beauty-Coaching B2C (jung, mit Produkt) ===")
    base, picks = suggest(b2b=False, lokal=False, jung=True, hat_produkt=True)
    print("Basis (immer):", ", ".join(base))
    print("Register-Quellen:")
    print(explain(picks))
