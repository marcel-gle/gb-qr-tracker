"""
Synthese-Schicht — Claude als Analyst, in ZWEI PÄSSEN.

Warum zwei Pässe? Zeigt man dem Modell die These VOR der Analyse, findet es
bevorzugt Bestätigung (Konfirmationsbias). Deshalb:

  PASS 1 (offenes Codieren): Claude sieht NUR die Aussagen — keine These.
          Aufgabe: Welche Probleme, Emotionen und Muster tauchen auf,
          rein datengetrieben? Mit Häufigkeits-Einschätzung.

  PASS 2 (These-Abgleich): Claude bekommt die Pass-1-Ergebnisse + die These
          und vergleicht: Deckt sich das dominante Muster mit der These?
          Erst hier entstehen Irrglauben-Analyse und Copy-Ansätze.

ZWEI MODI je nach Config:
  - THESIS ist gesetzt  -> KONFIRMATORISCH: Pass 2 prüft deine These gegen
    die Daten (bestätigt / teilweise / nicht bestätigt).
  - THESIS = None       -> EXPLORATIV: Pass 2 leitet die wahrscheinlichste
    Hauptproblem-Hypothese aus den Daten selbst her. Ideal, wenn du am
    Anfang stehst und noch KEINE These hast — die Daten schlagen sie dir vor.

In beiden Fällen entstehen Hauptproblem, Irrglauben, Einwände und Copy-Ansätze.

Zielgruppe, These und Suchbegriffe kommen aus der Config — dadurch ist die
Pipeline für beliebige Zielgruppen wiederverwendbar.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from storage import store

# ── LLM-Backend: Cloud (Anthropic) oder lokal (LM Studio) ──────────────
# Umschalten über Umgebungsvariable LLM_BACKEND:
#   LLM_BACKEND=cloud  (Default) -> Anthropic-API, braucht ANTHROPIC_API_KEY
#   LLM_BACKEND=local            -> LM Studio, lokal, kein Key, keine Cloud
#
# LM Studio: App starten, ein Modell laden, "Local Server" starten (Reiter
# "Developer" / "Local Server"). Standard-URL ist http://localhost:1234/v1.
# LOCAL_MODEL = der in LM Studio geladene Identifier (siehe Server-Log).
BACKEND = os.environ.get("LLM_BACKEND", "cloud").lower()

CLOUD_MODEL = "claude-opus-4-8"
LOCAL_MODEL = os.environ.get("LOCAL_MODEL", "local-model")
LOCAL_BASE_URL = os.environ.get("LOCAL_BASE_URL", "http://localhost:1234/v1")

PASS1_PROMPT = """Du bist ein qualitativer Marktforscher. Unten stehen echte, \
öffentliche Aussagen rund um diese Zielgruppe: {audience}

WICHTIG — das Material ist zweifach gegliedert, und beide Gliederungen musst du \
beachten:

1. NACH PERSPEKTIVE: Wer spricht, und in welchem Modus? Die Zielgruppe sind die \
Webdesigner und Agenturen selbst. Aussagen ihrer Kunden oder Angestellten sind \
Kontext, aber NICHT die Stimme der Zielgruppe. Ein eigener Block enthält die \
Verkaufsstimme der Zielgruppe (Werbeposts) — dort beschreibt sie den Schmerz \
IHRER Kunden, um etwas zu verkaufen; das ist nicht ihr eigener Schmerz. Nenne \
beim Zitieren immer die Perspektive.

2. NACH SPRACHE — das ist eine bewusste Arbeitsteilung, keine Zufälligkeit: \
Deutschsprachige Agenturinhaber sind öffentlich kaum auffindbar, im englischen \
Material dagegen gut. Deshalb gilt: Die PROBLEMSTRUKTUR (Cluster, Emotionen, \
gescheiterte Lösungsversuche) leitest du vor allem aus dem ENGLISCHEN Block ab. \
Die COPY-SPRACHE — jedes wörtliche Zitat, das später in einem deutschen \
Werbebrief stehen könnte — nimmst du ausschließlich aus dem DEUTSCHEN Block. \
Übersetze niemals ein englisches Zitat, um es als deutsche Copy auszugeben. \
Wenn ein wichtiges Problem im deutschen Material keine Entsprechung findet, \
benenne diese Lücke ausdrücklich — sie ist ein Rechercheergebnis, kein Makel.

Deine Aufgabe ist OFFENES CODIEREN — rein datengetrieben, ohne Vorannahmen:

1. PROBLEM-CLUSTER: Welche Probleme/Schmerzpunkte tauchen auf? Gruppiere sie \
und schätze pro Cluster die relative Häufigkeit (wie viele der Aussagen \
zahlen darauf ein). Sortiere nach Dominanz.
2. EMOTIONEN: Welche Emotionen schwingen mit (Frust, Angst, Resignation, \
Stolz, ...)? Woran machst du das fest?
3. WÖRTLICHE FORMULIERUNGEN: Die 10-15 aussagekräftigsten Original-Zitate, \
in denen die Zielgruppe ihren Schmerz in eigenen Worten beschreibt. \
Wörtlich übernehmen, Quelle in Klammern (z.B. reddit/freelance). Keine Personen. \
DIESE LISTE IST DAS COPY-ROHMATERIAL und stammt daher AUSSCHLIESSLICH aus dem \
deutschen Block. Nimm lieber ein schwächeres deutsches Zitat als ein starkes \
englisches. Reicht das deutsche Material für 10 Zitate nicht, liefere weniger \
und sag warum. Englische Zitate gehören in eine SEPARATE Liste "Belege aus dem \
englischen Material", klar getrennt und als [en] markiert.
4. LÖSUNGSVERSUCHE: Was hat die Zielgruppe laut den Aussagen schon versucht, \
und woran scheitert es?
5. DATENLAGE: Wie belastbar ist das Material (Menge, Quellenvielfalt, \
Sprachen)? Wo sind blinde Flecken?

Erfinde nichts. Wenn ein Muster nur 2-3x vorkommt, sag das dazu.

--- AUSSAGEN ---
{statements}"""

PASS2_PROMPT = """Du bist Positionierungs-Stratege für B2B-Direktmarketing im \
DACH-Raum (Methodik: klare Hauptproblem-Formulierung + zentraler Irrglauben \
der Zielgruppe als Copy-Fundament).

ZIELGRUPPE: {audience}

Ein Analyst hat echte Zielgruppen-Aussagen offen codiert (ohne Hypothesen zu \
kennen). Hier sein Bericht:

--- ANALYSE (PASS 1) ---
{pass1_result}
--- ENDE ANALYSE ---

Der Auftraggeber hat folgende ARBEITSTHESE über das Hauptproblem:
{thesis}

Deine Aufgaben:

1. THESE-ABGLEICH: Deckt sich das laut Daten DOMINANTE Problem mit der These? \
Urteil: bestätigt / teilweise bestätigt / nicht bestätigt — mit Begründung \
aus den Clustern. Sei ehrlich: Wenn die Daten ein anderes Problem als \
dominanter zeigen, sag das klar und benenne es.
2. HAUPTPROBLEM-FORMULIERUNG: Formuliere das Hauptproblem in EINEM Satz, in \
der Sprache der Zielgruppe (nutze die wörtlichen Formulierungen aus Pass 1).
3. ZENTRALER IRRGLAUBEN: Woran glaubt die Zielgruppe fälschlich? Leite das \
aus den gescheiterten Lösungsversuchen (Pass 1, Punkt 4) ab.
4. EINWAND-LANDKARTE: Die 5 wichtigsten Einwände/Ängste gegen ein Angebot, \
das am Hauptproblem ansetzt.
5. COPY-ANSÄTZE: 3 konkrete Winkel für Erstkontakt/Werbebrief — jeweils mit \
Hook in Zielgruppen-Sprache, angesprochenem Schmerz und gebrochenem Irrglauben.
6. OFFENE FRAGEN: Was sollte durch echte Gespräche/Interviews validiert \
werden, bevor viel Budget auf diese Positionierung geht?"""


PASS2_EXPLORATIV_PROMPT = """Du bist Positionierungs-Stratege für B2B-Direktmarketing im \
DACH-Raum (Methodik: klare Hauptproblem-Formulierung + zentraler Irrglauben \
der Zielgruppe als Copy-Fundament).

ZIELGRUPPE: {audience}

Ein Analyst hat echte Zielgruppen-Aussagen offen codiert (ohne Vorannahmen). \
Hier sein Bericht:

--- ANALYSE (PASS 1) ---
{pass1_result}
--- ENDE ANALYSE ---

Es gibt bewusst KEINE vorgegebene These. Deine Aufgabe ist EXPLORATIV: Leite \
die Positionierung aus den Daten selbst her.

1. HAUPTPROBLEM-HYPOTHESE: Was ist laut den Daten das dominante Problem der \
Zielgruppe? Formuliere es als klare Hypothese in EINEM Satz, in der Sprache \
der Zielgruppe. Stütze dich auf die Häufigkeit der Cluster aus Pass 1.
2. ALTERNATIVE HYPOTHESEN: Nenne 1-2 plausible Gegen-Hypothesen (andere \
Probleme, die auch dominant sein könnten) und sag, warum du sie für weniger \
wahrscheinlich hältst. So bleibt die Analyse ehrlich statt vorschnell.
3. ZENTRALER IRRGLAUBEN: Woran glaubt die Zielgruppe fälschlich? Leite das aus \
den gescheiterten Lösungsversuchen (Pass 1, Punkt 4) ab.
4. EINWAND-LANDKARTE: Die 5 wichtigsten Einwände/Ängste gegen ein Angebot, das \
am Hauptproblem ansetzt.
5. COPY-ANSÄTZE: 3 konkrete Winkel für Erstkontakt/Werbebrief — jeweils mit \
Hook in Zielgruppen-Sprache, angesprochenem Schmerz und gebrochenem Irrglauben.
6. VALIDIERUNG: Was solltest du durch echte Gespräche/Interviews prüfen, bevor \
viel Budget auf die Haupt-Hypothese geht? Konkrete Fragen."""


# Wer spricht in welchem Block — das Modell muss das wissen, sonst hält es
# Kundenbeschwerden für Aussagen der Zielgruppe.
PERSPEKTIV_LABELS = {
    "zielgruppe": ("AUSSAGEN DER ZIELGRUPPE SELBST (Webdesigner, Agenturen, "
                   "Freelancer). Hauptproblem und Irrglauben NUR hieraus ableiten. "
                   "Die Aufteilung nach Sprache innerhalb dieses Blocks beachten."),
    "kunde": ("AUSSAGEN VON KUNDEN DIESER ZIELGRUPPE — das sind NICHT die "
              "Webdesigner, sondern Leute, die eine Agentur beauftragt haben. "
              "Nur für die Einwand-Landkarte verwenden, niemals als Sprache "
              "der Zielgruppe zitieren."),
    "mitarbeiter": ("AUSSAGEN VON ANGESTELLTEN IN AGENTUREN — Innensicht auf den "
                    "Arbeitsalltag, aber nicht die Perspektive der Inhaber."),
    "fachpresse": ("FACHARTIKEL / BLOGS (Google-Suche) — redaktionell oder "
                   "salesy, selten ungefilterte Ich-Aussagen. Nutze für "
                   "Problemstruktur und Markt-Narrative, wenn konkrete Praxis "
                   "beschrieben wird. NICHT als Copy-Rohmaterial und nicht als "
                   "alleinigen Beleg für das Hauptproblem der Zielgruppe."),
    "marketing": ("VERKAUFSSTIMME DER ZIELGRUPPE (LinkedIn-Werbeposts) — die "
                  "Zielgruppe spricht hier, aber im Verkaufsmodus und meist an "
                  "IHRE Kunden gerichtet. Das ist KEIN Schmerz und keine "
                  "Copy-Vorlage. Nutze diesen Block ausschließlich für zwei "
                  "Dinge: (a) womit der Markt gesättigt ist, welche Versprechen "
                  "also abgenutzt sind und in einem Werbebrief nicht mehr "
                  "wirken; (b) welchen Irrglauben die Zielgruppe über die "
                  "eigene Außenwirkung pflegt. Niemals als Beleg für ein "
                  "Problem der Zielgruppe heranziehen."),
    "unbekannt": "AUSSAGEN MIT UNKLARER HERKUNFT — zurückhaltend behandeln.",
}
_PERSP_ORDER = ["zielgruppe", "kunde", "mitarbeiter", "fachpresse", "marketing", "unbekannt"]


def build_context(search_terms: list[str], max_statements: int = 200) -> tuple[str, int, dict]:
    """
    Baut den Aussagen-Block, GETRENNT nach Perspektive.
    Gibt (Text, Anzahl, Verteilung nach Sprache und Perspektive) zurück.
    """
    from collections import Counter
    rows = store.export_for_synthesis(search_terms, per_term=25)
    rows = rows[:max_statements]

    by_persp: dict[str, list] = {}
    for r in rows:
        by_persp.setdefault(r.get("perspektive") or "unbekannt", []).append(r)

    _LANG_ROLE = {
        "en": ("ENGLISCH — PROBLEMSTRUKTUR. Hier schreiben überwiegend "
               "Agenturinhaber (r/agency, r/digital_marketing). Leite die "
               "Problem-Cluster, Emotionen und gescheiterten Lösungsversuche "
               "VOR ALLEM hieraus ab. NICHT übersetzen und nicht als deutsche "
               "Copy verwenden."),
        "de": ("DEUTSCH — COPY-SPRACHE. Wenige, aber die einzigen Aussagen in "
               "der Sprache des Endprodukts. Nimm ALLE wörtlichen Zitate für "
               "den Werbebrief ausschließlich hieraus. Wenn ein Problem-Cluster "
               "hier keine Entsprechung hat, sag das ausdrücklich."),
    }

    blocks = []
    for persp in _PERSP_ORDER:
        group = by_persp.get(persp)
        if not group:
            continue
        lines = [f"### {PERSPEKTIV_LABELS[persp]}", f"({len(group)} Aussagen)", ""]
        # Innerhalb der Perspektive nach Sprache trennen: die Zielgruppe ist im
        # englischen Material greifbar, die Verkaufssprache nur im deutschen.
        by_lang: dict[str, list] = {}
        for r in group:
            by_lang.setdefault(r.get("lang") or "?", []).append(r)
        for lang in ("en", "de"):
            grp = by_lang.pop(lang, None)
            if not grp:
                continue
            # Die Rollenzuweisung (Problemstruktur / Copy-Sprache) gilt NUR im
            # Zielgruppen-Block. In den anderen Perspektiven wäre sie falsch —
            # aus Werbeposts soll gerade keine Copy gezogen werden.
            if persp == "zielgruppe":
                lines.append(f"#### {_LANG_ROLE[lang]}")
            else:
                lines.append(f"#### {'DEUTSCH' if lang == 'de' else 'ENGLISCH'}")
            lines.append(f"({len(grp)} Aussagen)")
            lines.append("")
            group = grp
            for r in grp:
                tag = f"[{r['source']}"
                if r.get("origin"):
                    tag += f"/{r['origin']}"
                tag += f", {r.get('lang','?')}"
                if r.get("engagement"):
                    tag += f", 👍{r['engagement']}"
                tag += "]"
                lines.append(f"{tag} {r['text']}")
            lines.append("")
        # Restsprachen unverändert anhängen
        group = [r for rest in by_lang.values() for r in rest]
        for r in group:
            tag = f"[{r['source']}"
            if r.get("origin"):
                tag += f"/{r['origin']}"
            tag += f", {r.get('lang','?')}"
            # Engagement nur zeigen, wenn es echte Werte gibt — sonst suggeriert
            # ein "👍0" an jeder Aussage fälschlich, alles sei gleich unbeachtet.
            if r.get("engagement"):
                tag += f", 👍{r['engagement']}"
            tag += "]"
            lines.append(f"{tag} {r['text']}")
        blocks.append("\n\n".join(lines))

    verteilung = {
        "sprache": dict(Counter(r.get("lang", "?") for r in rows)),
        "perspektive": dict(Counter(r.get("perspektive") or "unbekannt" for r in rows)),
    }
    return "\n\n---\n\n".join(blocks), len(rows), verteilung


def make_client():
    """Baut den passenden Client je nach BACKEND. Gibt (client, kind) zurück."""
    if BACKEND == "local":
        # LM Studio spricht das OpenAI-Protokoll — wir nutzen den openai-Client,
        # zeigen ihn aber auf den lokalen Server. Kein echter Key nötig.
        from openai import OpenAI
        client = OpenAI(base_url=LOCAL_BASE_URL, api_key="lm-studio")
        return client, "local"
    else:
        import anthropic
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise RuntimeError(
                "ANTHROPIC_API_KEY nicht gesetzt. Entweder exportieren, oder "
                "lokal analysieren: LLM_BACKEND=local (LM Studio) — siehe CLAUDE.md.")
        return anthropic.Anthropic(api_key=key), "cloud"


def _strip_reasoning(text: str) -> str:
    """
    Reasoning-Modelle (qwen3, deepseek-r1, ...) schreiben ihren Denkprozess als
    <think>...</think> in die Antwort. Das ist kein Analyse-Ergebnis und hat im
    Report nichts verloren. Ein unabgeschlossener Block (abgeschnitten durch
    max_tokens) wird ebenfalls entfernt.
    """
    import re
    text = re.sub(r"<think>.*?</think>", "", text or "", flags=re.DOTALL | re.I)
    text = re.sub(r"<think>.*$", "", text, flags=re.DOTALL | re.I)
    return text.strip()


def _ask(client, kind: str, prompt: str, max_tokens: int = 4000) -> str:
    if kind == "local":
        try:
            resp = client.chat.completions.create(
                model=LOCAL_MODEL,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as e:
            msg = str(e)
            # Häufigste lokale Stolperfalle: LM Studio lädt Modelle per Default
            # mit kleinem Kontext (oft 4096), unabhängig davon, wie viel das
            # Modell könnte. Der Rohfehler verrät das nur zwischen den Zeilen.
            if "context length" in msg or "context the overflows" in msg:
                raise RuntimeError(
                    f"Der Prompt ({len(prompt)//4:,} Tokens geschätzt) passt nicht in "
                    f"den geladenen Kontext von '{LOCAL_MODEL}'.\n"
                    "  -> In LM Studio das Modell mit größerer Context Length neu laden "
                    "(32768 reicht), ODER\n"
                    "  -> weniger Aussagen schicken: build_context(max_statements=...) "
                    "in analyze.py herunterdrehen.\n"
                    f"  Originalmeldung: {msg[:200]}") from e
            if "model_not_found" in msg or "Invalid model identifier" in msg:
                raise RuntimeError(
                    f"LOCAL_MODEL='{LOCAL_MODEL}' kennt LM Studio nicht.\n"
                    "  -> 'lms ps' zeigt die geladenen, 'lms ls' die vorhandenen Modelle.\n"
                    "  -> Für den Cloud-Weg stattdessen: unset LLM_BACKEND LOCAL_MODEL\n"
                    f"  Originalmeldung: {msg[:200]}") from e
            raise
        return _strip_reasoning(resp.choices[0].message.content)
    else:
        resp = client.messages.create(
            model=CLOUD_MODEL,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text


def analyze(cfg, out_path: str = None):
    """cfg: Config-Modul mit AUDIENCE, SEARCH_TERMS und THESIS.
    THESIS gesetzt  -> konfirmatorisch (These wird geprüft).
    THESIS = None   -> explorativ (Hypothese wird aus den Daten hergeleitet).
    """
    context, n, vert = build_context(cfg.SEARCH_TERMS)
    if n == 0:
        print("Keine Aussagen im Store — erst Collector laufen lassen.")
        return
    if n < 30:
        print(f"WARNUNG: Nur {n} Aussagen — Analyse läuft, aber Belastbarkeit ist gering.")
    langs = vert["sprache"]
    lang_str = ", ".join(f"{k}={v}" for k, v in sorted(langs.items()))
    persp_str = ", ".join(f"{k}={v}" for k, v in sorted(vert["perspektive"].items()))
    print(f"Basis: {n} Aussagen — Sprache: {lang_str} | Perspektive: {persp_str}")
    if langs.get("de", 0) < 20:
        print("WARNUNG: Wenig deutsches Material — die wörtlichen Zitate für die "
              "Werbebrief-Copy werden dünn. Mehr DACH-Quellen sammeln.")

    # None, leerer String oder nur Whitespace -> explorativer Modus
    thesis = (getattr(cfg, "THESIS", None) or "").strip()
    explorativ = not thesis

    client, kind = make_client()
    backend_label = f"lokal (LM Studio: {LOCAL_MODEL})" if kind == "local" else f"Cloud ({CLOUD_MODEL})"
    print(f"Backend: {backend_label}")

    print(f"Pass 1: offenes Codieren über {n} Aussagen ...")
    pass1 = _ask(client, kind, PASS1_PROMPT.format(audience=cfg.AUDIENCE, statements=context))

    if explorativ:
        print("Pass 2: EXPLORATIV — Hypothese aus den Daten herleiten ...")
        pass2 = _ask(client, kind, PASS2_EXPLORATIV_PROMPT.format(
            audience=cfg.AUDIENCE, pass1_result=pass1))
        pass2_titel = "Pass 2 — Explorativ: Hypothese aus den Daten"
    else:
        print("Pass 2: KONFIRMATORISCH — These gegen Daten prüfen ...")
        pass2 = _ask(client, kind, PASS2_PROMPT.format(
            audience=cfg.AUDIENCE, pass1_result=pass1, thesis=thesis))
        pass2_titel = "Pass 2 — These-Abgleich & Positionierung"

    modus = "explorativ (keine Vorab-These)" if explorativ else "konfirmatorisch (These geprüft)"
    result = (f"# Zielgruppen-Analyse: {cfg.AUDIENCE}\n\n"
              f"_Basis: {n} echte Aussagen · Sprache: {lang_str} · "
              f"Perspektive: {persp_str} · Modus: {modus} · Backend: {backend_label}_\n\n"
              f"## Pass 1 — Offenes Codieren (ohne These)\n\n{pass1}\n\n"
              f"---\n\n## {pass2_titel}\n\n{pass2}\n")

    out_path = out_path or str(Path(__file__).parent.parent / f"analyse_{cfg.SLUG}.md")
    Path(out_path).write_text(result, encoding="utf-8")
    print(f"\nAnalyse gespeichert: {out_path}")
    return result
