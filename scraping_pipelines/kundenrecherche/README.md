# Kundenrecherche-Pipeline (v2)

Tiefe Zielgruppen-Recherche über Reddit, Fachforen und LinkedIn — mit Claude
als Analyst in einem Zwei-Pass-Verfahren gegen Konfirmationsbias.

## Architektur

```
[1] COLLECT               [2] STORE                 [3] SYNTHESIZE (2 Pässe)
collectors/collect.py  →  storage/store.py       →  synthesis/analyze.py
Reddit (Apify ODER        SQLite + FTS5 mit          Pass 1: offenes Codieren
gratis JSON-API),         Präfix-Matching für         (Claude kennt die These
Foren, LinkedIn           deutsche Flexion,           NICHT — datengetrieben)
                          Junk-Filter, GDPR-safe     Pass 2: These-Abgleich +
                          (Aussagen, keine Profile)   Positionierung + Copy
```

## Warum Zwei-Pass-Synthese?

Zeigt man dem Modell die These vor der Analyse, findet es bevorzugt
Bestätigung. Pass 1 codiert die Aussagen deshalb offen (Problem-Cluster mit
Häufigkeit, Emotionen, wörtliche Zitate, gescheiterte Lösungsversuche).
Erst Pass 2 vergleicht mit der These — und sagt ehrlich "nicht bestätigt",
wenn die Daten ein anderes dominantes Problem zeigen.

## Setup

```bash
pip install apify-client anthropic
export ANTHROPIC_API_KEY=sk-ant-...
export APIFY_TOKEN=apify_api_...        # nur für die Apify-Collector nötig
```

## Empfohlener Ablauf

```bash
# 1. Gratis-Testlauf (kein Apify-Token nötig):
python run.py init
python run.py collect reddit-free       # Reddits öffentliche JSON-API
python run.py stats
python run.py analyze                   # → analyse_webdesigner.md

# 2. Wenn die Richtung stimmt, skalieren (Kommentare, Foren, LinkedIn):
python run.py collect                   # alle Apify-Collector
python run.py analyze
```

## Neue Zielgruppe anlegen

`config/webdesigner.py` kopieren (z.B. nach `config/coaching.py`), anpassen:
AUDIENCE, THESIS, Quellen, Keywords, SEARCH_TERMS. Dann:

```bash
python run.py collect --config coaching
python run.py analyze --config coaching
```

## Vor dem ersten Apify-Lauf prüfen (wichtig!)

1. **Actor-Input-Schemas**: Die IDs in `ACTORS` sind Startpunkte
   (`trudax/reddit-scraper` und `apify/website-content-crawler` verifiziert).
   Actors ändern ihre Schemas — einmal in der Apify-Console gegenprüfen.
   Der LinkedIn-Actor ist bewusst leer: aktuellen Post-Search-Actor
   auswählen und ID eintragen.
2. **Forum-URLs**: Im Browser prüfen, ob die URLs auf die richtigen
   Bereiche zeigen (Foren ändern Strukturen).
3. **Klein anfangen**: max_items niedrig halten, Ergebnis mit
   `python run.py stats` prüfen, dann hochdrehen.

## GDPR / Recht

- Nur öffentliche Inhalte, keine Logins, keine privaten Gruppen.
- Gespeichert werden AUSSAGEN, nicht Autoren.
- reddit-free hält 1,5s Delay ein (Rate-Limit-Höflichkeit).
- Foren-Scraping je nach ToS Grauzone — robots.txt/ToS im Zweifel prüfen.

## Bekannte Grenzen & Ausbaustufen

- **FTS5 stemmt nicht**: Präfix-Matching (`Preis*` → Preise/Preiskampf) fängt
  viel ab, aber Synonyme ("kostet zu viel" ≠ "Preis") findet erst eine
  semantische Suche. Nächste Stufe: Embeddings (sentence-transformers lokal).
- **reddit-free holt keine Kommentare** — die sind oft das Gold. Dafür den
  Apify-Weg nutzen (searchComments: true ist gesetzt).
- **Sprach-Heuristik** ist bewusst simpel (DE/EN-Funktionswörter). Reicht für
  Statistik, nicht für harte Filterung.
- **Scheduling**: collect als Cron über Nacht → Store wächst → Synthese wird
  mit der Zeit belastbarer.
- **Validierung**: Die Pipeline zeigt Muster in öffentlicher Sprache. Bevor
  viel Budget auf eine Positionierung geht: 5-10 echte Gespräche führen
  (Pass 2 liefert dafür die offenen Fragen mit).

## Quellen wählen (v3): Register + Empfehlung

Die Pipeline hat jetzt ein Quellen-Register (`collectors/registry.py`) mit
Bewertungs- und Social-Scrapern über Apify: Google Maps, Trustpilot, Amazon,
Kununu, TikTok, Instagram, YouTube-Kommentare. Jede Zielgruppe wählt in ihrer
Config über einen `SOURCES`-Block aus, was laufen soll.

Zusätzlich: **Google-Blogs/Fachartikel** über `GOOGLE_QUERIES` in der Config.
Zwei Schritte (SERP → Content-Crawl), Aufruf:

```bash
python run.py collect google --config recruiting_agenturen
```

Die Treffer landen als Quelle `blog` (Perspektive `fachpresse`) im Store und
werden in der Synthese mit ausgewertet.

### Welche Quellen für meine Zielgruppe?

Faustregel: Suche dort, wo die Zielgruppe **ehrlich über ihr Problem redet,
ohne verkaufen zu wollen**. Anonyme Räume (Reddit, Foren, Bewertungen) sind
ehrlicher als Selbstdarstellung (LinkedIn, Instagram-Posts).

Lass dir Quellen vorschlagen:

```bash
python run.py sources b2b lokal        # B2B, lokale Dienstleistung
python run.py sources produkt jung     # B2C-Produkt, junge Zielgruppe
```

Flags: `b2b` (sonst B2C), `lokal` (ortsgebundene Dienstleistung),
`jung` (Lifestyle/visuell), `produkt` (Produkt/SaaS mit Bewertungen).

Die Empfehlung übernimmst du als `SOURCES`-Block in die Config, z.B.:

```python
SOURCES = {
    "google_maps_reviews": {"queries": ["Webdesign Agentur München"], "max_reviews": 80},
    "kununu": {"urls": ["https://www.kununu.com/de/<agentur>"]},
}
```

### Register-Quellen sammeln

```bash
python run.py collect registry          # nur die Register-Quellen
python run.py collect                    # alles inkl. Register-Quellen
```

### Neue Quelle hinzufügen

Einen Eintrag in `collectors/registry.py` ergänzen (actor, source,
build_input, parse). Danach in jeder Config über SOURCES nutzbar. Vor dem
ersten Lauf das Input-Schema des Actors in der Apify-Console prüfen.

### Der Google-Maps-Trick

Für lokale/dienstleistungsnahe Zielgruppen sind Google-Maps-Bewertungen
extrem wertvoll: Dort schreiben die *Kunden* der Zielgruppe, was sie nervt —
also Einwand-Material aus der Gegenrichtung. Bei Webdesignern z.B. die
Bewertungen von Agenturen: was Kunden an Agenturen frustriert, ist genau der
Hebel für deine Positionierung.

## Analyse lokal statt Cloud (LM Studio)

Die Synthese kann statt der Anthropic-Cloud ein lokales Modell über LM Studio
nutzen — Daten verlassen dann deinen Rechner nie, und es kostet nichts.

### Einrichten

1. LM Studio installieren, ein Instruct-Modell laden (Empfehlung: 30B+ in
   guter Quantisierung, z.B. Qwen2.5-32B-Instruct — kleinere Modelle glätten
   Zitate und urteilen gefälliger).
2. In LM Studio den "Local Server" starten (Reiter Developer / Local Server).
   Standard-URL: http://localhost:1234/v1
3. Für den lokalen Modus zusätzlich installieren: `pip install openai`
   (nur nötig für lokal; der Cloud-Pfad braucht es nicht).

### Umschalten

```bash
export LLM_BACKEND=local
export LOCAL_MODEL="qwen2.5-32b-instruct"   # Identifier aus dem LM-Studio-Log
# optional, falls andere URL/Port:
# export LOCAL_BASE_URL="http://localhost:1234/v1"
python run.py analyze
```

Zurück zu Cloud: `export LLM_BACKEND=cloud` (oder Variable entfernen).

### Was ist besser?

- Pass 1 (offenes Codieren): lokal meist gut genug.
- Wörtliche Zitate + ehrliches Urteil in Pass 2: Cloud ist zuverlässiger —
  kleinere Modelle formulieren Zitate um (schlecht fürs Copy-Rohmaterial) und
  bestätigen Thesen lieber, als sie zu widerlegen.
- Empfehlung: lokal für explorative Erstläufe und Iteration, Cloud für den
  finalen Report, auf dessen Basis Budget fließt. Der Report-Header vermerkt,
  mit welchem Backend er erstellt wurde — so bleiben die Läufe vergleichbar.

Sammeln (collect) ist davon unberührt und läuft immer gleich; nur die
Analyse-Schicht wechselt das Backend.
