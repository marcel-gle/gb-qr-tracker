# Pflegeheime ab 80 Plätzen finden

Zwei Quellen, beide liefern die Platzzahl mit:

| Skript | Quelle | Gebiet |
|---|---|---|
| `heimaufsicht_berlin.py` | Prüfberichte der Berliner Heimaufsicht | Berlin, vollständig |
| `ketten_standorte.py` | Sitemaps der großen Pflegeketten | alle vier Zielregionen |

`plz_bundesland.py` ordnet Postleitzahlen den Bundesländern zu und muss im selben Ordner liegen.

## Einrichtung (einmalig)

```bash
cd scripts/scrapers/pflegeheim_scraper
python3 -m venv .venv && source .venv/bin/activate
pip install requests beautifulsoup4 pdfplumber pgeocode
```

## Teil A: Berlin über die Heimaufsicht

Vom Repo-Root aus (oder aus diesem Ordner):
```bash
.venv-scraper/bin/python scraping_pipelines/hps-pflege/pflegeheim_scraper/heimaufsicht_berlin.py links
```
(Braucht `pdfplumber` – im Projekt-`.venv` ggf. fehlend; `.venv-scraper` hat alles.)

**1. PDF-Links sammeln**
```bash
python heimaufsicht_berlin.py links
```
Das Skript lädt die öffentliche LAGeSo-CSV hinter dem Datawrapper auf berlin.de und speichert die Prüfbericht-Links in `heimaufsicht_pdf_links.txt` (im Skript-Ordner).

Falls die CSV nicht erreichbar ist: https://www.berlin.de/lageso/soziales/heimaufsicht/pruefberichte/ bzw. den Datawrapper-Export speichern, dann:
```bash
python heimaufsicht_berlin.py links --html seite1.html seite2.html
```

**2. Testlauf mit 20 Berichten**
```bash
python heimaufsicht_berlin.py auswerten --limit 20
```
`heimaufsicht_berlin.csv` öffnen und prüfen, ob Name, Adresse und Plätze stimmen.

**3. Alle Berichte auswerten**
```bash
python heimaufsicht_berlin.py auswerten
```
Die PDFs werden in `pdf_cache_heimaufsicht/` zwischengespeichert, ein zweiter Lauf lädt nichts neu.

**Ergebnis:** je Einrichtung der neueste Bericht. Filter in Excel: `Pflegeheim = ja` und `ab 80 Plätze = ja`.
Die Spalte **Personal-Befund** zeigt, ob die Heimaufsicht beim Personal eine Abweichung festgestellt hat. Das ist ein starkes Kaufsignal.

## Teil B: Pflegeketten

**1. URL-Muster je Kette finden** (nur für Ketten ohne Muster; Korian ist schon eingetragen)
```bash
python ketten_standorte.py erkunden alloheim
```
Die Ausgabe zeigt die URL-Gruppen der Sitemap und schlägt ein Muster vor, zum Beispiel
`r"/standorte/[^/]+/?$"`. Zwei, drei Beispiel-URLs im Browser öffnen: Ist das jeweils ein einzelnes Haus?
Dann das Muster in `ketten_standorte.py` oben bei `KETTEN` → `"muster"` eintragen.
Das für jede gewünschte Kette wiederholen. Findet `erkunden` keine Sitemap: eine Haus-Seite im Browser öffnen und das Muster aus ihrer URL ableiten.

**2. Haus-URLs sammeln**
```bash
python ketten_standorte.py sammeln
```

**3. Testlauf mit 10 Häusern je Kette**
```bash
python ketten_standorte.py auswerten --limit 10
```

**4. Alle Häuser auswerten**
```bash
python ketten_standorte.py auswerten
```
Standardmäßig nur Berlin, Brandenburg, Schleswig-Holstein und Saarland. Für ganz Deutschland: `--bundeslaender alle`.
Läuft das Skript erneut, überspringt es bereits erledigte Häuser. Für einen kompletten Neustart `ketten_standorte.csv` löschen.

**Ergebnis:** `ketten_standorte.csv`. Filter: `Status = ok` und `ab 80 Plätze = ja`.
Bei `ab 80 Plätze = unbekannt` steht keine Platzzahl auf der Seite. Diese Häuser im Kassenportal (z. B. pflegelotse.de) einzeln nachschlagen.
`Plätze-Quelle = aus Zimmerzahl berechnet` ist eine Näherung (Einzelzimmer + 2 × Doppelzimmer).

## Danach

Beide CSV-Dateien zurückschicken. Sie werden dann mit der Versandliste zusammengeführt:
Dubletten zu den 250 vorhandenen Heimen entfernt, gegen die Negativliste geprüft und für den
Ansprechpartner-Scraper vorbereitet (`--leadart Pflegeheim`).
