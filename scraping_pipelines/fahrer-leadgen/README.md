# Fahrer-Leadgen Pipeline

Findet Logistik-/Transportunternehmen (30–80 MA, **keine eigene
Personalabteilung**, sucht aktiv Fahrer) in Deutschland und bewertet sie.

## Funnel

```
Stage 1  Sourcing        North Data + Ocean.io CSV-Exports -> Master-Liste (dedup)
Stage 2  Hiring-Signal   Bundesagentur-API + Karriereseiten-Scraping + Google Jobs -> "sucht Fahrer?"
Stage 3  HR-Ausschluss   Impressum/Team/Kontakt scrapen + LLM -> HR? -> ausschließen
Stage 4  Scoring         gewichtete Signale -> Rangliste + qualify-Flag
```

HR-Ausschluss läuft **zuletzt**, nachdem der Pool schon eng ist — das spart
Scraping-/LLM-Kosten auf dem teuersten Filter.

## Setup

```bash
pip install httpx
# lege deine Exporte ab:
#   data/input/northdata_*.csv   (Prefix entscheidet über die Quelle)
#   data/input/ocean_*.csv
python pipeline/run.py            # oder --limit 20 für einen Schnelltest
```

Output nach `data/output/`:
- `qualified_leads.csv` — Score ≥ Schwelle, keine HR → Outreach
- `review_queue.csv` — unsicheres HR-Urteil → manuelle Prüfung
- `full_run.csv` — alles zur Nachvollziehbarkeit

## Was manuell ist vs. automatisiert

| Schritt | Status |
|---|---|
| North Data Power Search Export | **manuell** (keine API) — CSV ablegen |
| Ocean.io Export | **manuell** — CSV ablegen |
| Bundesagentur Jobsuche | automatisch (`X-API-Key: jobboerse-jobsuche`) |
| Karriereseiten-Scraping | automatisch |
| Google Jobs | automatisch, **optional** — braucht `SERPAPI_KEY` (env var); ohne Key liefert Methode C einfach keine Treffer |
| HR-Erkennung (LLM) | automatisch — **lokales Modell in `stage3` einhängen** |

## Was du noch anpassen musst

1. **`config.py` COLUMN_MAP** (in `stage1_source.py`) an deine echten
   Export-Spaltennamen anpassen.
2. **`stage3_hr_filter.call_llm()`** auf dein lokales Modell zeigen lassen
   (Ollama/vLLM-Beispiel steht im Docstring). Ohne das bleiben alle HR-Urteile
   "unsicher" und landen in der Review-Queue.
3. **`config.USER_AGENT`** auf eine echte Kontaktadresse setzen (robots.txt /
   Fairness beim Scraping deutscher KMU-Seiten).
4. **`SERPAPI_KEY`** als Umgebungsvariable setzen, falls Methode C (Google
   Jobs) genutzt werden soll — Account bei https://serpapi.com/ nötig
   (kostenpflichtig, hat aber ein kostenloses Kontingent zum Testen).

## Wichtige Einschränkung

"Keine Personalabteilung" ist aus öffentlichen Daten **nicht zuverlässig**
verifizierbar — eine HR-Person kann existieren, ohne online genannt zu sein.
Der Filter ist die Hauptfehlerquelle. Deshalb: mittlere Konfidenz → Review-Queue
statt Auto-Verwerfen. Wenn die Fehl-Ausschlussrate zu hoch wird, "keine HR" eher
als weiches Ranking-Signal statt als hartes Ausschlusskriterium behandeln.

## DSGVO

Scraping bleibt in-house (lokales LLM). Nur öffentliche Firmendaten
(Impressum, Karriereseiten, staatliche Jobbörse). Keine personenbezogenen Daten
über das Nötige hinaus speichern; Kontaktpersonen nur, soweit für B2B-Ansprache
nach UWG erforderlich.
```
