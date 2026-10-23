# PV-Upgrade Lead-Pipeline (MaStR)

Findet **gewerbliche PV-Anlagen (>30 kWp)** mit **alten, hochvergüteten
Anlagen ohne Speicher**, bei denen sich ein Speicher-/Technik-Upgrade lohnt —
inkl. Adresse, Koordinaten und einem priorisierenden Lead-Score.

## Warum genau dieses Segment?

Anlagen ab Baujahr ~2006–2013 wurden mit hoher EEG-Einspeisevergütung
(30–50 ct/kWh) auf **Volleinspeisung** optimiert. Nach 20 Jahren
(+ Inbetriebnahmejahr) endet die Förderung; danach gibt's nur noch den
Marktwert (~3–6 ct). Ab dem Moment ist jede *selbst verbrauchte* kWh
25–30 ct wert → Speicher + neuer Wechselrichter rechnen sich plötzlich.
Das gibt jedem Lead ein konkretes, datierbares Timing-Argument
("Ihre Förderung läuft Ende {Jahr} aus").

## Setup (einmalig)

```bash
pip install -r requirements.txt
```

## Ablauf

```bash
# 1. MaStR-Gesamtdatenauszug laden (Solar + Speicher). ~4 GB, 20–40 Min.
#    Idempotent — lädt nur, was fehlt.
python step1_download.py

# 2. Filtern + Scoren → pv_upgrade_leads.csv (läuft in Sekunden, offline)
python step2_score.py

# 3. (optional) Top-Leads mit Kontaktdaten anreichern
python step3_enrich.py --top 200
```

## Harte Filter (config.py)

| Filter | Wert | Grund |
|---|---|---|
| Bruttoleistung | 30–750 kWp | >30 kWp = Adresse/Koordinaten öffentlich; <750 = Mittelstand, kein Fonds |
| Inbetriebnahme | 2006–2014 | Hochvergütung, Förderende absehbar |
| Lage | Bauliche Anlagen | schließt Freiflächen-/Investorenparks aus |
| Betreiber | Organisation | Name nur bei juristischen Personen öffentlich |
| Status | In Betrieb | keine stillgelegten/geplanten |

## Lead-Score (0–100)

| Komponente | Gewicht | Logik |
|---|---|---|
| EEG-Ablauf | 35 | je näher/überschritten das Förderende, desto heißer |
| Anlagengröße | 25 | Sweet Spot 50–300 kWp = volles Projektvolumen |
| Kein Speicher | 20 | `SpeicherAmGleichenOrt`=Nein → offener Bedarf |
| Branche | 15 | Keyword-Match auf Firmenname → Verbrauchsprofil |
| Betreiber-Typ | 5 | Investment-Vehikel abgewertet |

Alle Gewichte, Größen-Schwellen und Branchen-Keywords sind in `config.py`
zentral tunebar.

## Wichtige Einschränkungen (ehrlich)

- **Betreiber ≠ Standortfirma:** Bei Pachtmodellen ist der Betreiber eine
  Investorengesellschaft. Der `investor_verdacht`-Flag und das Namens-Scoring
  fangen die häufigsten Fälle ab, aber nicht alle.
- **Landwirte fehlen teilweise:** Viele große Scheunendach-Anlagen laufen auf
  natürliche Personen → anonymisiert, trotz >30 kWp. Nicht adressgenau
  erreichbar (nur via PLZ-Targeting/Kooperation).
- **Branche via Name ist ein Proxy:** Für saubere WZ-Codes/Branchen in
  Schritt 3 anreichern (North Data / Handelsregister).

## Rechtliches

Es werden ausschließlich die **öffentlichen** MaStR-Daten von Organisationen
mit >30 kWp verwendet — genau die Daten, die die MaStR-Verordnung zur
Veröffentlichung vorsieht. Für die anschließende Ansprache (Direktmarketing
B2B) gelten UWG/DSGVO wie üblich; Rechtsgrundlage und Ansprache-Kanal
(Post/Telefon/E-Mail) im Einzelfall prüfen.
