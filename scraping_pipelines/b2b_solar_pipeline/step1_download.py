"""
Schritt 1: MaStR-Gesamtdatenauszug herunterladen und in lokale SQLite-DB laden.

Der Download ist groß (~4 GB gepackt, entpackt deutlich mehr) und dauert
je nach Verbindung 20-40 Minuten. Danach liegt alles lokal in SQLite —
alle folgenden Schritte laufen offline und in Sekunden.

Aufruf:  python step1_download.py
Der Auszug wird nur geladen, wenn er noch nicht existiert (Idempotenz).
"""
from open_mastr import Mastr


def main():
    db = Mastr()  # DB liegt unter ~/.open-MaStR/data/sqlite/open-mastr.db

    # Wir brauchen nur Solar + Speicher. 'data' begrenzt den Download,
    # spart Zeit und Plattenplatz erheblich gegenüber dem Voll-Auszug.
    db.download(
        method="bulk",
        data=["solar", "storage"],
    )
    print("\n✓ Download & Import abgeschlossen.")
    print(f"  DB-Pfad: {db._sqlite_folder}")


if __name__ == "__main__":
    main()
