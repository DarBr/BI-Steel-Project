import mysql.connector
from datetime import datetime
from dotenv import load_dotenv
import os

# .env-Datei laden
load_dotenv()

# Verbindungseinstellungen aus der .env-Datei
HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT", 3306))
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("PASSWORD")  # Hier wird 'PASSWORD' verwendet
DATABASE_SOURCE = os.getenv("DATABASE_SOURCE")
DATABASE_DEST = os.getenv("DATABASE_DEST")

def daily_snapshot_lagerbestand(conn):
    """
    Erstellt für den heutigen Tag einen Snapshot aller vorhandenen Materialien
    aus tb_Lagerbestand (database-steel). Schreibt in Fakt_Lagerbestand (database-dwh).
    Falls ein Eintrag bereits existiert, wird dieser aktualisiert.
    """
    cursor = conn.cursor()

    # 1) Heutiges Datum
    today = datetime.now().date()
    zeitid = today.strftime("%Y-%m-%d:00-00")

    # 2) Pro Material den letzten bekannten Datensatz (<= heute) ermitteln
    select_sql = f"""
        SELECT lb.MaterialID, lb.Menge, lb.Mindestbestand
        FROM `{DATABASE_SOURCE}`.tb_Lagerbestand lb
        JOIN (
            SELECT MaterialID, MAX(Bestandsdatum) AS MaxDatum
            FROM `{DATABASE_SOURCE}`.tb_Lagerbestand
            WHERE Bestandsdatum <= %s
            GROUP BY MaterialID
        ) t 
          ON lb.MaterialID = t.MaterialID
         AND lb.Bestandsdatum = t.MaxDatum
    """
    cursor.execute(select_sql, (today,))
    rows = cursor.fetchall()

    if not rows:
        print("Keine Daten gefunden, die <= heute sind. Abbruch.")
        cursor.close()
        return

    # 3) Insert mit "ON DUPLICATE KEY UPDATE"
    insert_sql = f"""
        INSERT INTO `{DATABASE_DEST}`.Fakt_Lagerbestand
            (ZeitID, MaterialID, Menge, Mindestbestand)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Menge = VALUES(Menge),
            Mindestbestand = VALUES(Mindestbestand);
    """

    for (material_id, menge, mindestbestand) in rows:
        cursor.execute(insert_sql, (zeitid, material_id, menge, mindestbestand))

    conn.commit()
    cursor.close()
    print(f"Täglicher Snapshot in Fakt_Lagerbestand für {zeitid} abgeschlossen.")

def main():
    # Verbindung zur Datenbank herstellen
    conn = mysql.connector.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD
    )

    daily_snapshot_lagerbestand(conn)
    conn.close()

if __name__ == "__main__":
    main()
