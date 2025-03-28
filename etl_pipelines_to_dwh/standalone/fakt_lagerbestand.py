import mysql.connector
from datetime import datetime

def daily_snapshot_lagerbestand(conn):
    """
    Erstellt für den heutigen Tag einen Snapshot aller vorhandenen Materialien
    aus tb_Lagerbestand (database-steel). Schreibt in Fakt_Lagerbestand (database-dwh).
    Falls ein Eintrag bereits existiert, wird dieser aktualisiert.
    """
    cursor = conn.cursor()

    # 1) Heutiges Datum
    today = datetime.now().date()
    # Format für ZeitID, z. B. "2025-03-28:00-00"
    zeitid = today.strftime("%Y-%m-%d:00-00")

    # 2) Pro Material den letzten bekannten Datensatz (<= heute) ermitteln
    select_sql = """
        SELECT lb.MaterialID, lb.Menge, lb.Mindestbestand
        FROM `database-steel`.tb_Lagerbestand lb
        JOIN (
            SELECT MaterialID, MAX(Bestandsdatum) AS MaxDatum
            FROM `database-steel`.tb_Lagerbestand
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
    insert_sql = """
        INSERT INTO `database-dwh`.Fakt_Lagerbestand
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
    # Verbindung zur DB (keine bestimmte 'database=' nötig, 
    # da wir per Schema-Name auf die Tabellen zugreifen)
    conn = mysql.connector.connect(
        host="13.60.244.59",
        port=3306,
        user="user",
        password="clientserver"
    )

    daily_snapshot_lagerbestand(conn)
    conn.close()

if __name__ == "__main__":
    main()
