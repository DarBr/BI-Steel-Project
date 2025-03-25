import pandas as pd
import mysql.connector
import sys
from datetime import datetime, timedelta
import random
import math

def import_auftraege(conn, csv_path_orders):
    """
    Liest die Aufträge aus csv_path_orders (AuftragsID, KundenID, Bestelldatum, Auftragsvolumen, Lieferdatum),
    filtert nur Datensätze mit Bestelldatum <= heute und fügt sie per INSERT IGNORE in tb_Kundenauftrag ein.
    Anschließend werden die eingefügten Datensätze aus der CSV entfernt.
    """
    print("=== Starte Import der Aufträge ===")
    cursor = conn.cursor()

    # 1) CSV einlesen, Datumsfelder parsen
    df = pd.read_csv(
        csv_path_orders,
        parse_dates=['Bestelldatum', 'Lieferdatum']
    )

    # 2) Nur Zeilen bis einschließlich heute
    today = pd.Timestamp.today().normalize()
    df_to_insert = df[df['Bestelldatum'] <= today]

    # 3) Insert-Statement
    insert_query = """
        INSERT IGNORE INTO tb_Kundenauftrag
        (AuftragsID, KundenID, Bestelldatum, Auftragsvolumen, Lieferdatum)
        VALUES (%s, %s, %s, %s, %s)
    """

    # 4) Einfügen
    for _, row in df_to_insert.iterrows():
        cursor.execute(insert_query, (
            int(row['AuftragsID']),
            int(row['KundenID']),
            row['Bestelldatum'].strftime('%Y-%m-%d'),
            float(row['Auftragsvolumen']),  # float => behält .00
            row['Lieferdatum'].strftime('%Y-%m-%d')
        ))

    conn.commit()

    # 5) Entferne die eingefügten Zeilen aus der CSV (d. h. Bestelldatum <= heute)
    df_remaining = df[df['Bestelldatum'] > today]
    df_remaining.to_csv(csv_path_orders, index=False)

    print("Import der Aufträge abgeschlossen.")
    cursor.close()

def import_positionen(conn, csv_path_positions):
    """
    Liest die Auftragspositionen aus csv_path_positions (ID, ProduktID, Menge, Preis, AuftragsID),
    fügt sie per INSERT IGNORE in tb_Kundenauftragspositionen ein (sofern die AuftragsID existiert).
    Für jede neu eingefügte Position werden automatisch Bestellungen in tb_Bestellung angelegt,
    basierend auf tb_MaterialZuProdukt (Verhaeltnis).
    Anschließend werden die eingefügten Positionen aus der CSV entfernt.
    """
    print("=== Starte Import der Auftragspositionen ===")
    cursor = conn.cursor()

    # 1) CSV einlesen
    df = pd.read_csv(csv_path_positions)

    # 2) Welche AuftragsIDs existieren bereits?
    cursor.execute("SELECT AuftragsID FROM tb_Kundenauftrag")
    existing_orders = set(row[0] for row in cursor.fetchall())

    # 3) Nur Positionen, deren AuftragsID existiert
    df_to_insert = df[df['AuftragsID'].isin(existing_orders)]

    insert_query_positions = """
        INSERT IGNORE INTO tb_Kundenauftragspositionen
        (ID, ProduktID, Menge, Preis, AuftragsID)
        VALUES (%s, %s, %s, %s, %s)
    """

    # Zweiter Cursor für die Bestellungen
    cursor2 = conn.cursor()

    # Indizes, die wir erfolgreich eingefügt haben
    inserted_indices = []

    for index, row in df_to_insert.iterrows():
        # 4) Position einfügen
        cursor.execute(insert_query_positions, (
            int(row['ID']),
            int(row['ProduktID']),
            int(row['Menge']),
            float(row['Preis']),   # float => behält Nachkommastellen
            int(row['AuftragsID'])
        ))

        # Nur wenn tatsächlich neu eingefügt (rowcount=1)
        if cursor.rowcount == 1:
            inserted_indices.append(index)

            # 5) Bestelldatum aus tb_Kundenauftrag holen
            cursor2.execute("""
                SELECT Bestelldatum
                FROM tb_Kundenauftrag
                WHERE AuftragsID = %s
            """, (int(row['AuftragsID']),))
            result = cursor2.fetchone()
            if not result:
                # Sollte eigentlich nie vorkommen, da wir AuftragsID gefiltert haben
                continue
            bestelldatum = result[0]

            # 6) Lieferdatum = Bestelldatum + random(3..5) Tage
            offset_days = random.randint(3, 5)
            lieferdatum = bestelldatum + timedelta(days=offset_days)

            # 7) tb_MaterialZuProdukt abfragen
            cursor2.execute("""
                SELECT MaterialID, Verhaeltnis
                FROM tb_MaterialZuProdukt
                WHERE ProduktID = %s
            """, (int(row['ProduktID']),))
            mat_rows = cursor2.fetchall()

            # 8) Pro Material eine Bestellung anlegen
            insert_bestellung = """
                INSERT IGNORE INTO tb_Bestellung
                (MaterialID, LieferantID, Bestellmenge, Bestelldatum, Lieferdatum)
                VALUES (%s, %s, %s, %s, %s)
            """
            for (mat_id, verh) in mat_rows:
                # Menge * Verhaeltnis => aufrunden
                bestellmenge = math.ceil(int(row['Menge']) * verh)
                # Zufälliger Lieferant 1..4
                lieferant_id = random.randint(1, 4)

                cursor2.execute(insert_bestellung, (
                    mat_id,
                    lieferant_id,
                    bestellmenge,
                    bestelldatum,
                    lieferdatum
                ))

    conn.commit()
    cursor2.close()

    # 9) Entferne die eingefügten Zeilen aus der CSV
    df_remaining = df.drop(index=inserted_indices)
    df_remaining.to_csv(csv_path_positions, index=False)

    print("Import der Positionen abgeschlossen.")
    cursor.close()

def main():
    try:
        # 1) Verbindung aufbauen
        conn = mysql.connector.connect(
            host="13.60.244.59",
            port=3306,
            user="user",
            password="clientserver",
            database="database-steel"
        )

        # 2) Aufträge importieren (mit Datumsfilter)
        csv_path_orders = r"C:\Users\morit\Documents\auftraege_6_monate.csv"
        import_auftraege(conn, csv_path_orders)

        # 3) Positionen importieren (inkl. Bestellungen)
        csv_path_positions = r"C:\Users\morit\Documents\Kundenauftragspositionen_exakt.csv"
        import_positionen(conn, csv_path_positions)

        # 4) Verbindung schließen
        conn.close()
        print("=== Gesamter Import erfolgreich abgeschlossen ===")

    except Exception as e:
        print(f"Fehler beim Import: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    main()
