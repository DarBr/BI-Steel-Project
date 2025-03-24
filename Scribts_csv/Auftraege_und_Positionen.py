import pandas as pd
import mysql.connector
import sys
from datetime import datetime

def import_auftraege(conn, csv_path_orders):
    """
    Liest die Aufträge aus csv_path_orders (AuftragsID, KundenID, Bestelldatum, Auftragsvolumen, Lieferdatum),
    filtert nur Datensätze mit Bestelldatum <= heute und fügt sie per INSERT IGNORE in tb_Kundenauftrag ein.
    Bereits eingefügte werden aus der CSV entfernt.
    """
    print("=== Starte Import der Aufträge ===")
    cursor = conn.cursor()

    # CSV einlesen, Datumsfelder parsen
    df = pd.read_csv(
        csv_path_orders,
        parse_dates=['Bestelldatum', 'Lieferdatum']
    )

    # Nur Zeilen bis einschließlich heute
    today = pd.Timestamp.today().normalize()
    df_to_insert = df[df['Bestelldatum'] <= today]

    insert_query = """
        INSERT IGNORE INTO tb_Kundenauftrag
        (AuftragsID, KundenID, Bestelldatum, Auftragsvolumen, Lieferdatum)
        VALUES (%s, %s, %s, %s, %s)
    """

    inserted_indices = []
    for index, row in df_to_insert.iterrows():
        cursor.execute(insert_query, (
            row['AuftragsID'],
            row['KundenID'],
            row['Bestelldatum'].strftime('%Y-%m-%d'),
            row['Auftragsvolumen'],
            row['Lieferdatum'].strftime('%Y-%m-%d')
        ))
        if cursor.rowcount == 1:
            inserted_indices.append(index)

    conn.commit()

    # Nur die Zeilen behalten, die NICHT eingefügt wurden (und die in der Zukunft liegen)
    df_remaining = df.drop(index=inserted_indices)
    df_remaining.to_csv(csv_path_orders, index=False)

    print(f"Import der Aufträge abgeschlossen. Eingefügte Datensätze: {len(inserted_indices)}")
    cursor.close()

def import_positionen(conn, csv_path_positions):
    """
    Liest die Auftragspositionen aus csv_path_positions (ID, ProduktID, Menge, Preis, AuftragsID),
    prüft, ob AuftragsID in tb_Kundenauftrag existiert, und fügt sie per INSERT IGNORE ein.
    Bereits eingefügte werden aus der CSV entfernt.
    """
    print("=== Starte Import der Auftragspositionen ===")
    cursor = conn.cursor()

    # CSV einlesen
    df = pd.read_csv(csv_path_positions)

    # AuftragsIDs ermitteln, die bereits in tb_Kundenauftrag existieren
    cursor.execute("SELECT AuftragsID FROM tb_Kundenauftrag")
    existing_orders = set(row[0] for row in cursor.fetchall())

    # Nur Positionen, deren AuftragsID bereits existiert
    df_to_insert = df[df['AuftragsID'].isin(existing_orders)]

    insert_query = """
        INSERT IGNORE INTO tb_Kundenauftragspositionen
        (ID, ProduktID, Menge, Preis, AuftragsID)
        VALUES (%s, %s, %s, %s, %s)
    """

    inserted_indices = []
    for index, row in df_to_insert.iterrows():
        cursor.execute(insert_query, (
            row['ID'],
            row['ProduktID'],
            row['Menge'],
            row['Preis'],
            row['AuftragsID']
        ))
        # rowcount == 1 => neu eingefügt (kein Duplikat)
        if cursor.rowcount == 1:
            inserted_indices.append(index)

    conn.commit()

    # Aus der CSV entfernen: nur Datensätze behalten, die NICHT eingefügt wurden
    # sowie solche, deren AuftragsID noch nicht existiert
    df_remaining = df.drop(index=inserted_indices)
    df_remaining.to_csv(csv_path_positions, index=False)

    print(f"Import der Auftragspositionen abgeschlossen. Eingefügte Datensätze: {len(inserted_indices)}")
    cursor.close()

def main():
    try:
        # Pfade zu den beiden CSV-Dateien anpassen
        csv_path_orders = r"C:\Users\morit\Documents\auftraege_6_monate.csv"
        csv_path_positions = r"C:\Users\morit\Documents\Kundenauftragspositionen_exakt.csv"

        # Verbindung zur MySQL-Datenbank
        conn = mysql.connector.connect(
            host="13.60.244.59",
            port=3306,
            user="user",
            password="clientserver",
            database="database-steel"
        )

        # 1) Zuerst die Aufträge einlesen und einfügen
        import_auftraege(conn, csv_path_orders)

        # 2) Danach die Auftragspositionen einlesen und einfügen
        import_positionen(conn, csv_path_positions)

        # Verbindung schließen
        conn.close()

        print("=== Gesamter Import erfolgreich abgeschlossen ===")

    except Exception as e:
        print(f"Fehler beim Import: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    main()
