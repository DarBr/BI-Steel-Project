import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Verbindungseinstellungen
HOST = "13.60.244.59"
PORT = 3306
USER = "user"
PASSWORD = "clientserver"

# Extraktion Produktionsdaten
def fetch_production_data():
    print("Starte Extraktion der Produktionsdaten...")
    try:
        source_conn = mysql.connector.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database="database-steel")
        source_cursor = source_conn.cursor(dictionary=True)
        query = """
            SELECT ProduktionsID, MaschinenID, Startzeit, Produktionsmenge, Ausschussmenge, ProduktID, Auslastung, Verbrauch
            FROM tb_Produktionsauftrag;
        """
        source_cursor.execute(query)
        result = source_cursor.fetchall()
        df = pd.DataFrame(result)
        source_cursor.close()
        source_conn.close()
        print("Produktionsdaten erfolgreich extrahiert.")
        return df
    except mysql.connector.Error as err:
        print(f"Fehler beim Abrufen der Produktionsdaten: {err}")
        return None

# Extraktion Verkaufsdaten
def fetch_sales_data():
    print("Starte Extraktion der Verkaufsdaten...")
    try:
        source_conn = mysql.connector.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database="database-steel")
        source_cursor = source_conn.cursor(dictionary=True)
        query = """
            SELECT p.ID, p.ProduktID, p.Menge, p.Preis, k.AuftragsID, k.KundenID, k.Bestelldatum, k.Lieferdatum, k.Auftragsvolumen
            FROM tb_Kundenauftragspositionen p
            JOIN tb_Kundenauftrag k ON p.AuftragsID = k.AuftragsID;
        """
        source_cursor.execute(query)
        result = source_cursor.fetchall()
        df = pd.DataFrame(result)
        source_cursor.close()
        source_conn.close()
        print("Verkaufsdaten erfolgreich extrahiert.")
        return df
    except mysql.connector.Error as err:
        print(f"Fehler beim Abrufen der Verkaufsdaten: {err}")
        return None

# Hilfsfunktion zur Verarbeitung der ZeitID
def get_or_insert_time_id(date_value, dest_cursor):
    date_obj = datetime.strptime(date_value, "%Y-%m-%d") if isinstance(date_value, str) else date_value
    zeit_id = date_obj.strftime("%Y-%m-%d:00-00")
    query_check = "SELECT ZeitID FROM Zeit WHERE ZeitID = %s;"
    dest_cursor.execute(query_check, (zeit_id,))
    result = dest_cursor.fetchone()
    if result:
        return result[0]
    else:
        insert_query = """
            INSERT INTO Zeit (ZeitID, Datum, Uhrzeit, Jahr, Monat, Q, Wochentag)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        data_tuple = (zeit_id, date_obj, "00:00:00", date_obj.year, date_obj.month, (date_obj.month - 1) // 3 + 1, date_obj.strftime("%A"))
        dest_cursor.execute(insert_query, data_tuple)
        print(f"Neue ZeitID {zeit_id} wurde eingefügt.")
        return zeit_id

# Laden der Produktionsdaten ins DWH
def load_production_data(df):
    print("Lade Produktionsdaten in das DWH...")
    try:
        dest_conn = mysql.connector.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database="database-dwh")
        dest_cursor = dest_conn.cursor()
        insert_query = """
            INSERT INTO Fakt_Produktionsauftrag (ProduktID, MaschinenID, ZeitID, Auslastung, Produktionsmenge, Ausschussmenge, Verbrauch)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE Auslastung = VALUES(Auslastung), Produktionsmenge = VALUES(Produktionsmenge), Ausschussmenge = VALUES(Ausschussmenge), Verbrauch = VALUES(Verbrauch);
        """
        for _, row in df.iterrows():
            start_time = row['Startzeit']
            time_id = f"{start_time.date()}:{start_time.strftime('%H-%M')}"
            data_tuple = (row['ProduktID'], row['MaschinenID'], time_id, row['Auslastung'], row['Produktionsmenge'], row['Ausschussmenge'], row['Verbrauch'])
            dest_cursor.execute(insert_query, data_tuple)
        dest_conn.commit()
        print("Produktionsdaten erfolgreich in das DWH geladen.")
        dest_cursor.close()
        dest_conn.close()
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Produktionsdaten: {err}")

# Laden der Verkaufsdaten ins DWH
def load_sales_data(df):
    print("Lade Verkaufsdaten in das DWH...")
    try:
        dest_conn = mysql.connector.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database="database-dwh")
        dest_cursor = dest_conn.cursor()
        insert_query = """
            INSERT INTO Fakt_Sales (AuftragsID, KundenID, Bestelldatum, Lieferdatum, ProduktID, Menge, Preis, Auftragsvolumen)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE Bestelldatum = VALUES(Bestelldatum), Lieferdatum = VALUES(Lieferdatum), Menge = VALUES(Menge), Preis = VALUES(Preis), Auftragsvolumen = VALUES(Auftragsvolumen);
        """
        for _, row in df.iterrows():
            bestellzeit_id = get_or_insert_time_id(row['Bestelldatum'], dest_cursor)
            lieferzeit_id = get_or_insert_time_id(row['Lieferdatum'], dest_cursor)
            data_tuple = (row['AuftragsID'], row['KundenID'], bestellzeit_id, lieferzeit_id, row['ProduktID'], row['Menge'], row['Preis'], row['Auftragsvolumen'])
            dest_cursor.execute(insert_query, data_tuple)
        dest_conn.commit()
        print("Verkaufsdaten erfolgreich in das DWH geladen.")
        dest_cursor.close()
        dest_conn.close()
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Verkaufsdaten: {err}")

# Lagerbestand-Snapshot
def daily_snapshot_lagerbestand(conn):
    print("Erstelle täglichen Snapshot des Lagerbestands...")
    cursor = conn.cursor()

    today = datetime.now().date()
    zeitid = today.strftime("%Y-%m-%d:00-00")

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

# Hauptprogramm
if __name__ == "__main__":
    db_conn = mysql.connector.connect(host=HOST, port=PORT, user=USER, password=PASSWORD)
    
    df_production = fetch_production_data()
    if df_production is not None and not df_production.empty:
        load_production_data(df_production)
    
    df_sales = fetch_sales_data()
    if df_sales is not None and not df_sales.empty:
        load_sales_data(df_sales)

    daily_snapshot_lagerbestand(db_conn)
    db_conn.close()
