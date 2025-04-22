import mysql.connector
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
load_dotenv()

# Verbindungseinstellungen aus .env
HOST = os.getenv("DB_HOST")
PORT = int(os.getenv("DB_PORT"))
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
SOURCE_DB = "database-steel"
DWH_DB = "database-dwh"

def fetch_production_data():
    print("Starte Extraktion der Produktionsdaten...")
    try:
        conn = mysql.connector.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database=SOURCE_DB)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT ProduktionsID, MaschinenID, Startzeit, Produktionsmenge, Ausschussmenge, ProduktID, Auslastung, Verbrauch
            FROM tb_Produktionsauftrag;
        """)
        df = pd.DataFrame(cursor.fetchall())
        cursor.close()
        conn.close()
        print("Produktionsdaten erfolgreich extrahiert.")
        return df
    except mysql.connector.Error as err:
        print(f"Fehler beim Abrufen der Produktionsdaten: {err}")
        return None

def fetch_sales_data():
    print("Starte Extraktion der Verkaufsdaten...")
    try:
        conn = mysql.connector.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database=SOURCE_DB)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT p.ID, p.ProduktID, p.Menge, p.Preis, k.AuftragsID, k.KundenID, k.Bestelldatum, k.Lieferdatum, k.Auftragsvolumen
            FROM tb_Kundenauftragspositionen p
            JOIN tb_Kundenauftrag k ON p.AuftragsID = k.AuftragsID;
        """)
        df = pd.DataFrame(cursor.fetchall())
        cursor.close()
        conn.close()
        print("Verkaufsdaten erfolgreich extrahiert.")
        return df
    except mysql.connector.Error as err:
        print(f"Fehler beim Abrufen der Verkaufsdaten: {err}")
        return None

def get_or_insert_time_id(date_value, cursor):
    date_obj = datetime.strptime(date_value, "%Y-%m-%d") if isinstance(date_value, str) else date_value
    zeit_id = date_obj.strftime("%Y-%m-%d:00-00")
    cursor.execute("SELECT ZeitID FROM Zeit WHERE ZeitID = %s;", (zeit_id,))
    if cursor.fetchone():
        return zeit_id
    insert = """
        INSERT INTO Zeit (ZeitID, Datum, Uhrzeit, Jahr, Monat, Q, Wochentag)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    data = (zeit_id, date_obj, "00:00:00", date_obj.year, date_obj.month, (date_obj.month - 1) // 3 + 1, date_obj.strftime("%A"))
    cursor.execute(insert, data)
    print(f"Neue ZeitID {zeit_id} eingefügt.")
    return zeit_id

def load_production_data(df):
    print("Lade Produktionsdaten in das DWH...")
    try:
        conn = mysql.connector.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database=DWH_DB)
        cursor = conn.cursor()
        query = """
            INSERT INTO Fakt_Produktionsauftrag (ProduktID, MaschinenID, ZeitID, Auslastung, Produktionsmenge, Ausschussmenge, Verbrauch)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Auslastung = VALUES(Auslastung),
                Produktionsmenge = VALUES(Produktionsmenge),
                Ausschussmenge = VALUES(Ausschussmenge),
                Verbrauch = VALUES(Verbrauch);
        """
        for _, row in df.iterrows():
            zeit_id = f"{row['Startzeit'].date()}:{row['Startzeit'].strftime('%H-%M')}"
            data = (row['ProduktID'], row['MaschinenID'], zeit_id, row['Auslastung'], row['Produktionsmenge'], row['Ausschussmenge'], row['Verbrauch'])
            cursor.execute(query, data)
        conn.commit()
        cursor.close()
        conn.close()
        print("Produktionsdaten erfolgreich geladen.")
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Produktionsdaten: {err}")

def load_sales_data(df):
    print("Lade Verkaufsdaten in das DWH...")
    try:
        conn = mysql.connector.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database=DWH_DB)
        cursor = conn.cursor()
        query = """
            INSERT INTO Fakt_Sales (AuftragsID, KundenID, Bestelldatum, Lieferdatum, ProduktID, Menge, Preis, Auftragsvolumen)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Bestelldatum = VALUES(Bestelldatum),
                Lieferdatum = VALUES(Lieferdatum),
                Menge = VALUES(Menge),
                Preis = VALUES(Preis),
                Auftragsvolumen = VALUES(Auftragsvolumen);
        """
        for _, row in df.iterrows():
            bestell_id = get_or_insert_time_id(row['Bestelldatum'], cursor)
            liefer_id = get_or_insert_time_id(row['Lieferdatum'], cursor)
            data = (row['AuftragsID'], row['KundenID'], bestell_id, liefer_id, row['ProduktID'], row['Menge'], row['Preis'], row['Auftragsvolumen'])
            cursor.execute(query, data)
        conn.commit()
        cursor.close()
        conn.close()
        print("Verkaufsdaten erfolgreich geladen.")
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Verkaufsdaten: {err}")

def daily_snapshot_lagerbestand():
    print("Erstelle täglichen Snapshot des Lagerbestands...")
    try:
        conn = mysql.connector.connect(host=HOST, port=PORT, user=USER, password=PASSWORD)
        cursor = conn.cursor()

        today = datetime.now().date()
        zeitid = today.strftime("%Y-%m-%d:00-00")

        select_sql = f"""
            SELECT lb.MaterialID, lb.Menge, lb.Mindestbestand
            FROM `{SOURCE_DB}`.tb_Lagerbestand lb
            JOIN (
                SELECT MaterialID, MAX(Bestandsdatum) AS MaxDatum
                FROM `{SOURCE_DB}`.tb_Lagerbestand
                WHERE Bestandsdatum <= %s
                GROUP BY MaterialID
            ) t 
            ON lb.MaterialID = t.MaterialID AND lb.Bestandsdatum = t.MaxDatum
        """
        cursor.execute(select_sql, (today,))
        rows = cursor.fetchall()

        if not rows:
            print("Keine Lagerdaten vorhanden.")
            return

        insert_sql = f"""
            INSERT INTO `{DWH_DB}`.Fakt_Lagerbestand (ZeitID, MaterialID, Menge, Mindestbestand)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE Menge = VALUES(Menge), Mindestbestand = VALUES(Mindestbestand);
        """
        for (mat_id, menge, min_bestand) in rows:
            cursor.execute(insert_sql, (zeitid, mat_id, menge, min_bestand))

        conn.commit()
        print(f"Snapshot für {zeitid} erfolgreich erstellt.")
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Fehler beim Snapshot Lagerbestand: {err}")

# Hauptprogramm
if __name__ == "__main__":
    df_production = fetch_production_data()
    if df_production is not None and not df_production.empty:
        load_production_data(df_production)

    df_sales = fetch_sales_data()
    if df_sales is not None and not df_sales.empty:
        load_sales_data(df_sales)

    daily_snapshot_lagerbestand()
