import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Verbindungseinstellungen
HOST = "13.60.244.59"
PORT = 3306
USER = "user"
PASSWORD = "clientserver"

### SALES-DATEN ###
def fetch_sales_data():
    """ Extrahiere Verkaufsdaten aus database-steel """
    try:
        source_conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database="database-steel"
        )
        source_cursor = source_conn.cursor(dictionary=True)
        
        query = """
            SELECT p.ID, p.ProduktID, p.Menge, p.Preis, 
                   k.AuftragsID, k.KundenID, k.Bestelldatum, k.Lieferdatum, k.Auftragsvolumen
            FROM tb_Kundenauftragspositionen p
            JOIN tb_Kundenauftrag k ON p.AuftragsID = k.AuftragsID;
        """
        source_cursor.execute(query)
        result = source_cursor.fetchall()
        df = pd.DataFrame(result)
        
        source_cursor.close()
        source_conn.close()
        return df
    except mysql.connector.Error as err:
        print(f"Fehler beim Abrufen der Verkaufsdaten: {err}")
        return None

def get_or_insert_time_id(date_value, dest_cursor):
    """ Prüft, ob die ZeitID existiert, und fügt sie falls nötig ein """
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
        return zeit_id

def load_sales_data(df):
    """ Lädt Verkaufsdaten in database-dwh """
    try:
        dest_conn = mysql.connector.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database="database-dwh")
        dest_cursor = dest_conn.cursor()
        
        for _, row in df.iterrows():
            bestellzeit_id = get_or_insert_time_id(row['Bestelldatum'], dest_cursor)
            lieferzeit_id = get_or_insert_time_id(row['Lieferdatum'], dest_cursor)
            
            insert_query = """
                INSERT INTO Fakt_Sales (AuftragsID, KundenID, Bestelldatum, Lieferdatum, ProduktID, Menge, Preis, Auftragsvolumen)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE Menge=VALUES(Menge), Preis=VALUES(Preis), Auftragsvolumen=VALUES(Auftragsvolumen);
            """
            data_tuple = (row['AuftragsID'], row['KundenID'], bestellzeit_id, lieferzeit_id, row['ProduktID'], row['Menge'], row['Preis'], row['Auftragsvolumen'])
            dest_cursor.execute(insert_query, data_tuple)
        
        dest_conn.commit()
        print("Sales-Daten erfolgreich geladen.")
        dest_cursor.close()
        dest_conn.close()
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Sales-Daten: {err}")

### PRODUKTIONS-DATEN ###
def fetch_production_data():
    """ Extrahiere Produktionsdaten aus database-steel """
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
        return df
    except mysql.connector.Error as err:
        print(f"Fehler beim Abrufen der Produktionsdaten: {err}")
        return None

def load_production_data(df):
    """ Lädt Produktionsdaten in database-dwh """
    try:
        dest_conn = mysql.connector.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database="database-dwh")
        dest_cursor = dest_conn.cursor()
        
        for _, row in df.iterrows():
            start_time = row['Startzeit']
            time_id = f"{start_time.date()}:{start_time.strftime('%H-%M')}"
            
            insert_query = """
                INSERT INTO Fakt_Produktionsauftrag (ProduktID, MaschinenID, ZeitID, Auslastung, Produktionsmenge, Ausschussmenge, Verbrauch)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE Auslastung=VALUES(Auslastung), Produktionsmenge=VALUES(Produktionsmenge);
            """
            data_tuple = (row['ProduktID'], row['MaschinenID'], time_id, row['Auslastung'], row['Produktionsmenge'], row['Ausschussmenge'], row['Verbrauch'])
            dest_cursor.execute(insert_query, data_tuple)
        
        dest_conn.commit()
        print("Produktionsdaten erfolgreich geladen.")
        dest_cursor.close()
        dest_conn.close()
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Produktionsdaten: {err}")

if __name__ == "__main__":
    df_sales = fetch_sales_data()
    if df_sales is not None and not df_sales.empty:
        load_sales_data(df_sales)
    else:
        print("Keine neuen Sales-Daten gefunden.")
    
    df_production = fetch_production_data()
    if df_production is not None and not df_production.empty:
        load_production_data(df_production)
    else:
        print("Keine neuen Produktionsdaten gefunden.")
