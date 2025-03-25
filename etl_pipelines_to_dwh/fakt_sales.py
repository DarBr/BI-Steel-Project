import mysql.connector
import pandas as pd
from datetime import datetime

# Verbindungseinstellungen
HOST = "13.60.244.59"
PORT = 3306
USER = "user"
PASSWORD = "clientserver"

# Extrahiere Daten aus database-steel
def fetch_sales_data():
    try:
        source_conn = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database="database-steel"
        )
        source_cursor = source_conn.cursor(dictionary=True)

        query = """
            SELECT 
                p.ID, p.ProduktID, p.Menge, p.Preis, 
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
        print(f"Fehler beim Abrufen der Daten aus database-steel: {err}")
        return None

def get_or_insert_time_id(date_value, dest_cursor):
    """ Prüft, ob die ZeitID existiert, und fügt sie falls nötig ein """
    date_obj = datetime.strptime(date_value, "%Y-%m-%d") if isinstance(date_value, str) else date_value
    
    # Format wie in Zeit.ZeitID (z. B. 2025-01-01:00-00)
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
        data_tuple = (
            zeit_id,                     # ZeitID im Format 2025-01-01:00-00
            date_obj,                    # Datum (als date-Objekt)
            "00:00:00",                  # Uhrzeit
            date_obj.year,
            date_obj.month,
            (date_obj.month - 1) // 3 + 1,
            date_obj.strftime("%A")
        )
        dest_cursor.execute(insert_query, data_tuple)
        return zeit_id


# Daten in database-dwh Fakt_Sales laden
def load_sales_data(df):
    try:
        dest_conn = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database="database-dwh"
        )
        dest_cursor = dest_conn.cursor()

        # Bestelldatum und Lieferdatum in ZeitID umwandeln
        for _, row in df.iterrows():
            bestellzeit_id = get_or_insert_time_id(row['Bestelldatum'], dest_cursor)
            lieferzeit_id = get_or_insert_time_id(row['Lieferdatum'], dest_cursor)

            insert_query = """
                INSERT INTO Fakt_Sales (AuftragsID, KundenID, Bestelldatum, Lieferdatum, ProduktID, Menge, Preis, Auftragsvolumen)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    Bestelldatum = VALUES(Bestelldatum),
                    Lieferdatum = VALUES(Lieferdatum),
                    Menge = VALUES(Menge),
                    Preis = VALUES(Preis),
                    Auftragsvolumen = VALUES(Auftragsvolumen);
            """
            data_tuple = (
                row['AuftragsID'], row['KundenID'], bestellzeit_id, lieferzeit_id,
                row['ProduktID'], row['Menge'], row['Preis'], row['Auftragsvolumen']
            )
            dest_cursor.execute(insert_query, data_tuple)

        dest_conn.commit()
        print("Daten erfolgreich in Fakt_Sales geladen.")

        dest_cursor.close()
        dest_conn.close()
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Daten in Fakt_Sales: {err}")

# Hauptablauf
if __name__ == "__main__":
    df_sales = fetch_sales_data()
    if df_sales is not None and not df_sales.empty:
        print("Daten aus database-steel:")
        print(df_sales.head())

        # 1. Daten in Fakt_Sales-Tabelle laden (inkl. Zeit-ID-Umwandlung)
        load_sales_data(df_sales)
    else:
        print("Keine neuen Daten zum Laden gefunden.")
