import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import os

# .env-Datei laden
load_dotenv()

HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT"))
DB_USER = os.getenv("DB_USER")
PASSWORD = os.getenv("PASSWORD")
DATABASE_SOURCE = os.getenv("DATABASE_SOURCE")
DATABASE_DEST = os.getenv("DATABASE_DEST")

def fetch_customer_data():
    """
    Extrahiert alle relevanten Kundendaten aus der Quell-Datenbank (database-steel).
    """
    try:
        # Verbindung zur Quell-Datenbank herstellen
        source_conn = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=DB_USER,
            password=PASSWORD,
            database=DATABASE_SOURCE
        )
        source_cursor = source_conn.cursor(dictionary=True)
        
        query = """
            SELECT KundenID, Firma, Straße, PLZ, Stadt, Land
            FROM tb_Kunde;
        """
        source_cursor.execute(query)
        result = source_cursor.fetchall()
        df = pd.DataFrame(result)
        
        source_cursor.close()
        source_conn.close()
        
        return df

    except mysql.connector.Error as err:
        print(f"Fehler beim Abrufen der Daten aus der Quelle: {err}")
        return None

def load_customer_data(df):
    """
    Lädt die Kundendaten in die Ziel-Datenbank (database-dwh) in die Tabelle Dim_Kunde.
    Hier wird ein Full Load durchgeführt (Tabelle wird vor dem Laden geleert).
    """
    try:
        # Verbindung zur Ziel-Datenbank herstellen
        dest_conn = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=DB_USER,
            password=PASSWORD,
            database=DATABASE_DEST
        )
        dest_cursor = dest_conn.cursor()

        # Option 1: Full Load – bestehende Daten in Dim_Kunde löschen
        # dest_cursor.execute("TRUNCATE TABLE Dim_Kunde;")  # Entferne dies, wenn du nur hinzufügen möchtest

        insert_query = """
            INSERT INTO Dim_Kunde (KundenID, Firma, Straße, PLZ, Stadt, Land)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Firma = VALUES(Firma),
                Straße = VALUES(Straße),
                PLZ = VALUES(PLZ),
                Stadt = VALUES(Stadt),
                Land = VALUES(Land);
        """
        for _, row in df.iterrows():
            data_tuple = (
                row['KundenID'],
                row['Firma'],
                row['Straße'],
                row['PLZ'],
                row['Stadt'],
                row['Land']
            )
            dest_cursor.execute(insert_query, data_tuple)
        
        dest_conn.commit()
        print("Daten wurden erfolgreich in das DWH geladen.")
        
        dest_cursor.close()
        dest_conn.close()
    
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Daten in das DWH: {err}")

if __name__ == "__main__":
    # Extraktion der Kundendaten aus der Quell-Datenbank
    df_customers = fetch_customer_data()
    if df_customers is not None and not df_customers.empty:
        print("Daten aus der Quell-Datenbank:")
        print(df_customers)
        # Laden der Daten in das DWH
        load_customer_data(df_customers)
    else:
        print("Keine Daten zum Laden gefunden.")
