import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import os

# .env-Datei laden
load_dotenv()

# Verbindungseinstellungen aus .env
HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT", 3306))
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("PASSWORD")
DATABASE_SOURCE = os.getenv("DATABASE_SOURCE")
DATABASE_DEST = os.getenv("DATABASE_DEST")

# Extraktion Maschinendaten
def fetch_machine_data():
    print("Starte Extraktion der Maschinendaten...")
    try:
        source_conn = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE_SOURCE
        )
        source_cursor = source_conn.cursor(dictionary=True)

        query = """
            SELECT MaschinenID, Typ, Wartungsstatus, Verbrauch, Produktionskapazität
            FROM tb_Maschine;
        """
        source_cursor.execute(query)
        result = source_cursor.fetchall()
        df = pd.DataFrame(result)

        source_cursor.close()
        source_conn.close()

        print("Maschinendaten erfolgreich extrahiert.")
        return df

    except mysql.connector.Error as err:
        print(f"Fehler beim Abrufen der Maschinendaten: {err}")
        return None

# Laden der Maschinendaten ins DWH
def load_machine_data(df):
    print("Lade Maschinendaten in das DWH...")
    try:
        dest_conn = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE_DEST
        )
        dest_cursor = dest_conn.cursor()

        insert_query = """
            INSERT INTO Dim_Maschine (MaschinenID, Typ, Standort, Wartungsstatus, Energieverbrauch)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Typ = VALUES(Typ),
                Standort = VALUES(Standort),
                Wartungsstatus = VALUES(Wartungsstatus),
                Energieverbrauch = VALUES(Energieverbrauch);
        """

        for _, row in df.iterrows():
            standort = "Unbekannt"
            data_tuple = (
                row['MaschinenID'],
                row['Typ'],
                standort,
                row['Wartungsstatus'],
                row['Verbrauch']
            )
            dest_cursor.execute(insert_query, data_tuple)

        dest_conn.commit()
        print("Maschinendaten erfolgreich in das DWH geladen.")

        dest_cursor.close()
        dest_conn.close()

    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Maschinendaten: {err}")

# Hauptprogramm
if __name__ == "__main__":
    df_machines = fetch_machine_data()
    if df_machines is not None and not df_machines.empty:
        load_machine_data(df_machines)
    else:
        print("Keine Maschinendaten zum Laden gefunden.")
