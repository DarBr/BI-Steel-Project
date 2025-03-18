import mysql.connector
import pandas as pd

# Verbindungseinstellungen (anpassen!)
HOST = "13.60.244.59"
PORT = 3306
USER = "user"
PASSWORD = "clientserver"

def fetch_machine_data():
    """
    Extrahiert alle relevanten Maschinendaten aus der Quell-Datenbank (database-steel).
    """
    try:
        # Verbindung zur Quell-Datenbank herstellen
        source_conn = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database="database-steel"
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
        
        return df

    except mysql.connector.Error as err:
        print(f"Fehler beim Abrufen der Daten aus der Quelle: {err}")
        return None

def load_machine_data(df):
    """
    Lädt die Maschinendaten in die Ziel-Datenbank (database-dwh) in die Tabelle Dim_Maschine.
    Wenn die MaschinenID bereits existiert, werden die Daten aktualisiert.
    """
    try:
        # Verbindung zur Ziel-Datenbank herstellen
        dest_conn = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database="database-dwh"
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
        
        # Hier musst du den Standort festlegen. Zum Beispiel: 'Unbekannt'
        for _, row in df.iterrows():
            data_tuple = (
                row['MaschinenID'],
                row['Typ'],
                "Unbekannt",  # Hier kannst du den Standort nach Bedarf anpassen
                row['Wartungsstatus'],
                row['Verbrauch']  # 'Energieverbrauch' anstatt 'Verbrauch' in Dim_Maschine
            )
            dest_cursor.execute(insert_query, data_tuple)
        
        dest_conn.commit()
        print("Daten wurden erfolgreich in das DWH geladen.")
        
        dest_cursor.close()
        dest_conn.close()
    
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Daten in das DWH: {err}")

if __name__ == "__main__":
    # Extraktion der Maschinendaten aus der Quell-Datenbank
    df_machines = fetch_machine_data()
    if df_machines is not None and not df_machines.empty:
        print("Daten aus der Quell-Datenbank:")
        print(df_machines)
        # Laden der Daten in das DWH
        load_machine_data(df_machines)
    else:
        print("Keine Daten zum Laden gefunden.")
