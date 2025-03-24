import mysql.connector
import pandas as pd

# Verbindungseinstellungen (anpassen!)
HOST = "13.60.244.59"
PORT = 3306
USER = "user"
PASSWORD = "clientserver"

def fetch_material_data():
    """
    Extrahiert alle relevanten Materialdaten aus der Quell-Datenbank (database-steel).
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
            SELECT MaterialID, Name, Einheit
            FROM tb_Material;
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

def load_material_data(df):
    """
    Lädt die Materialdaten in die Ziel-Datenbank (database-dwh) in die Tabelle Dim_Material.
    Hier wird ein Full Load durchgeführt (Tabelle wird vor dem Laden geleert).
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

        # Option 1: Full Load – bestehende Daten in Dim_Material löschen
        # dest_cursor.execute("TRUNCATE TABLE Dim_Material;")  # Entferne dies, wenn du nur hinzufügen möchtest

        insert_query = """
            INSERT INTO Dim_Material (MaterialID, Materialname, Einheit)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Materialname = VALUES(Materialname),
                Einheit = VALUES(Einheit);
        """
        
        # Setze den Anteil auf NULL, da er leer bleiben kann
        for _, row in df.iterrows():
            data_tuple = (
                row['MaterialID'],  # MaterialID
                row['Name'],        # Materialname
                row['Einheit']      # Einheit
            )
            dest_cursor.execute(insert_query, data_tuple)
        
        dest_conn.commit()
        print("Daten wurden erfolgreich in das DWH geladen.")
        
        dest_cursor.close()
        dest_conn.close()
    
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Daten in das DWH: {err}")

if __name__ == "__main__":
    # Extraktion der Materialdaten aus der Quell-Datenbank
    df_materials = fetch_material_data()
    if df_materials is not None and not df_materials.empty:
        print("Daten aus der Quell-Datenbank:")
        print(df_materials)
        # Laden der Daten in das DWH
        load_material_data(df_materials)
    else:
        print("Keine Daten zum Laden gefunden.")
