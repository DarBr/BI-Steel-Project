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
        source_conn = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database="database-steel"
        )
        source_cursor = source_conn.cursor(dictionary=True)
        
        query = """
            SELECT ID, ProduktID, MaterialID, Verhaeltnis 
            FROM tb_MaterialZuProdukt;
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
    Lädt die Materialdaten in die Ziel-Datenbank (database-dwh) in die Tabelle ZuordnungProduktMaterial.
    """
    try:
        dest_conn = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database="database-dwh"
        )
        dest_cursor = dest_conn.cursor()
        
        # Leere die Tabelle vor dem Laden (Full Load)
        dest_cursor.execute("DELETE FROM ZuordnungProduktMaterial;")
        
        # SQL-Query zum Einfügen der Daten
        insert_query = """
            INSERT INTO ZuordnungProduktMaterial (ID, ProduktID, MaterialID, Verhaeltnis)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                Verhaeltnis = VALUES(Verhaeltnis);
        """
        
        for _, row in df.iterrows():
            data_tuple = (row['ID'], row['ProduktID'], row['MaterialID'], row['Verhaeltnis'])
            dest_cursor.execute(insert_query, data_tuple)
        
        dest_conn.commit()
        print("Daten wurden erfolgreich in das DWH geladen.")
        
        dest_cursor.close()
        dest_conn.close()
    
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Daten in das DWH: {err}")
        
if __name__ == "__main__":
    # Extraktion der Materialdaten aus der Quell-Datenbank
    df_material = fetch_material_data()
    if df_material is not None and not df_material.empty:
        print("Daten aus der Quell-Datenbank:")
        print(df_material)
        # Laden der Daten in das DWH
        load_material_data(df_material)
    else:
        print("Keine Daten zum Laden gefunden.")
