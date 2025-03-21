import mysql.connector
import pandas as pd

# Verbindungseinstellungen (anpassen!)
HOST = "13.60.244.59"
PORT = 3306
USER = "user"
PASSWORD = "clientserver"

def fetch_product_data():
    """
    Extrahiert alle relevanten Produktdaten aus der Quell-Datenbank (database-steel).
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
            SELECT ProduktID, Name, Produkttyp, PreisProEinheit
            FROM tb_Produkt;
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

def load_product_data(df):
    """
    Lädt die Produktdaten in die Ziel-Datenbank (database-dwh) in die Tabelle Dim_Produkt.
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

        # Option 1: Full Load – bestehende Daten in Dim_Produkt löschen
        # dest_cursor.execute("TRUNCATE TABLE Dim_Produkt;")  # Entferne dies, wenn du nur hinzufügen möchtest

        insert_query = """
            INSERT INTO Dim_Produkt (ProduktID, Name, Produkttyp, PreisProEinheit)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Name = VALUES(Name),
                Produkttyp = VALUES(Produkttyp),
                PreisProEinheit = VALUES(PreisProEinheit);
        """
        
        for _, row in df.iterrows():
            data_tuple = (
                row['ProduktID'],         # ProduktID
                row['Name'],              # Name
                row['Produkttyp'],        # Produkttyp
                row['PreisProEinheit']    # PreisProEinheit
            )
            dest_cursor.execute(insert_query, data_tuple)
        
        dest_conn.commit()
        print("Daten wurden erfolgreich in das DWH geladen.")
        
        dest_cursor.close()
        dest_conn.close()
    
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Daten in das DWH: {err}")

if __name__ == "__main__":
    # Extraktion der Produktdaten aus der Quell-Datenbank
    df_products = fetch_product_data()
    if df_products is not None and not df_products.empty:
        print("Daten aus der Quell-Datenbank:")
        print(df_products)
        # Laden der Daten in das DWH
        load_product_data(df_products)
    else:
        print("Keine Daten zum Laden gefunden.")
