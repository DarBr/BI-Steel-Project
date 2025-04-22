import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import os

# .env-Datei laden
load_dotenv()

# Verbindungseinstellungen aus der .env-Datei
HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT", 3306))
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("PASSWORD")
DATABASE_SOURCE = os.getenv("DATABASE_SOURCE")
DATABASE_DEST = os.getenv("DATABASE_DEST")

def fetch_product_data():
    """
    Extrahiert alle relevanten Produktdaten aus der Quell-Datenbank (database-steel).
    """
    print("Starte Extraktion der Produktdaten...")
    try:
        # Verbindung zur Quell-Datenbank (database-steel) herstellen
        source_conn = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE_SOURCE
        )
        source_cursor = source_conn.cursor(dictionary=True)

        # SQL-Query zum Abrufen der Produktdaten
        query = """
            SELECT ProduktID, Name, Produkttyp, PreisProEinheit
            FROM tb_Produkt;
        """
        source_cursor.execute(query)
        result = source_cursor.fetchall()
        df = pd.DataFrame(result)

        source_cursor.close()
        source_conn.close()

        print("Produktdaten erfolgreich extrahiert.")
        return df

    except mysql.connector.Error as err:
        print(f"Fehler beim Abrufen der Produktdaten: {err}")
        return None

def load_product_data(df):
    """
    Lädt die Produktdaten in die Ziel-Datenbank (database-dwh) in die Tabelle Dim_Produkt.
    """
    print("Lade Produktdaten in das DWH...")
    try:
        # Verbindung zur Ziel-Datenbank (database-dwh) herstellen
        dest_conn = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE_DEST
        )
        dest_cursor = dest_conn.cursor()

        # SQL-Query zum Einfügen der Produktdaten in die Tabelle Dim_Produkt
        insert_query = """
            INSERT INTO Dim_Produkt (ProduktID, Name, Produkttyp, PreisProEinheit)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Name = VALUES(Name),
                Produkttyp = VALUES(Produkttyp),
                PreisProEinheit = VALUES(PreisProEinheit);
        """

        # Iteriere über die Daten und führe die Einfügeoperation durch
        for _, row in df.iterrows():
            data_tuple = (
                row['ProduktID'],
                row['Name'],
                row['Produkttyp'],
                row['PreisProEinheit']
            )
            dest_cursor.execute(insert_query, data_tuple)

        # Änderungen an der Ziel-Datenbank bestätigen
        dest_conn.commit()
        print("Produktdaten erfolgreich in das DWH geladen.")

        dest_cursor.close()
        dest_conn.close()

    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Produktdaten: {err}")

if __name__ == "__main__":
    # Extrahieren der Produktdaten aus der Quell-Datenbank
    df_products = fetch_product_data()
    if df_products is not None and not df_products.empty:
        # Wenn Daten vorhanden sind, lade sie in das DWH
        load_product_data(df_products)
    else:
        print("Keine Produktdaten zum Laden gefunden.")
