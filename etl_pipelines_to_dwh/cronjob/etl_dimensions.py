import mysql.connector
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

# Datenbank-Zugangsdaten
HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT"))
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("PASSWORD")
DB_SOURCE = os.getenv("DATABASE_SOURCE")
DB_DEST = os.getenv("DATABASE_DEST")

# Funktion zum Abrufen von Kundendaten
def fetch_customer_data():
    try:
        source_conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database=DB_SOURCE
        )
        source_cursor = source_conn.cursor(dictionary=True)
        source_cursor.execute("SELECT KundenID, Firma, Straße, PLZ, Stadt, Land FROM tb_Kunde;")
        df = pd.DataFrame(source_cursor.fetchall())
        source_cursor.close()
        source_conn.close()
        return df
    except mysql.connector.Error as err:
        print(f"Fehler beim Abrufen der Kundendaten: {err}")
        return None

def load_customer_data(df):
    try:
        dest_conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database=DB_DEST
        )
        dest_cursor = dest_conn.cursor()
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
            dest_cursor.execute(insert_query, (
                row['KundenID'], row['Firma'], row['Straße'], row['PLZ'], row['Stadt'], row['Land']
            ))
        dest_conn.commit()
        print("Kundendaten erfolgreich geladen.")
        dest_cursor.close()
        dest_conn.close()
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Kundendaten: {err}")

def fetch_machine_data():
    try:
        conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database=DB_SOURCE
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT MaschinenID, Typ, Wartungsstatus, Verbrauch, Produktionskapazität FROM tb_Maschine;")
        df = pd.DataFrame(cursor.fetchall())
        cursor.close()
        conn.close()
        return df
    except mysql.connector.Error as err:
        print(f"Fehler beim Abrufen der Maschinendaten: {err}")
        return None

def load_machine_data(df):
    try:
        conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database=DB_DEST
        )
        cursor = conn.cursor()
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
            cursor.execute(insert_query, (
                row['MaschinenID'], row['Typ'], "Unbekannt", row['Wartungsstatus'], row['Verbrauch']
            ))
        conn.commit()
        print("Maschinendaten erfolgreich geladen.")
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Maschinendaten: {err}")

def fetch_material_data():
    try:
        conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database=DB_SOURCE
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT MaterialID, Name, Einheit FROM tb_Material;")
        df = pd.DataFrame(cursor.fetchall())
        cursor.close()
        conn.close()
        return df
    except mysql.connector.Error as err:
        print(f"Fehler beim Abrufen der Materialdaten: {err}")
        return None

def load_material_data(df):
    try:
        conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database=DB_DEST
        )
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO Dim_Material (MaterialID, Materialname, Einheit)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Materialname = VALUES(Materialname),
                Einheit = VALUES(Einheit);
        """
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row['MaterialID'], row['Name'], row['Einheit']
            ))
        conn.commit()
        print("Materialdaten erfolgreich geladen.")
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Materialdaten: {err}")

def fetch_product_data():
    try:
        conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database=DB_SOURCE
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT ProduktID, Name, Produkttyp, PreisProEinheit FROM tb_Produkt;")
        df = pd.DataFrame(cursor.fetchall())
        cursor.close()
        conn.close()
        return df
    except mysql.connector.Error as err:
        print(f"Fehler beim Abrufen der Produktdaten: {err}")
        return None

def load_product_data(df):
    try:
        conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database=DB_DEST
        )
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO Dim_Produkt (ProduktID, Name, Produkttyp, PreisProEinheit)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Name = VALUES(Name),
                Produkttyp = VALUES(Produkttyp),
                PreisProEinheit = VALUES(PreisProEinheit);
        """
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row['ProduktID'], row['Name'], row['Produkttyp'], row['PreisProEinheit']
            ))
        conn.commit()
        print("Produktdaten erfolgreich geladen.")
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Produktdaten: {err}")

# Hauptfunktion
if __name__ == "__main__":
    print("Starte Extraktion und Laden der Daten...")

    df_customers = fetch_customer_data()
    if df_customers is not None and not df_customers.empty:
        load_customer_data(df_customers)

    df_machines = fetch_machine_data()
    if df_machines is not None and not df_machines.empty:
        load_machine_data(df_machines)

    df_materials = fetch_material_data()
    if df_materials is not None and not df_materials.empty:
        load_material_data(df_materials)

    df_products = fetch_product_data()
    if df_products is not None and not df_products.empty:
        load_product_data(df_products)

    print("ETL-Prozess abgeschlossen.")
