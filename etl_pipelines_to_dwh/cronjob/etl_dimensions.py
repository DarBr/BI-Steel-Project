import mysql.connector
import pandas as pd

# Verbindungseinstellungen (anpassen!)
HOST = "13.60.244.59"
PORT = 3306
USER = "user"
PASSWORD = "clientserver"

# Funktion zum Abrufen von Kundendaten
def fetch_customer_data():
    try:
        source_conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database="database-steel"
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
        print(f"Fehler beim Abrufen der Kundendaten: {err}")
        return None

# Funktion zum Laden von Kundendaten
def load_customer_data(df):
    try:
        dest_conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database="database-dwh"
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
            data_tuple = (
                row['KundenID'], row['Firma'], row['Straße'], row['PLZ'], row['Stadt'], row['Land']
            )
            dest_cursor.execute(insert_query, data_tuple)
        
        dest_conn.commit()
        print("Kundendaten wurden erfolgreich in das DWH geladen.")
        
        dest_cursor.close()
        dest_conn.close()
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Kundendaten: {err}")

# Funktion zum Abrufen von Maschinendaten
def fetch_machine_data():
    try:
        source_conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database="database-steel"
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
        print(f"Fehler beim Abrufen der Maschinendaten: {err}")
        return None

# Funktion zum Laden von Maschinendaten
def load_machine_data(df):
    try:
        dest_conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database="database-dwh"
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
            data_tuple = (
                row['MaschinenID'], row['Typ'], "Unbekannt", row['Wartungsstatus'], row['Verbrauch']
            )
            dest_cursor.execute(insert_query, data_tuple)
        
        dest_conn.commit()
        print("Maschinendaten wurden erfolgreich in das DWH geladen.")
        
        dest_cursor.close()
        dest_conn.close()
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Maschinendaten: {err}")

# Funktion zum Abrufen von Materialdaten
def fetch_material_data():
    try:
        source_conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database="database-steel"
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
        print(f"Fehler beim Abrufen der Materialdaten: {err}")
        return None

# Funktion zum Laden von Materialdaten
def load_material_data(df):
    try:
        dest_conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database="database-dwh"
        )
        dest_cursor = dest_conn.cursor()

        insert_query = """
            INSERT INTO Dim_Material (MaterialID, Materialname, Einheit)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Materialname = VALUES(Materialname),
                Einheit = VALUES(Einheit);
        """
        for _, row in df.iterrows():
            data_tuple = (
                row['MaterialID'], row['Name'], row['Einheit']
            )
            dest_cursor.execute(insert_query, data_tuple)
        
        dest_conn.commit()
        print("Materialdaten wurden erfolgreich in das DWH geladen.")
        
        dest_cursor.close()
        dest_conn.close()
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Materialdaten: {err}")

# Funktion zum Abrufen von Produktdaten
def fetch_product_data():
    try:
        source_conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database="database-steel"
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
        print(f"Fehler beim Abrufen der Produktdaten: {err}")
        return None

# Funktion zum Laden von Produktdaten
def load_product_data(df):
    try:
        dest_conn = mysql.connector.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD, database="database-dwh"
        )
        dest_cursor = dest_conn.cursor()

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
                row['ProduktID'], row['Name'], row['Produkttyp'], row['PreisProEinheit']
            )
            dest_cursor.execute(insert_query, data_tuple)
        
        dest_conn.commit()
        print("Produktdaten wurden erfolgreich in das DWH geladen.")
        
        dest_cursor.close()
        dest_conn.close()
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Produktdaten: {err}")

# Hauptfunktion für die Ausführung der gesamten Aufgabe
if __name__ == "__main__":
    print("Starte Extraktion und Laden der Daten...")

    # Extrahieren und Laden der Daten für alle Tabellen
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

    print("Daten wurden erfolgreich verarbeitet.")
