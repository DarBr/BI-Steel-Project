import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

# Verbindungseinstellungen (anpassen!)
HOST = "13.60.244.59"
PORT = 3306
USER = "user"
PASSWORD = "clientserver"

def fetch_production_data():
    """
    Extrahiert alle relevanten Produktionsdaten aus der Quell-Datenbank (database-steel).
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
            SELECT ProduktionsID, MaschinenID, Startzeit, Produktionsmenge, Ausschussmenge, ProduktID, Auslastung
            FROM tb_Produktionsauftrag;
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

def load_production_data(df):
    """
    Lädt die Produktionsdaten in die Ziel-Datenbank (database-dwh) in die Tabelle Fakt_Produktionsauftrag.
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

        # Abfrage zum Prüfen, ob die ZeitID bereits existiert
        query_check_time_id = """
            SELECT ZeitID 
            FROM Zeit
            WHERE ZeitID = %s;
        """

        # Abfrage zum Einfügen einer neuen ZeitID in die Zeit-Tabelle
        query_insert_time = """
            INSERT INTO Zeit (ZeitID, Datum, Uhrzeit, Jahr, Monat, Q, Wochentag)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        
        # Einfügen der Produktionsdaten in die Fakt_Produktionsauftrag Tabelle
        insert_query = """
            INSERT INTO Fakt_Produktionsauftrag (ProduktID, MaschinenID, ZeitID, Auslastung, Produktionsmenge, Ausschussmenge)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        
        for _, row in df.iterrows():
            # Extrahiere Datum und Uhrzeit von der Startzeit
            start_time = row['Startzeit']
            date_part = start_time.date()  # Extrahiere das Datum
            time_part = start_time.strftime('%H:%M:%S')  # Extrahiere die Uhrzeit im Format 'HH:MM:SS'
            
            # Erstelle die ZeitID (Format: 'YYYY-MM-DD:HH-MM')
            time_id = f"{date_part}:{time_part[:2]}-{time_part[3:5]}"

            # Prüfe, ob die ZeitID bereits existiert
            dest_cursor.execute(query_check_time_id, (time_id,))
            time_id_result = dest_cursor.fetchone()

            if not time_id_result:
                # Falls die ZeitID nicht existiert, füge sie in die Zeit-Tabelle ein
                year = date_part.year
                month = date_part.month
                week = start_time.strftime('%U')  # Wochennummer
                weekday = start_time.strftime('%A')  # Wochentag
                
                # Füge die neue ZeitID in die Zeit-Tabelle ein
                dest_cursor.execute(query_insert_time, (time_id, date_part, time_part, year, month, week, weekday))
                print(f"Neue ZeitID {time_id} wurde in die Zeit-Tabelle eingefügt.")
            
            # Verwende die ZeitID (die bereits existieren sollte oder eben gerade eingefügt wurde)
            data_tuple = (
                row['ProduktID'],
                row['MaschinenID'],
                time_id,  # Verknüpfung mit ZeitID
                row['Auslastung'],
                row['Produktionsmenge'],
                row['Ausschussmenge']
            )
            
            # Füge die Produktionsdaten in die Fakt_Produktionsauftrag Tabelle ein
            dest_cursor.execute(insert_query, data_tuple)

        dest_conn.commit()
        print("Daten wurden erfolgreich in das DWH geladen.")
        
        dest_cursor.close()
        dest_conn.close()
    
    except mysql.connector.Error as err:
        print(f"Fehler beim Laden der Daten in das DWH: {err}")
        
def plot_machine_utilization(df):
    """
    Plottet die Auslastung jeder Maschine an einem bestimmten Tag.
    """
    # Extrahiere die Stunde von der Startzeit und gruppiere nach MaschinenID
    df['Stunde'] = df['Startzeit'].apply(lambda x: x.hour)
    df_grouped = df.groupby(['MaschinenID', 'Stunde']).agg({'Auslastung': 'mean'}).reset_index()

    # Erstelle das Diagramm
    plt.figure(figsize=(10, 6))
    for machine_id in df_grouped['MaschinenID'].unique():
        machine_data = df_grouped[df_grouped['MaschinenID'] == machine_id]
        plt.plot(machine_data['Stunde'], machine_data['Auslastung'], label=f'Maschine {machine_id}')

    plt.title('Auslastung der Maschinen am 2025-01-01')
    plt.xlabel('Stunden des Tages')
    plt.ylabel('Auslastung')
    plt.legend(title="Maschinen")
    plt.grid(True)
    plt.xticks(range(0, 24, 1))
    plt.tight_layout()

    # Zeige das Diagramm
    plt.show()
    
    


if __name__ == "__main__":
    # Extraktion der Produktionsdaten aus der Quell-Datenbank
    df_production = fetch_production_data()
    if df_production is not None and not df_production.empty:
        # Visualisierung der Auslastung der Maschinen
        # plot_machine_utilization(df_production)
        
        print("Daten aus der Quell-Datenbank:")
        print(df_production)
        # Laden der Daten in das DWH
        load_production_data(df_production)
    else:
        print("Keine Daten zum Laden gefunden.")
