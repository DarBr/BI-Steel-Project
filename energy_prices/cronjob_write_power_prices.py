import requests
import pandas as pd
import mysql.connector
import os
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pytz
from pytz import timezone

# Umleitungen von print in eine Datei (Standardausgabe) im Cronjob
def print_to_log(message):
    print(message)

def fetch_energy_data():
    url = "https://apis.smartenergy.at/market/v1/price"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Wird eine Ausnahme bei Fehlerstatus auslösen
        print_to_log("Daten erfolgreich abgerufen.")
        return response.json()
    except requests.exceptions.HTTPError as errh:
        print_to_log(f"HTTP Fehler: {errh}")
    except requests.exceptions.RequestException as err:
        print_to_log(f"Fehler beim Abrufen der Daten: {err}")
    return None

def save_to_dataframe(data):
    if data and 'data' in data:
        records = data['data']
        df = pd.DataFrame(records)
        df.rename(columns={'date': 'zeit', 'value': 'preis'}, inplace=True)
        
        # Konvertiere Zeit und konvertiere Zeitzone zu CET
        df['zeit'] = pd.to_datetime(df['zeit'], utc=True).dt.tz_convert('Europe/Vienna')
        
        # Gruppiere nach Stunde
        df_hourly = df.resample('h', on='zeit').mean().reset_index()
        df_hourly.rename(columns={'preis': 'preis_pro_stunde'}, inplace=True)
        
        print_to_log("Daten erfolgreich in DataFrame umgewandelt.")
        return df_hourly
    else:
        print_to_log("Keine Daten zum Speichern.")
    return None

def save_time_to_db(cursor, timestamp):
    ZeitID = timestamp.strftime('%Y-%m-%d:%H') + "-00"
    datum = timestamp.date()
    uhrzeit = timestamp.time()
    jahr = timestamp.year
    monat = timestamp.month
    quartal = (monat - 1) // 3 + 1
    wochentag = timestamp.strftime('%A')

    cursor.execute("SELECT COUNT(*) FROM Zeit WHERE ZeitID = %s", (ZeitID,))
    if cursor.fetchone()[0] == 0:
        insert_query = """
            INSERT INTO Zeit (ZeitID, Datum, Uhrzeit, Jahr, Monat, Q, Wochentag)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (ZeitID, datum, uhrzeit, jahr, monat, quartal, wochentag))
        print_to_log(f"Zeit {ZeitID} erfolgreich in Datenbank gespeichert.")

def save_to_db(df):
    try:
        connection = mysql.connector.connect(
            host="13.60.244.59",
            port=3306,
            user="user",
            password="clientserver",
            database="database-dwh"
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            insert_query = """
                INSERT INTO Fakt_Energiepreise (Energiepreis, ZeitID)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE Energiepreis = VALUES(Energiepreis);
            """
            for _, row in df.iterrows():
                timestamp = row['zeit'].to_pydatetime()
                ZeitID = timestamp.strftime('%Y-%m-%d:%H') + "-00"
                save_time_to_db(cursor, timestamp)
                cursor.execute(insert_query, (row['preis_pro_stunde'], ZeitID))
            connection.commit()
            print_to_log("Daten erfolgreich in die Datenbank gespeichert oder aktualisiert.")
            cursor.close()
            connection.close()
    
    except mysql.connector.Error as err:
        print_to_log(f"Datenbankfehler: {err}")

def save_to_csv(df):
    # Aktuelles Arbeitsverzeichnis überprüfen
    current_working_dir = os.getcwd()
    print(f"Aktueller Arbeitsordner: {current_working_dir}")

    # Basisverzeichnis relativ zum Skriptverzeichnis setzen
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Verzeichnis des Skripts
    base_dir = os.path.join(script_dir, "..", "machine_learning")  # Ein Verzeichnis nach oben
    filename = os.path.join(base_dir, "energy_prices_data.csv")

    print(f"Speichere Datei hier: {filename}")  # Debug-Print zur Überprüfung des Pfads

    # Falls das Verzeichnis nicht existiert, erstelle es
    if not os.path.exists(base_dir):
        print(f"Verzeichnis existiert nicht, wird erstellt: {base_dir}")
        os.makedirs(base_dir)

    # Existierende Daten laden (falls vorhanden)
    if os.path.exists(filename):
        print(f"Datei existiert, lade bestehende Daten: {filename}")
        existing_df = pd.read_csv(filename, sep=';', header=None, dtype=str)
    else:
        print(f"Datei existiert nicht, erstelle neue Datei: {filename}")
        existing_df = pd.DataFrame()

    # Neue Daten vorbereiten
    new_entries = []
    for _, row in df.iterrows():
        # Zeitangaben formatieren
        cet_time = row['zeit'].astimezone(timezone('Europe/Vienna'))
        date_str = cet_time.strftime('%d.%m.%Y')
        start_time = cet_time.strftime('%H:%M')
        end_time = (cet_time + timedelta(hours=1)).strftime('%H:%M')
        
        # Preis formatieren
        price_str = f"{row['preis_pro_stunde']:.2f}".replace('.', ',')
        
        # CSV-Zeile erstellen
        csv_line = f"{date_str};{start_time};CET;{end_time};CET;{price_str}"

        # Prüfen, ob die Zeile bereits existiert (Vermeidung von Duplikaten)
        if not existing_df.empty:
            exists = ((existing_df[0] == date_str) & (existing_df[1] == start_time)).any()
            if not exists:
                new_entries.append(csv_line)
        else:
            new_entries.append(csv_line)

    # Überprüfen, ob tatsächlich neue Einträge vorliegen
    if new_entries:
        # Wenn die Datei noch leer ist, schreibe den Header, bevor die neuen Daten angehängt werden
        with open(filename, 'a', encoding='utf-8') as f:  # 'a' = Anhängen, keine Überschreibung
            # Wenn die Datei noch leer ist, schreibe den Header
            if existing_df.empty:
                f.write("Datum;Startzeit;Zeitzone;Endzeit;Zeitzone;Preis\n")
            # Füge die neuen Einträge hinzu
            f.writelines([line + "\n" for line in new_entries])  # Mehrere Zeilen anhängen
        print_to_log(f"{len(new_entries)} neue Einträge in CSV gespeichert.")
    else:
        print_to_log("Keine neuen Daten für CSV.")



if __name__ == "__main__":
    print_to_log("Skript gestartet.")
    data = fetch_energy_data()
    if data:
        df = save_to_dataframe(data)
        if df is not None:
            print_to_log("Aktuelle Daten:")
            print_to_log(df)
            save_to_db(df)
            save_to_csv(df)
    else:
        print_to_log("Daten konnten nicht abgerufen werden.")
