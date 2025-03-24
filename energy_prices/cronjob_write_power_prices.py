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
        df['zeit'] = pd.to_datetime(df['zeit']).dt.tz_convert(timezone('Europe/Vienna'))
        
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
                INSERT INTO Energiepreise (Energiepreis, ZeitID)
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
    filename = "machine_learning/energy_prices_data.csv"
    
    # Existierende Daten lesen
    if os.path.exists(filename):
        existing_df = pd.read_csv(filename, sep=';', header=None, dtype=str)
    else:
        existing_df = pd.DataFrame()

    new_entries = []
    for _, row in df.iterrows():
        # Zeitangaben formatieren
        cet_time = row['zeit'].astimezone(timezone('Europe/Vienna'))
        date_str = cet_time.strftime('%d.%m.%Y')
        start_time = cet_time.strftime('%H:%M')
        end_time = (cet_time + timedelta(hours=1)).strftime('%H:%M')
        
        # Preis formatieren
        price_str = f"{row['preis_pro_stunde']:.2f}".replace('.', ',')
        
        csv_line = f"{date_str};{start_time};CET;{end_time};CET;{price_str}"
        
        # Duplikatprüfung
        if not existing_df.empty:
            exists = ((existing_df[0] == date_str) & 
                     (existing_df[1] == start_time)).any()
            if not exists:
                new_entries.append(csv_line)
        else:
            new_entries.append(csv_line)
    
    # Neue Einträge speichern
    if new_entries:
        with open(filename, 'a', encoding='utf-8') as f:
            f.write('\n'.join(new_entries) + '\n')
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
