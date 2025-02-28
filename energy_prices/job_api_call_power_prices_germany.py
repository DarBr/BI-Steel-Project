import requests
import pandas as pd
import mysql.connector
from datetime import datetime
import schedule
import time
import logging

logging.basicConfig(level=logging.INFO)

def fetch_energy_data():
    url = "https://apis.smartenergy.at/market/v1/price"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Fehler beim Abrufen der Daten: {response.status_code}")
        return None

def save_to_dataframe(data):
    if data and 'data' in data:
        records = data['data']
        df = pd.DataFrame(records)
        df.rename(columns={'date': 'zeit', 'value': 'preis'}, inplace=True)
        
        # Konvertiere die Zeitspalte in ein datetime-Format
        df['zeit'] = pd.to_datetime(df['zeit'])
        
        # Gruppiere nach Stunde und berechne den Durchschnittspreis pro Stunde
        df_hourly = df.resample('H', on='zeit').mean().reset_index()
        df_hourly.rename(columns={'preis': 'preis_pro_stunde'}, inplace=True)
        
        return df_hourly
    else:
        logging.error("Keine Daten zum Speichern.")
        return None

def save_to_db(df):
    try:
        connection = mysql.connector.connect(
            host="3.142.199.164",
            port=3306,
            user="user",
            password="clientserver",
            database="database-steel"
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            for _, row in df.iterrows():
                # Überprüfen, ob ein Eintrag für die gegebene Zeit existiert
                select_query = "SELECT COUNT(*) FROM Energiepreise WHERE zeit = %s"
                cursor.execute(select_query, (row['zeit'],))
                result = cursor.fetchone()
                
                if result[0] > 0:
                    # Aktualisieren, wenn ein Eintrag existiert
                    update_query = """
                        UPDATE Energiepreise
                        SET Strompreis = %s
                        WHERE Zeit = %s
                    """
                    cursor.execute(update_query, (row['preis_pro_stunde'], row['zeit']))
                    logging.info(f"Eintrag für {row['zeit']} aktualisiert.")
                else:
                    # Einfügen, wenn kein Eintrag existiert
                    insert_query = """
                        INSERT INTO Energiepreise (Zeit, Strompreis)
                        VALUES (%s, %s)
                    """
                    cursor.execute(insert_query, (row['zeit'], row['preis_pro_stunde']))
                    logging.info(f"Neuer Eintrag für {row['zeit']} erstellt.")
            
            connection.commit()
            logging.info("Daten erfolgreich gespeichert.")
            cursor.close()
            connection.close()
    
    except mysql.connector.Error as err:
        logging.error(f"Fehler: {err}")

def job():
    data = fetch_energy_data()
    if data:
        df = save_to_dataframe(data)
        if df is not None:
            logging.info("Daten zum Speichern vorbereitet.")
            save_to_db(df)

schedule.every().day.at("00:00").do(job)

if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(1)
