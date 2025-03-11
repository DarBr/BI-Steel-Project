import requests
import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import time

def fetch_energy_data():
    url = "https://apis.smartenergy.at/market/v1/price"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Fehler beim Abrufen der Daten: {response.status_code}")
        return None

def save_to_dataframe(data):
    if data and 'data' in data:
        records = data['data']
        df = pd.DataFrame(records)
        df.rename(columns={'date': 'zeit', 'value': 'preis'}, inplace=True)
        df['zeit'] = pd.to_datetime(df['zeit'])
        
        # Gruppiere nach Stunde und berechne den Durchschnittspreis pro Stunde
        df_hourly = df.resample('h', on='zeit').mean().reset_index()
        df_hourly.rename(columns={'preis': 'preis_pro_stunde'}, inplace=True)
        
        return df_hourly
    else:
        print("Keine Daten zum Speichern.")
        return None

def save_time_to_db(cursor, timestamp):
    """Fügt die Zeitdaten in die Zeittabelle ein, falls diese noch nicht existiert."""
    ZeitID = timestamp.strftime('%Y-%m-%d:%H') + "-00"
    datum = timestamp.date()
    uhrzeit = timestamp.time()
    jahr = timestamp.year
    monat = timestamp.month
    quartal = (monat - 1) // 3 + 1
    wochentag = timestamp.strftime('%A')

    # Prüfen, ob ZeitID bereits existiert
    cursor.execute("SELECT COUNT(*) FROM Zeit WHERE ZeitID = %s", (ZeitID,))
    if cursor.fetchone()[0] == 0:
        # ZeitID in die Zeittabelle einfügen
        insert_query = """
            INSERT INTO Zeit (ZeitID, Datum, Uhrzeit, Jahr, Monat, Q, Wochentag)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (ZeitID, datum, uhrzeit, jahr, monat, quartal, wochentag))

def save_to_db(df):
    try:
        connection = mysql.connector.connect(
            host="3.142.199.164",
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
                timestamp = row['zeit']
                ZeitID = timestamp.strftime('%Y-%m-%d:%H') + "-00"  # ZeitID mit voller Stunde erstellen
                save_time_to_db(cursor, timestamp)  # ZeitID in Zeittabelle speichern
                cursor.execute(insert_query, (row['preis_pro_stunde'], ZeitID))
            connection.commit()
            print("Daten erfolgreich gespeichert oder aktualisiert.")
            cursor.close()
            connection.close()
    
    except mysql.connector.Error as err:
        print(f"Fehler: {err}")

def read_from_db():
    try:
        # SQLAlchemy-Engine erstellen
        db_uri = "mysql+mysqlconnector://user:clientserver@3.142.199.164:3306/database-dwh"
        engine = create_engine(db_uri)

        query = "SELECT Zeit, Energiepreis FROM Energiepreise ORDER BY Zeit ASC;"
        df = pd.read_sql(query, con=engine)  # SQLAlchemy wird jetzt genutzt!
        
        return df

    except Exception as err:
        print(f"Fehler: {err}")
        return None

def plot_energy_prices(df):
    if df is not None and not df.empty:
        plt.figure(figsize=(10, 5))
        plt.plot(df['Zeit'], df['Energiepreis'], marker='o', linestyle='-', color='b')
        plt.xlabel('Zeit')
        plt.ylabel('Energiepreis')
        plt.title('Energiepreise über die Zeit')
        plt.xticks(rotation=45)
        plt.grid()
        plt.show()
    else:
        print("Keine Daten zum Plotten verfügbar.")

def wait_until_next_run():
    """Wartet bis 18 Uhr des aktuellen Tages oder des nächsten Tages"""
    now = datetime.now()
    target_time = now.replace(hour=18, minute=0, second=0, microsecond=0)
    
    if now >= target_time:
        # Wenn es bereits nach 18 Uhr ist, warte bis zum nächsten Tag
        target_time += timedelta(days=1)
    
    # Berechne die verbleibende Zeit bis zur nächsten 18 Uhr
    wait_time = (target_time - now).total_seconds()
    
    print(f"Warte bis 18 Uhr. (Wartezeit: {wait_time} Sekunden)")
    time.sleep(wait_time)

if __name__ == "__main__":
    while True:
        wait_until_next_run()  # Warte bis 18 Uhr
        data = fetch_energy_data()
        if data:
            df = save_to_dataframe(data)
            if df is not None:
                print(df)
                save_to_db(df)
        # Option zum Plotten (kommentiere aus, wenn nicht benötigt)
        #df_db = read_from_db()
        #plot_energy_prices(df_db)
