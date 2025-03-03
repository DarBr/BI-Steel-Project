import requests
import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
from datetime import datetime

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
        df_hourly = df.resample('H', on='zeit').mean().reset_index()
        df_hourly.rename(columns={'preis': 'preis_pro_stunde'}, inplace=True)
        
        return df_hourly
    else:
        print("Keine Daten zum Speichern.")
        return None

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
                INSERT INTO Energiepreise (Zeit, Energiepreis)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE Energiepreis = VALUES(Energiepreis);
            """
            for _, row in df.iterrows():
                cursor.execute(insert_query, (row['zeit'], row['preis_pro_stunde']))
            connection.commit()
            print("Daten erfolgreich gespeichert oder aktualisiert.")
            cursor.close()
            connection.close()
    
    except mysql.connector.Error as err:
        print(f"Fehler: {err}")

def read_from_db():
    try:
        connection = mysql.connector.connect(
            host="3.142.199.164",
            port=3306,
            user="user",
            password="clientserver",
            database="database-dwh"
        )
        
        if connection.is_connected():
            query = "SELECT Zeit, Energiepreis FROM Energiepreise ORDER BY Zeit ASC;"
            df = pd.read_sql(query, con=connection)
            connection.close()
            return df
    except mysql.connector.Error as err:
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

if __name__ == "__main__":
    data = fetch_energy_data()
    if data:
        df = save_to_dataframe(data)
        if df is not None:
            print(df)
            save_to_db(df)
    
    #df_db = read_from_db()
    #plot_energy_prices(df_db)