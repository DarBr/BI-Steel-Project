import requests
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import mysql.connector

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
                INSERT INTO Energiepreise (Zeit, Strompreis)
                VALUES (%s, %s)
            """
            for _, row in df.iterrows():
                cursor.execute(insert_query, (row['zeit'], row['preis_pro_stunde']))
            connection.commit()
            print("Daten erfolgreich gespeichert.")
            cursor.close()
            connection.close()
    
    except mysql.connector.Error as err:
        print(f"Fehler: {err}")

def plot_energy_prices(df):
    plt.figure(figsize=(10, 5))
    plt.plot(df['zeit'], df['preis_pro_stunde'], marker='o', linestyle='-')
    plt.xlabel('Zeit')
    plt.ylabel('Preis pro Stunde (ct/kWh)')
    plt.title('Strompreis pro Stunde')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    data = fetch_energy_data()
    if data:
        df = save_to_dataframe(data)
        if df is not None:
            print(df)
            save_to_db(df)
            #plot_energy_prices(df)