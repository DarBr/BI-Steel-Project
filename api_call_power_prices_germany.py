import requests
import pandas as pd
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
        return df
    else:
        print("Keine Daten zum Speichern.")
        return None

if __name__ == "__main__":
    data = fetch_energy_data()
    if data:
        df = save_to_dataframe(data)
        if df is not None:
            print(df)