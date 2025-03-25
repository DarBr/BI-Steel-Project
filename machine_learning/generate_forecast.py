import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
from tensorflow.keras.models import load_model
from tensorflow.keras.losses import MeanSquaredError
import mysql.connector
import json
from datetime import datetime, timedelta

# Dynamische Pfade setzen
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "energy_prices_data.csv")
MODEL_PATH = os.path.join(BASE_DIR, "energy_price_model.h5")

def load_data():
    """Lädt die Strompreisdaten und bereitet sie vor."""
    df = pd.read_csv(DATA_PATH, sep=';', decimal=',')
    df['Datetime'] = pd.to_datetime(df['Datum'] + ' ' + df['von'], format='%d.%m.%Y %H:%M', errors='coerce')
    df.dropna(subset=['Datetime'], inplace=True)
    df = df[['Datetime', 'Spotmarktpreis in ct/kWh']]
    df.rename(columns={'Spotmarktpreis in ct/kWh': 'Spotpreis'}, inplace=True)
    df.dropna(subset=['Spotpreis'], inplace=True)
    df.set_index('Datetime', inplace=True)

    # Normalisiere die Spotpreis-Daten
    scaler = MinMaxScaler(feature_range=(0, 1))
    df['Spotpreis'] = scaler.fit_transform(df[['Spotpreis']])
    
    return df, scaler

def create_sequences(data, seq_length=336, output_length=24):
    """Erstellt Sequenzen für die Eingabe und das erwartete Ausgabeformat."""
    X, y = [], []
    for i in range(len(data) - seq_length - output_length):
        X.append(data[i:i + seq_length])
        y.append(data[i + seq_length:i + seq_length + output_length])
    return np.array(X), np.array(y)

def load_model_and_predict(data, scaler, seq_length=336, output_length=24):
    """Lädt das Modell, gibt die Vorhersage zurück und skaliert sie zurück."""
    # Modell laden
    model = load_model(MODEL_PATH, custom_objects={'mse': MeanSquaredError()}, compile=False)
    
    # Vorhersage für den nächsten Tag
    latest_data = np.array([data[-seq_length:]])
    print(latest_data)
    latest_data = latest_data.reshape((1, seq_length, 1))
    predictions = model.predict(latest_data)
    
    # Zurückskalieren der Vorhersage auf Originalwerte
    predictions = scaler.inverse_transform(predictions)

    return predictions


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
        print(f"ZeitID {ZeitID} wurde erfolgreich in die Zeittabelle eingefügt.")

def save_forecast_to_db(predictions, next_day):
    """Speichert die Vorhersage in der Datenbank, nachdem die ZeitID überprüft wurde."""
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

            # Speichern der ZeitID, falls nicht bereits vorhanden
            save_time_to_db(cursor, next_day)
            
            forecast_time = next_day.strftime('%Y-%m-%d') + ":00-00"
            forecast_json = {"forecast": predictions.tolist()}

            insert_query = """
                INSERT INTO Energiepreis_Vorhersage (ZeitID, Vorhersage)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE Vorhersage = VALUES(Vorhersage);
            """
            cursor.execute(insert_query, (forecast_time, json.dumps(forecast_json)))
            connection.commit()
            print(f"Vorhersage für {forecast_time} erfolgreich gespeichert.")
            
            cursor.close()
            connection.close()

    except mysql.connector.Error as err:
        print(f"Fehler beim Speichern der Vorhersage in die Datenbank: {err}")
        
def main():
   
    df, scaler = load_data()
    
    data = df['Spotpreis'].values
    
    predictions = load_model_and_predict(data, scaler)
    
    last_date = df.index[-1]
    next_day = last_date + timedelta(days=1)
    print(predictions)
    save_forecast_to_db(predictions, next_day)

if __name__ == "__main__":
    main()
