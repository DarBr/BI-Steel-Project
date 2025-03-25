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
data_path = os.path.join(BASE_DIR, "energy_prices_data.csv")
model_path = os.path.join(BASE_DIR, "energy_price_model.h5")

# Lade die Strompreisdaten
df = pd.read_csv(data_path, sep=';', decimal=',')
df['Datetime'] = pd.to_datetime(df['Datum'] + ' ' + df['von'], format='%d.%m.%Y %H:%M', errors='coerce')

df.dropna(subset=['Datetime'], inplace=True)
df = df[['Datetime', 'Spotmarktpreis in ct/kWh']]
df.rename(columns={'Spotmarktpreis in ct/kWh': 'Spotpreis'}, inplace=True)
df.dropna(subset=['Spotpreis'], inplace=True)
df.set_index('Datetime', inplace=True)

# Normalisiere die Spotpreis-Daten
scaler = MinMaxScaler(feature_range=(0, 1))
df['Spotpreis'] = scaler.fit_transform(df[['Spotpreis']])

def create_sequences(data, seq_length=336, output_length=24):
    X, y = [], []
    for i in range(len(data) - seq_length - output_length):
        X.append(data[i:i + seq_length])
        y.append(data[i + seq_length:i + seq_length + output_length])
    return np.array(X), np.array(y)

seq_length = 336
output_length = 24
data = df['Spotpreis'].values
X, y = create_sequences(data, seq_length, output_length)
X = X.reshape((X.shape[0], X.shape[1], 1))

# Modell laden
model = load_model(model_path, custom_objects={'mse': MeanSquaredError()}, compile=False)

def predict_for_next_day(model, data, seq_length=336, output_length=24):
    latest_data = np.array([data[-seq_length:]])
    latest_data = latest_data.reshape((1, seq_length, 1))
    predictions = model.predict(latest_data)
    predictions = scaler.inverse_transform(predictions)
    return predictions[0]

predictions = predict_for_next_day(model, data)

last_date = df.index[-1]
next_day = last_date + timedelta(days=1)

def save_forecast_to_db(predictions, next_day):
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

save_forecast_to_db(predictions, next_day)
