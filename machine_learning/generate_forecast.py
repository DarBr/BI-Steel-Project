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

# Dateipfad für die CSV-Daten
file_path = "energy_prices_data.csv"
model_path = "energy_price_model.h5"  

# Lade die Strompreisdaten
df = pd.read_csv(file_path, sep=';', decimal=',')
df['Datetime'] = pd.to_datetime(df['Datum'] + ' ' + df['von'], format='%d.%m.%Y %H:%M', errors='coerce')

# Entferne Zeilen mit NaN-Werten in der 'Datetime'-Spalte
df.dropna(subset=['Datetime'], inplace=True)

# Wähle die relevanten Spalten und benenne sie um
df = df[['Datetime', 'Spotmarktpreis in ct/kWh']]
df.rename(columns={'Spotmarktpreis in ct/kWh': 'Spotpreis'}, inplace=True)

# Entfernen von Zeilen mit NaN-Werten in der Spotpreis-Spalte
df.dropna(subset=['Spotpreis'], inplace=True)

# Setze 'Datetime' als Index
df.set_index('Datetime', inplace=True)

# Normalisiere die Spotpreis-Daten
scaler = MinMaxScaler(feature_range=(0, 1))
df['Spotpreis'] = scaler.fit_transform(df[['Spotpreis']])

# Sequenzen erstellen
def create_sequences(data, seq_length=336, output_length=24):
    X, y = [], []
    for i in range(len(data) - seq_length - output_length):
        X.append(data[i:i + seq_length])  # 14 Tage Input
        y.append(data[i + seq_length:i + seq_length + output_length])  # 24 Stunden Output
    return np.array(X), np.array(y)

# Sequenzen für das Modell erstellen
seq_length = 336  # 14 Tage
output_length = 24  # 24 Stunden Vorhersage
data = df['Spotpreis'].values
X, y = create_sequences(data, seq_length, output_length)

X = X.reshape((X.shape[0], X.shape[1], 1))

# Modell laden
model = load_model(model_path, custom_objects={'mse': MeanSquaredError()})

# Vorhersage für den nächsten Tag berechnen
def predict_for_next_day(model, data, seq_length=336, output_length=24):
    latest_data = np.array([data[-seq_length:]])  # Nimm die letzten 14 Tage (336 Stunden)
    latest_data = latest_data.reshape((1, seq_length, 1))  # Reshape für das Modell
    predictions = model.predict(latest_data)  # Vorhersage für die nächsten 24 Stunden
    
    # Transformiere die Vorhersage zurück auf den originalen Preisbereich
    predictions = scaler.inverse_transform(predictions)
    return predictions[0]

# Berechne die Vorhersage
predictions = predict_for_next_day(model, data)

# Bestimme das Datum für die Vorhersage
last_date = df.index[-1]  # Das Datum des letzten Eintrags
next_day = last_date + timedelta(days=1)  # Der nächste Tag nach dem letzten Eintrag

# Speichern der Vorhersage in der Datenbank
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
            
            # ZeitID für die Vorhersage (nächster Tag)
            forecast_time = next_day.strftime('%Y-%m-%d') + ":00-00"  # Format: YYYY-MM-DD:00-00
            
            # Umwandeln der Vorhersage in JSON
            forecast_json = {"forecast": predictions.tolist()}  # JSON format

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

# Speichern der Vorhersage in die Datenbank
save_forecast_to_db(predictions, next_day)
