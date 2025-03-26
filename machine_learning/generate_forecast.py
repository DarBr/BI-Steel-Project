import pandas as pd
import numpy as np
import os
import json
import mysql.connector
from datetime import datetime, timedelta
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import load_model
from tensorflow.keras.losses import MeanSquaredError

# Dynamische Pfade
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "energy_prices_data.csv")
MODEL_PATH = os.path.join(BASE_DIR, "energy_price_model.h5")

def load_data():
    """Lädt die Strompreisdaten und bereitet sie vor."""
    print("Lade Daten...")
    df = pd.read_csv(DATA_PATH, sep=';', decimal=',')
    df.dropna(inplace=True)
    
    df['Datetime'] = pd.to_datetime(df['Datum'] + ' ' + df['von'], format='%d.%m.%Y %H:%M', errors='coerce')
    df.dropna(subset=['Datetime'], inplace=True)
    
    df = df[['Datetime', 'Spotmarktpreis in ct/kWh']]
    df.rename(columns={'Spotmarktpreis in ct/kWh': 'Spotpreis'}, inplace=True)
    df.set_index('Datetime', inplace=True)
    
    # Normalisierung
    scaler = MinMaxScaler(feature_range=(0, 1))
    df['Spotpreis'] = scaler.fit_transform(df[['Spotpreis']])
    print("Daten erfolgreich geladen und normalisiert.")
    
    return df, scaler

def load_model_and_predict(data, scaler, seq_length=336):
    """Lädt das Modell und gibt die Vorhersage zurück."""
    print("Lade Modell...")
    model = load_model(MODEL_PATH, custom_objects={'mse': MeanSquaredError()}, compile=False)
    print("Modell erfolgreich geladen.")
    
    latest_data = np.array([data[-seq_length:]]).reshape((1, seq_length, 1))
    print("Berechne Vorhersage...")
    predictions = model.predict(latest_data)
    
    predictions = scaler.inverse_transform(predictions)
    print("Vorhersage erfolgreich berechnet.")
    
    return predictions

def save_time_to_db(cursor, timestamp):
    """Fügt 24 Zeit-IDs für den Vorhersagetag in die Datenbank ein, falls sie nicht existieren."""
    print(f"Speichere Zeit-IDs für {timestamp.strftime('%Y-%m-%d')}...")
    for hour in range(24):
        current_time = timestamp.replace(hour=hour, minute=0, second=0)
        ZeitID = current_time.strftime('%Y-%m-%d:%H') + "-00"
        datum = current_time.date()
        uhrzeit = current_time.time()
        jahr = current_time.year
        monat = current_time.month
        quartal = (monat - 1) // 3 + 1
        wochentag = current_time.strftime('%A')
        
        cursor.execute("SELECT COUNT(*) FROM Zeit WHERE ZeitID = %s", (ZeitID,))
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                INSERT INTO Zeit (ZeitID, Datum, Uhrzeit, Jahr, Monat, Q, Wochentag)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (ZeitID, datum, uhrzeit, jahr, monat, quartal, wochentag))
            print(f"ZeitID {ZeitID} gespeichert.")

def save_forecast_to_db(predictions, next_day):
    """Speichert die Vorhersage für den nächsten Tag in der Datenbank."""
    try:
        print("Verbinde mit der Datenbank...")
        connection = mysql.connector.connect(
            host="13.60.244.59",
            port=3306,
            user="user",
            password="clientserver",
            database="database-dwh"
        )
        print("Verbindung erfolgreich hergestellt.")
        
        cursor = connection.cursor()
        save_time_to_db(cursor, next_day)
        
        forecast_time = next_day.strftime('%Y-%m-%d') + ":00-00"
        forecast_json = json.dumps({"forecast": predictions.tolist()})
        
        cursor.execute("""
            INSERT INTO Fakt_Energiepreisvorhersage (ZeitID, Vorhersage)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE Vorhersage = VALUES(Vorhersage);
        """, (forecast_time, forecast_json))
        
        connection.commit()
        print(f"Vorhersage für {forecast_time} erfolgreich gespeichert.")
        
        cursor.close()
        connection.close()
        print("Datenbankverbindung geschlossen.")
    except mysql.connector.Error as err:
        print(f"Fehler beim Speichern der Vorhersage: {err}")

def main():
    print("Starte Prozess...")
    df, scaler = load_data()
    
    data = df['Spotpreis'].values
    predictions = load_model_and_predict(data, scaler)
    
    last_date = df.index[-1]
    next_day = last_date + timedelta(days=1)
    
    print(f"Letztes Datum: {last_date}, Vorhersage für: {next_day}")
    print("Speichere Vorhersage in die Datenbank...")
    save_forecast_to_db(predictions, next_day)
    print("Prozess abgeschlossen.")

if __name__ == "__main__":
    main()
