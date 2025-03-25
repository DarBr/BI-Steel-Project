import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error
from tensorflow.keras.models import load_model
from tensorflow.keras.losses import MeanSquaredError
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import os

# Datei- und Modellpfade
data_path = "machine_learning/energy_prices_data.csv"
model_path = "machine_learning/energy_price_model.h5"

# Lade die Daten
df = pd.read_csv(data_path, sep=';', decimal=',', header=0)
df['Datetime'] = pd.to_datetime(df['Datum'] + ' ' + df['von'], format='%d.%m.%Y %H:%M', errors='coerce')
df.dropna(subset=['Datetime'], inplace=True)
df = df[['Datetime', 'Spotmarktpreis in ct/kWh']]
df.rename(columns={'Spotmarktpreis in ct/kWh': 'Spotpreis'}, inplace=True)
df.dropna(subset=['Spotpreis'], inplace=True)
df.set_index('Datetime', inplace=True)

# Normalisierung
scaler = MinMaxScaler(feature_range=(0, 1))
df['Spotpreis'] = scaler.fit_transform(df[['Spotpreis']])

def create_sequences(data, seq_length=336, output_length=24):
    X, y = [], []
    for i in range(len(data) - seq_length - output_length):
        X.append(data[i:i + seq_length])
        y.append(data[i + seq_length:i + seq_length + output_length])
    return np.array(X), np.array(y)

seq_length = 336  # 14 Tage
output_length = 24  # 24 Stunden Vorhersage

data = df['Spotpreis'].values
X, y = create_sequences(data, seq_length, output_length)
X = X.reshape((X.shape[0], X.shape[1], 1))

# Train-Test-Split
train_size = int(len(X) * 0.8)
X_train, X_test = X[:train_size], X[train_size:]
y_train, y_test = y[:train_size], y[train_size:]

# Lade das Modell
if os.path.exists(model_path):
    model = load_model(model_path, custom_objects={'mse': MeanSquaredError()})
else:
    raise FileNotFoundError("Modell nicht gefunden. Bitte trainiere es zuerst.")

# Vorhersagen generieren
y_pred = model.predict(X_test)

# Rücktransformation der Vorhersagen
y_pred_rescaled = scaler.inverse_transform(y_pred.reshape(-1, 1))
y_test_rescaled = scaler.inverse_transform(y_test.reshape(-1, 1))

# Berechnung der Metriken
mse = mean_squared_error(y_test_rescaled, y_pred_rescaled)
rmse = np.sqrt(mse)
print(f"Mean Squared Error (MSE): {mse}")
print(f"Root Mean Squared Error (RMSE): {rmse}")