import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

# Dynamischer Dateipfad, falls das Skript im gleichen Verzeichnis wie die CSV-Datei liegt
file_path = os.path.join(os.path.dirname(__file__), "energy_prices_data.csv")  # Stelle sicher, dass der Pfad korrekt ist

# Lade die Daten
df = pd.read_csv(file_path, sep=';', decimal=',', header=0)

# Kombiniere Datum und von für die 'Datetime'-Spalte
df['Datetime'] = pd.to_datetime(df['Datum'] + ' ' + df['von'], format='%d.%m.%Y %H:%M', errors='coerce')

# Entferne Zeilen mit NaT (Fehler bei der Datumsumwandlung)
df.dropna(subset=['Datetime'], inplace=True)

# Wähle die relevanten Spalten und benenne sie um
df = df[['Datetime', 'Spotmarktpreis in ct/kWh']]
df.rename(columns={'Spotmarktpreis in ct/kWh': 'Spotpreis'}, inplace=True)

# Entfernen von Zeilen mit NaN-Werten in der Spotpreis-Spalte
df.dropna(subset=['Spotpreis'], inplace=True)

# Setze 'Datetime' als Index
df.set_index('Datetime', inplace=True)

# Grundlegende statistische Analyse
print("Grundlegende Statistiken:")
print(df['Spotpreis'].describe())

# Weitere statistische Kennzahlen
mean_price = df['Spotpreis'].mean()  # Mittelwert
median_price = df['Spotpreis'].median()  # Median
std_dev = df['Spotpreis'].std()  # Standardabweichung

print(f"\nMittelwert der Spotpreise: {mean_price:.2f} ct/kWh")
print(f"Median der Spotpreise: {median_price:.2f} ct/kWh")
print(f"Standardabweichung der Spotpreise: {std_dev:.2f} ct/kWh")

# 1. Visualisierung der Spotpreise über die Zeit
plt.figure(figsize=(12, 6))
plt.plot(df.index, df['Spotpreis'], label="Spotpreis", color='blue')
plt.xlabel('Zeit')
plt.ylabel('Spotpreis (ct/kWh)')
plt.title('Spotpreis über die Zeit')
plt.legend()
plt.grid(True)
plt.show()

# 2. Boxplot der Spotpreise
plt.figure(figsize=(10, 6))
plt.boxplot(df['Spotpreis'], vert=False, patch_artist=True, notch=True, boxprops=dict(facecolor='skyblue'))
plt.xlabel('Spotpreis (ct/kWh)')
plt.title('Boxplot der Spotpreise')
plt.grid(True)
plt.show()

# 3. Visualisierung der Korrelation zwischen Spotpreis und Wochentag
df['Wochentag'] = df.index.weekday  # 0 = Montag, 6 = Sonntag

# Ersetze die Wochentagsnummern durch die Namen
wochentag_mapping = {
    0: 'Montag', 1: 'Dienstag', 2: 'Mittwoch', 3: 'Donnerstag', 
    4: 'Freitag', 5: 'Samstag', 6: 'Sonntag'
}
df['Wochentag'] = df['Wochentag'].map(wochentag_mapping)

plt.figure(figsize=(8, 6))
plt.scatter(df['Wochentag'], df['Spotpreis'], alpha=0.5, color='orange')
plt.xlabel('Wochentag')
plt.ylabel('Spotpreis (ct/kWh)')
plt.title('Korrelation zwischen Spotpreis und Wochentag')
plt.grid(True)
plt.xticks(rotation=45)  # Dreht die Wochentagsbezeichnungen
plt.show()

# 4. Saisonale Trends: Durchschnitt pro Monat
df['Monat'] = df.index.month
monthly_avg = df.groupby('Monat')['Spotpreis'].mean()

# Visualisierung des monatlichen Durchschnitts mit Monatsnamen
monthly_avg.index = monthly_avg.index.map({
    1: 'Januar', 2: 'Februar', 3: 'März', 4: 'April', 5: 'Mai', 6: 'Juni',
    7: 'Juli', 8: 'August', 9: 'September', 10: 'Oktober', 11: 'November', 12: 'Dezember'
})

plt.figure(figsize=(10, 6))
monthly_avg.plot(kind='bar', color='purple', edgecolor='black')
plt.xlabel('Monat')
plt.ylabel('Durchschnittlicher Spotpreis (ct/kWh)')
plt.title('Durchschnittlicher Spotpreis pro Monat')
plt.grid(True)
plt.show()

# 5. Durchschnittlicher Spotpreis pro Stunde des Tages
df['Stunde'] = df.index.hour  # Extrahiere die Stunde des Tages

# Berechne den durchschnittlichen Spotpreis pro Stunde
hourly_avg = df.groupby('Stunde')['Spotpreis'].mean()

# Visualisierung des durchschnittlichen Spotpreises pro Stunde des Tages
plt.figure(figsize=(10, 6))
hourly_avg.plot(kind='line', marker='o', color='red', linestyle='-', linewidth=2, markersize=6)
plt.xlabel('Stunde des Tages')
plt.ylabel('Durchschnittlicher Spotpreis (ct/kWh)')
plt.title('Durchschnittlicher Spotpreis pro Stunde des Tages')
plt.grid(True)
plt.xticks(range(0, 24)) 
plt.show()

# 6. Autokorrelation der Spotpreise für die ersten 14 Tage (336 Lag)
from pandas.plotting import autocorrelation_plot

# Beschränke die Daten auf die ersten 14 Tage (336 Stunden)
df_14_days = df[:14 * 24]  # 14 Tage * 24 Stunden = 336 Datenpunkte

plt.figure(figsize=(10, 6))
autocorrelation_plot(df_14_days['Spotpreis'])
plt.title('Autokorrelation der Spotpreise für die ersten 14 Tage')
plt.show()
