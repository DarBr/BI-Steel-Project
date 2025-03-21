import pandas as pd
import matplotlib.pyplot as plt

# Dateipfad definieren (der Pfad zur aktuellen Datei mit den korrekten Daten)
file_correct = "/Users/Andre/vscode-projects/BI-Steel-Project/machine_learning/energy_prices_data.csv"

# Daten einlesen
data = pd.read_csv(file_correct, sep=';', decimal=',')

# Sicherstellen, dass das Datum im richtigen Format ist
data['Datum'] = pd.to_datetime(data['Datum'], format='%d.%m.%Y')

# Den letzten Tag finden
last_day = data['Datum'].max()

# Daten für den letzten Tag filtern
last_day_data = data[data['Datum'] == last_day]

# Überprüfen, ob Daten für den letzten Tag vorhanden sind
if not last_day_data.empty:
    # Plot der Strompreise vom letzten Tag
    plt.figure(figsize=(10, 6))
    plt.plot(last_day_data['von'], last_day_data['Spotmarktpreis in ct/kWh'], marker='o', linestyle='-', color='b')
    plt.title(f'Strompreise vom {last_day.strftime("%d.%m.%Y")}')
    plt.xlabel('Zeit (von)')
    plt.ylabel('Preis in ct/kWh')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()
else:
    print("Es gibt keine Daten für den letzten Tag.")
