import mysql.connector
import random
from datetime import datetime, timedelta

def generate_production_data(start_date, num_days):
    """Erstellt Produktionsaufträge für die angegebene Anzahl von Tagen."""
    # Maschinen und ihre Produktionskapazität und Verbrauch (nur Maschine 1 und 2)
    maschinen = {
        1: {'kapazitaet': 12.0, 'verbrauch': 25000.00},   # Lichtbogenofen 1: 12 T/h, 25.000 kWh/h
        2: {'kapazitaet': 15.0, 'verbrauch': 30000.00}    # Lichtbogenofen 2: 15 T/h, 30.000 kWh/h
    }
    
    # Produkte (1-8)
    produkte = list(range(1, 9))

    auftraege = []
    maschinen_betriebsstunden = {maschine: 0 for maschine in maschinen}  # Betriebsstunden pro Maschine
    
    for day in range(num_days):
        date = start_date + timedelta(days=day)
        
        for maschine, details in maschinen.items():
            kapazitaet = details['kapazitaet']
            verbrauch_pro_tonne = details['verbrauch']  # Verbrauch pro Stunde für 1 Tonne

            # Definiere Produktwechselzeiten (drei Schichten: 00:00, 08:00, 16:00)
            schichtwechsel = [0, 8, 16]
            produkt_id = random.choice(produkte)  # Anfangsprodukt setzen

            for hour in range(24):
                startzeit = datetime(date.year, date.month, date.day, hour, 0, 0)

                # Produktwechsel alle 8 Stunden
                if hour in schichtwechsel:
                    produkt_id = random.choice(produkte)

                # Wartung einplanen: wenn die Betriebsstunden einer Maschine zwischen 2000 und 4000 liegen
                maschinen_betriebsstunden[maschine] += 1
                if 2000 <= maschinen_betriebsstunden[maschine] <= 4000:
                    # Wartung für 8 Stunden
                    if hour < 8:
                        auslastung = 0
                        produktionsmenge = 0
                        ausschussmenge = 0
                        verbrauch = 0
                    else:
                        auslastung = 0
                        produktionsmenge = 0
                        ausschussmenge = 0
                        verbrauch = 0
                else:
                    # Auslastung je nach Uhrzeit variieren
                    if 10 <= hour < 16:
                        auslastung = random.randint(85, 100)  # Höhere Auslastung am Tag
                    else:
                        auslastung = random.randint(70, 85)  # Niedrigere Auslastung sonst

                    # Berechnung der Produktionsmenge
                    produktionsmenge = round(kapazitaet * (auslastung / 100), 3)

                    # Ausschussmenge als 1-5% der Produktionsmenge
                    ausschussmenge = round(produktionsmenge * random.uniform(0.01, 0.05), 3)  # 1-5% für alle Maschinen

                    # Berechnung des Verbrauchs (kWh) basierend auf der Produktionsmenge
                    verbrauch = round((produktionsmenge / kapazitaet) * verbrauch_pro_tonne, 2)

                # Erstelle den Produktionsauftrag
                auftraege.append((
                    maschine,           # MaschinenID
                    startzeit,          # Startzeit des Produktionsauftrags
                    produktionsmenge,   # Produktionsmenge
                    ausschussmenge,     # Ausschussmenge
                    produkt_id,         # ProduktID
                    auslastung,         # Auslastung in Prozent
                    verbrauch           # Verbrauch in kWh
                ))
    
    return auftraege

def insert_into_db(data):
    """Fügt Produktionsdaten in die Datenbank ein."""
    try:
        connection = mysql.connector.connect(
            host="13.60.244.59",
            port=3306,
            user="user",
            password="clientserver",
            database="database-steel"
        )
        cursor = connection.cursor()
        
        insert_query = """
            INSERT INTO tb_Produktionsauftrag (MaschinenID, Startzeit, Produktionsmenge, Ausschussmenge, ProduktID, Auslastung, Verbrauch)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE Produktionsmenge = VALUES(Produktionsmenge), Ausschussmenge = VALUES(Ausschussmenge), Auslastung = VALUES(Auslastung), Verbrauch = VALUES(Verbrauch);
        """
        
        cursor.executemany(insert_query, data)
        connection.commit()
        print(f"{cursor.rowcount} Produktionsaufträge erfolgreich eingefügt.")
        
        cursor.close()
        connection.close()
    except mysql.connector.Error as err:
        print(f"Fehler: {err}")

if __name__ == "__main__":
    start_date = datetime(2025, 1, 1)  # Startdatum: 1. Januar 2025
    today = datetime.now()  # Aktuelles Datum
    num_days = (today - start_date).days  # Anzahl der Tage bis heute

    print(f"Generiere Produktionsaufträge für {num_days} Tage...")
    
    for day in range(num_days):
        data = generate_production_data(start_date + timedelta(days=day), 1)
        insert_into_db(data)  # Daten für den Tag speichern
        print(f"Tag {day + 1}/{num_days} abgeschlossen.")

    print("Historische Produktionsaufträge erfolgreich nachgeholt!")
