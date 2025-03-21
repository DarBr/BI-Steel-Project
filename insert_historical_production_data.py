import mysql.connector
import random
from datetime import datetime, timedelta

def generate_production_data(start_date, num_days):
    """Erstellt Produktionsaufträge für die angegebene Anzahl von Tagen."""
    # Maschinen und ihre Produktionskapazität
    maschinen = {
        1: 1.2,   # Lichtbogenofen 1: 1.2 T/h
        2: 1.2,   # Lichtbogenofen 2: 1.2 T/h
        3: 2.0    # Lichtbogenofen 3: 2.0 T/h
    }
    
    # Produkte (1-8)
    produkte = list(range(1, 9))

    auftraege = []

    for day in range(num_days):
        date = start_date + timedelta(days=day)
        for maschine, kapazitaet in maschinen.items():
            # Definiere Produktwechselzeiten (drei Schichten: 00:00, 08:00, 16:00)
            schichtwechsel = [0, 8, 16]
            produkt_id = random.choice(produkte)  # Anfangsprodukt setzen

            for hour in range(24):
                startzeit = datetime(date.year, date.month, date.day, hour, 0, 0)

                # Produktwechsel alle 8 Stunden
                if hour in schichtwechsel:
                    produkt_id = random.choice(produkte)

                # Auslastung je nach Uhrzeit variieren
                if 10 <= hour < 16:
                    auslastung = random.randint(85, 100)  # Höhere Auslastung am Tag
                else:
                    auslastung = random.randint(70, 85)  # Niedrigere Auslastung sonst

                # Berechnung der Produktionsmenge
                produktionsmenge = round(kapazitaet * (auslastung / 100), 3)

                # Ausschussmenge als 1-5% der Produktionsmenge
                ausschussmenge = round(produktionsmenge * random.uniform(0.01, 0.05), 3)

                auftraege.append((maschine, startzeit, produktionsmenge, ausschussmenge, produkt_id, auslastung))
    
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
            INSERT INTO tb_Produktionsauftrag (MaschinenID, Startzeit, Produktionsmenge, Ausschussmenge, ProduktID, Auslastung)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE Produktionsmenge = VALUES(Produktionsmenge), Ausschussmenge = VALUES(Ausschussmenge), Auslastung = VALUES(Auslastung);
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
