import mysql.connector
import random
from datetime import datetime, timedelta

def generate_production_data(date):
    """Erstellt Produktionsaufträge für einen einzelnen Tag."""
    maschinen = {
        1: {'kapazitaet': 12.0, 'verbrauch': 25000.00},   # Lichtbogenofen 1
        2: {'kapazitaet': 15.0, 'verbrauch': 30000.00}    # Lichtbogenofen 2
    }
    
    produkte = list(range(1, 9))
    auftraege = []
    schichtwechsel = [0, 8, 16]
    
    for maschine, details in maschinen.items():
        kapazitaet = details['kapazitaet']
        verbrauch_pro_tonne = details['verbrauch']
        produkt_id = random.choice(produkte)

        for hour in range(24):
            startzeit = datetime(date.year, date.month, date.day, hour, 0, 0)
            if hour in schichtwechsel:
                produkt_id = random.choice(produkte)

            if 10 <= hour < 16:
                auslastung = random.randint(85, 100)
            else:
                auslastung = random.randint(70, 85)
            
            produktionsmenge = round(kapazitaet * (auslastung / 100), 3)
            ausschussmenge = round(produktionsmenge * random.uniform(0.01, 0.05), 3)
            verbrauch = round((produktionsmenge / kapazitaet) * verbrauch_pro_tonne, 2)

            auftraege.append((maschine, startzeit, produktionsmenge, ausschussmenge, produkt_id, auslastung, verbrauch))
    
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
    today = datetime.now()
    print(f"Generiere Produktionsaufträge für {today.strftime('%Y-%m-%d')}...")
    data = generate_production_data(today)
    insert_into_db(data)
    print("Daten erfolgreich eingefügt.")
