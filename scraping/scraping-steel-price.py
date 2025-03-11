import requests
import re
import json
import mysql.connector
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
import time

def fetch_hrc_price():
    url = 'https://tradingeconomics.com/commodity/hrc-steel'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, wie Gecko) Chrome/110.0.0.0 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        script_tags = soup.find_all("script")
        
        hrc_price = None
        for tag in script_tags:
            if "TEChartsMeta" in tag.text:
                json_match = re.search(r"TEChartsMeta\s*=\s*(\[.*?\]);", tag.text, re.DOTALL)
                if json_match:
                    json_data = json.loads(json_match.group(1))
                    hrc_price = json_data[0].get("value")
                    break
        
        if hrc_price:
            print(f"Aktueller HRC-Stahlpreis: {hrc_price} USD/T")
            return hrc_price
        else:
            print("Fehler: Konnte den Stahlpreis nicht finden.")
            return None
    else:
        print(f"Fehler bei der Anfrage: {response.status_code}")
        return None


def save_to_db(price):
    try:
        connection = mysql.connector.connect(
            host="3.142.199.164",
            port=3306,
            user="user",
            password="clientserver",
            database="database-dwh"
        )
        
        if connection.is_connected():
            cursor = connection.cursor()

            # Runde den aktuellen Zeitpunkt auf die volle Stunde
            current_datetime = datetime.now(timezone.utc)
            rounded_datetime = current_datetime.replace(minute=0, second=0, microsecond=0)

            # Extrahiere die vollen Stunden (ZeitID)
            zeit_id = rounded_datetime.strftime('%Y-%m-%d:%H') + "-00"  # ZeitID als vollen Zeitstempel (z. B. '2025-03-08:12-00')
            current_date = rounded_datetime.date()
            jahr = current_date.year
            monat = current_date.month
            quartal = (monat - 1) // 3 + 1  # Berechnung des Quartals
            wochentag = current_date.strftime("%A")  # Wochentag (z. B. 'Montag')
            
            # Check if ZeitID exists in Zeit table
            check_zeit_query = "SELECT COUNT(*) FROM Zeit WHERE ZeitID = %s"
            cursor.execute(check_zeit_query, (zeit_id,))
            result = cursor.fetchone()
            
            if result[0] == 0:
                # Insert ZeitID into Zeit table if not found
                insert_zeit_query = """
                INSERT INTO Zeit (ZeitID, Datum, Uhrzeit, Jahr, Monat, Q, Wochentag)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_zeit_query, (zeit_id, current_date, rounded_datetime.time(), jahr, monat, quartal, wochentag))
                connection.commit()
                print("Neue ZeitID in Zeit eingefügt.")
            else:
                print("ZeitID existiert bereits in Zeit.")
            
            # Jetzt die Daten in Dim_Marktpreise einfügen mit der ZeitID
            materialname = "HRC Stahl"
            einheit = "USD/T"
            insert_query = """
                INSERT INTO Dim_Marktpreise (ZeitID, Materialname, Preis, Einheit)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE Preis = VALUES(Preis);
            """
            cursor.execute(insert_query, (zeit_id, materialname, price, einheit))
            connection.commit()
            print("Daten erfolgreich gespeichert oder aktualisiert.")
            
            cursor.close()
            connection.close()
    except mysql.connector.Error as err:
        print(f"Fehler: {err}")


def wait_until_next_hour():
    # Berechne die Zeit bis zur nächsten vollen Stunde
    now = datetime.now(timezone.utc)
    next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    wait_time = (next_hour - now).total_seconds()
    
    print(f"Warte bis zur nächsten vollen Stunde. (Wartezeit: {wait_time} Sekunden)")
    time.sleep(wait_time)


if __name__ == "__main__":
    while True:
        wait_until_next_hour()  # Warte bis zur nächsten vollen Stunde
        price = fetch_hrc_price()
        if price is not None:
            save_to_db(price)
