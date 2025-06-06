from dotenv import load_dotenv
import os
import requests
import re
import json
import mysql.connector
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
import pytz
load_dotenv()

# Funktion zum Ausgeben von Logs
def print_to_log(message):
    print(message)

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
            print_to_log(f"Aktueller HRC-Stahlpreis: {hrc_price} USD/T")
            return hrc_price
        else:
            print_to_log("Fehler: Konnte den Stahlpreis nicht finden.")
            return None
    else:
        print_to_log(f"Fehler bei der Anfrage: {response.status_code}")
        return None


def save_to_db(price):
    try:
        connection = mysql.connector.connect(
        host=os.getenv("HOST"),
        port=int(os.getenv("PORT")),
        user=os.getenv("DB_USER"),
        password=os.getenv("PASSWORD"),
        database=os.getenv("DATABASE_DEST")
    )

        if connection.is_connected():
            cursor = connection.cursor()

            # Holen der deutschen Zeitzone (Europe/Berlin)
            berlin_tz = pytz.timezone('Europe/Berlin')
            current_datetime = datetime.now(pytz.utc).astimezone(berlin_tz)  # UTC nach Berlin Zeit konvertieren
            rounded_datetime = current_datetime.replace(minute=0, second=0, microsecond=0)

            # Extrahiere die vollen Stunden (ZeitID)
            zeit_id = rounded_datetime.strftime('%Y-%m-%d:%H') + "-00"  
            current_date = rounded_datetime.date()
            jahr = current_date.year
            monat = current_date.month
            quartal = (monat - 1) // 3 + 1  
            wochentag = current_date.strftime("%A") 
            
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
                print_to_log("Neue ZeitID in Zeit eingefügt.")
            else:
                print_to_log("ZeitID existiert bereits in Zeit.")
            
            # Daten in Dim_Marktpreise einfügen mit der ZeitID
            materialname = "HRC Stahl"
            einheit = "USD/T"
            insert_query = """
                INSERT INTO Dim_Marktpreise (ZeitID, Materialname, Preis, Einheit)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE Preis = VALUES(Preis);
            """
            cursor.execute(insert_query, (zeit_id, materialname, price, einheit))
            connection.commit()
            print_to_log("Daten erfolgreich gespeichert oder aktualisiert.")
            
            cursor.close()
            connection.close()
    except mysql.connector.Error as err:
        print_to_log(f"Fehler: {err}")


def main():
    price = fetch_hrc_price()
    if price is not None:
        save_to_db(price)


if __name__ == "__main__":
    main()
