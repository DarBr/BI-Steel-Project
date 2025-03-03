import requests
import re
import json
import mysql.connector
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import locale

# Monatsnamen für manuellen Fallback
month_mapping = {
    "Januar": "January", "Februar": "February", "März": "March",
    "April": "April", "Mai": "May", "Juni": "June",
    "Juli": "July", "August": "August", "September": "September",
    "Oktober": "October", "November": "November", "Dezember": "December"
}

def convert_german_date(date_str):
    """Ersetzt deutsche Monatsnamen durch englische für strptime."""
    for de, en in month_mapping.items():
        if de in date_str:
            date_str = date_str.replace(de, en)
            break
    return date_str

def fetch_news_wvstahl_wirtschafthandelspolitik():
    news_list_wvstahl_wirtschafthandelspolitik = []
    # URL der Zielwebseite
    url = 'https://www.wvstahl.de/wirtschafts-und-handelspolitik/'

    # HTTP-Anfrage an die Webseite senden
    response = requests.get(url)
    response.raise_for_status()  # Überprüfen, ob die Anfrage erfolgreich war

    # Inhalt der Webseite parsen
    soup = BeautifulSoup(response.text, 'html.parser')

    # Nachrichtenbeiträge finden
    news_items = soup.select('ul.loop-block > li')

    if news_items:
        # Informationen aus jedem Nachrichtenbeitrag extrahieren
        for item in news_items:
            title_tag = item.find('h2').find('a')
            title = title_tag.get_text(strip=True)
            link = title_tag['href']
            summary = item.find('p').get_text(strip=True)
            
            # Datum aus der Zusammenfassung extrahieren 
            date = summary.split('|')[0].strip()
            
            news_list_wvstahl_wirtschafthandelspolitik.append({
                'title': title,
                'date': date,
                'summary': summary,
                'link': link,
                'category': 'Wirtschafts- und Handelspolitik'
            })
        return news_list_wvstahl_wirtschafthandelspolitik
    else:
        return None
    

def fetch_news_wvstahl_energieklimapolitik():
    news_list_wvstahl_energieklimapolitik = []
    # URL der Zielwebseite
    url = 'https://www.wvstahl.de/energie-und-klimapolitik/'

    # HTTP-Anfrage an die Webseite senden
    response = requests.get(url)
    response.raise_for_status()  # Überprüfen, ob die Anfrage erfolgreich war

    # Inhalt der Webseite parsen
    soup = BeautifulSoup(response.text, 'html.parser')

    # Nachrichtenbeiträge finden
    news_items = soup.select('ul.loop-block > li')

    if news_items:
        # Informationen aus jedem Nachrichtenbeitrag extrahieren
        for item in news_items:
            title_tag = item.find('h2').find('a')
            title = title_tag.get_text(strip=True)
            link = title_tag['href']
            summary = item.find('p').get_text(strip=True)
            
            # Datum aus der Zusammenfassung extrahieren 
            date = summary.split('|')[0].strip()
            
            news_list_wvstahl_energieklimapolitik.append({
                'title': title,
                'date': date,
                'summary': summary,
                'link': link,
                'category': 'Energie- und Klimapolitik'
            })
        return news_list_wvstahl_energieklimapolitik
    else:
        return None
    

def fetch_news_wvstahl_umweltnachhaltigkeitspolitik():
    news_list_wvstahl_umweltnachhaltigkeitspolitik = []
    # URL der Zielwebseite
    url = 'https://www.wvstahl.de/umwelt-und-nachhaltigkeitspolitik/'

    # HTTP-Anfrage an die Webseite senden
    response = requests.get(url)
    response.raise_for_status()  # Überprüfen, ob die Anfrage erfolgreich war

    # Inhalt der Webseite parsen
    soup = BeautifulSoup(response.text, 'html.parser')

    # Nachrichtenbeiträge finden
    news_items = soup.select('ul.loop-block > li')

    if news_items:
        # Informationen aus jedem Nachrichtenbeitrag extrahieren
        for item in news_items:
            title_tag = item.find('h2').find('a')
            title = title_tag.get_text(strip=True)
            link = title_tag['href']
            summary = item.find('p').get_text(strip=True)
            
            # Datum aus der Zusammenfassung extrahieren 
            date = summary.split('|')[0].strip()
            
            news_list_wvstahl_umweltnachhaltigkeitspolitik.append({
                'title': title,
                'date': date,
                'summary': summary,
                'link': link,
                'category': 'Umwelt- und Nachhaltigkeitspolitik'
            })
        return news_list_wvstahl_umweltnachhaltigkeitspolitik
    else:
        return None


def fetch_news_wvstahl_verkehrinfrastrukturpolitik():
    news_list_wvstahl_verkehrinfrastrukturpolitik = []
    # URL der Zielwebseite
    url = 'https://www.wvstahl.de/verkehrs-und-infrastrukturpolitik/'

    # HTTP-Anfrage an die Webseite senden
    response = requests.get(url)
    response.raise_for_status()  # Überprüfen, ob die Anfrage erfolgreich war

    # Inhalt der Webseite parsen
    soup = BeautifulSoup(response.text, 'html.parser')

    # Nachrichtenbeiträge finden
    news_items = soup.select('ul.loop-block > li')

    if news_items:
        # Informationen aus jedem Nachrichtenbeitrag extrahieren
        for item in news_items:
            title_tag = item.find('h2').find('a')
            title = title_tag.get_text(strip=True)
            link = title_tag['href']
            summary = item.find('p').get_text(strip=True)
            
            # Datum aus der Zusammenfassung extrahieren 
            date = summary.split('|')[0].strip()
            
            news_list_wvstahl_verkehrinfrastrukturpolitik.append({
                'title': title,
                'date': date,
                'summary': summary,
                'link': link,
                'category': 'Verkehrs- und Infrastrukturpolitik'
            })
        return news_list_wvstahl_verkehrinfrastrukturpolitik
    else:
        return None


def save_to_db(news):
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


            date_en = convert_german_date(news['date'])
            date_withPlace = re.search(r'(\d{1,2}\.\s+[A-Za-zäöüÄÖÜ]+\s+\d{4})', date_en)
            date_withoutPlace = re.search(r'(\d{1,2}\.\s+[A-Za-zäöüÄÖÜ]+\s+\d{4})', date_en)
            if date_withPlace:
                cleaned_date = date_withPlace.group(1)
                formatted_date = datetime.strptime(cleaned_date, '%d. %B %Y').strftime('%Y-%m-%d %H:%M:%S')
            elif date_withoutPlace:
                formatted_date = datetime.strptime(date_en, '%d. %B %Y').strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_date = None  # Falls das Datum nicht erkannt wird

            insert_query = """
                INSERT INTO News (date, titel, summary, link, category)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (formatted_date, news['title'], news['summary'], news['link'], news['category']))
            connection.commit()
    except mysql.connector.Error as err:
        print(f"Fehler: {err}")
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    # Liste zum Speichern der extrahierten Nachrichten
    news_list = []

    #Wirschafst- und Handelspolitik
    if fetch_news_wvstahl_wirtschafthandelspolitik() is None:
        print("Fehler beim Abrufen der Wirtschafts- und Handelspolitik von wvstahl.de")
    else:    
        news_list.extend(fetch_news_wvstahl_wirtschafthandelspolitik())

    #Energie- und Klimapolitik
    if fetch_news_wvstahl_energieklimapolitik() is None:
        print("Fehler beim Abrufen der Energie- und Klimapolitik von wvstahl.de")
    else:
        news_list.extend(fetch_news_wvstahl_energieklimapolitik())

    #Umwelt- und Nachhaltigkeitspolitik
    if fetch_news_wvstahl_umweltnachhaltigkeitspolitik() is None:
        print("Fehler beim Abrufen der Umwelt- und Nachhaltigkeitspolitik von wvstahl.de")
    else:
        news_list.extend(fetch_news_wvstahl_umweltnachhaltigkeitspolitik())
    
    #Verkehrs- und Infrastrukturpolitik
    if fetch_news_wvstahl_verkehrinfrastrukturpolitik() is None:
        print("Fehler beim Abrufen der Verkehrs- und Infrastrukturpolitik von wvstahl.de")
    else:
        news_list.extend(fetch_news_wvstahl_verkehrinfrastrukturpolitik())
    
    if news_list:
        for news in news_list:
            save_to_db(news)
        print("Alle Nachrichten wurden erfolgreich abgerufen und in die Datenbank gespeichert.")

