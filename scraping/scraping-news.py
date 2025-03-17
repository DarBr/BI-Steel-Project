import requests
import re
import json
import mysql.connector
import time
from bs4 import BeautifulSoup
from datetime import datetime, timezone

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

def fetch_news(url, category):
    news_list = []
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        news_items = soup.select('ul.loop-block > li')

        if not news_items:
            print(f"Keine Nachrichten gefunden auf {url}")
            return None

        for item in news_items:
            title_tag = item.find('h2').find('a')
            title = title_tag.get_text(strip=True)
            link = title_tag['href']
            summary = item.find('p').get_text(strip=True)
            date = summary.split('|')[0].strip()

            news_list.append({
                'title': title,
                'date': date,
                'summary': summary,
                'link': link,
                'category': category
            })
        return news_list
    except Exception as e:
        print(f"Fehler beim Abrufen von {url}: {e}")
        return None

def save_to_db(news):
    try:
        connection = mysql.connector.connect(
            host="13.60.244.59",
            port=3306,
            user="user",
            password="clientserver",
            database="database-dwh"
        )
        cursor = connection.cursor()

        check_query = "SELECT COUNT(*) FROM News WHERE link = %s"
        cursor.execute(check_query, (news['link'],))
        result = cursor.fetchone()

        if result[0] > 0:
            print(f"News '{news['title']}' existiert bereits.")
            return

        date_en = convert_german_date(news['date'])
        date_match = re.search(r'(\d{1,2}\.\s+[A-Za-zäöüÄÖÜ]+\s+\d{4})', date_en)
        formatted_date = datetime.strptime(date_match.group(1), '%d. %B %Y').strftime('%Y-%m-%d %H:%M:%S') if date_match else None

        insert_query = """
            INSERT INTO News (date, titel, summary, link, category)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (formatted_date, news['title'], news['summary'], news['link'], news['category']))
        connection.commit()
        print(f"News '{news['title']}' gespeichert.")
    except mysql.connector.Error as err:
        print(f"Datenbankfehler: {err}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

def main():
    urls = [
        ("https://www.wvstahl.de/wirtschafts-und-handelspolitik/", "Wirtschafts- und Handelspolitik"),
        ("https://www.wvstahl.de/energie-und-klimapolitik/", "Energie- und Klimapolitik"),
        ("https://www.wvstahl.de/umwelt-und-nachhaltigkeitspolitik/", "Umwelt- und Nachhaltigkeitspolitik"),
        ("https://www.wvstahl.de/verkehrs-und-infrastrukturpolitik/", "Verkehrs- und Infrastrukturpolitik")
    ]
    news_list = []

    for url, category in urls:
        news = fetch_news(url, category)
        if news:
            news_list.extend(news)

    if news_list:
        for news in news_list:
            save_to_db(news)
        print("Alle Nachrichten wurden gespeichert.")
    else:
        print("Keine neuen Nachrichten zum Speichern.")

if __name__ == "__main__":
    while True:
        print("Starte den täglichen News-Scraper...")
        main()
        print("Warte 24 Stunden bis zum nächsten Durchlauf...")
        time.sleep(86400)
