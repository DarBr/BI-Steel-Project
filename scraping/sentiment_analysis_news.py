import feedparser
import requests
import mysql.connector
from textblob import TextBlob
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
from dotenv import load_dotenv
import os

load_dotenv()

# Live-RSS-Feed der Tagesschau
RSS_URL = 'https://www.tagesschau.de/xml/rss2'

# Wirtschaftlich relevante Schlüsselwörter & zugehörige Kategorien
ECONOMIC_KEYWORDS = [
    "Wirtschaft", "Industrie", "Stahl", "Energiepreise", "Rohstoffe", "Infrastruktur", 
    "Export", "Import", "Zölle", "Inflation", "Arbeitsmarkt", "Konjunktur", "BIP",
    "Stahlindustrie", "Auftragslage", "Aufschwung", "Rezession", "Wirtschaftswachstum"
]

CATEGORY_MAPPING = {
    "Wirtschaft": "Wirtschafts- und Handelspolitik",
    "Industrie": "Wirtschafts- und Handelspolitik",
    "Stahl": "Wirtschafts- und Handelspolitik",
    "Energiepreise": "Energie- und Klimapolitik",
    "Rohstoffe": "Energie- und Klimapolitik",
    "Infrastruktur": "Verkehrs- und Infrastrukturpolitik",
    "Export": "Außenhandelspolitik",
    "Import": "Außenhandelspolitik",
    "Zölle": "Außenhandelspolitik"
}

# Monatsnamen für manuellen Fallback
month_mapping = {
    "Januar": "January", "Februar": "February", "März": "March",
    "April": "April", "Mai": "May", "Juni": "June",
    "Juli": "July", "August": "August", "September": "September",
    "Oktober": "October", "November": "November", "Dezember": "December"
}

# Datenbankverbindung
DB_CONFIG = {
    "host": os.getenv("HOST"),
    "port": int(os.getenv("PORT")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("PASSWORD"),
    "database": os.getenv("DATABASE_DEST") 
}

def analyze_sentiment(text):
    """ 
    Sentiment-Analyse für wirtschaftliche Artikel.
    """
    blob = TextBlob(text)
    sentiment_score = blob.sentiment.polarity

    if sentiment_score > 0.1:
        return "positiv"
    elif sentiment_score < -0.1:
        return "negativ"
    else:
        return "neutral"

def determine_category(title, description):
    """
    Bestimmt die Kategorie eines Artikels basierend auf Schlüsselwörtern.
    """
    text = (title + " " + description).lower()

    for keyword, category in CATEGORY_MAPPING.items():
        if keyword.lower() in text:
            return category
    
    return "Sonstiges"  # Falls keine passende Kategorie gefunden wird

def round_time_to_hour(dt):
    """Rundet die Zeit auf die nächste volle Stunde"""
    return dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

def convert_german_date(date_str):
    """Ersetzt deutsche Monatsnamen durch englische und entfernt unnötige Zeichen."""
    # Entfernen von Ortsnamen wie "Berlin," und extrahieren des Datums
    date_str = date_str.split(',')[-1].strip()  # Nimmt nur das Datum nach dem Komma
    
    # Entfernen des Punktes hinter dem Tag (z.B. '20.' wird zu '20')
    date_str = date_str.replace('.', '')
    
    # Mapping von deutschen zu englischen Monatsnamen
    month_mapping = {
        "Januar": "January", "Februar": "February", "März": "March",
        "April": "April", "Mai": "May", "Juni": "June",
        "Juli": "July", "August": "August", "September": "September",
        "Oktober": "October", "November": "November", "Dezember": "December"
    }
    
    # Ersetze deutsche Monatsnamen durch englische
    for de, en in month_mapping.items():
        if de in date_str:
            date_str = date_str.replace(de, en)
            break

    return date_str


def generate_zeitid(rounded_date):
    """Erstellt die ZeitID im Format YYYY-MM-DD:HH-MM"""
    return rounded_date.strftime("%Y-%m-%d:%H-%M")

def fetch_news(url, category):
    """Scrapt die News von einer Webseite und gibt eine Liste zurück."""
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

def save_news_to_db(news_data):
    """Speichert die News-Artikel in die Datenbank"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO News (titel, summary, link, category, einschaetzung, ZeitID)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                summary = VALUES(summary), 
                category = VALUES(category),
                einschaetzung = VALUES(einschaetzung),
                ZeitID = VALUES(ZeitID);
        """

        cursor.executemany(insert_query, news_data)
        conn.commit()

        print(f"{len(news_data)} Artikel erfolgreich gespeichert.")
        
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Fehler bei der Datenbankspeicherung: {err}")

def get_economic_news():
    """Holt sich die wirtschaftlichen Nachrichten vom RSS Feed der Tagesschau und speichert sie in der DB."""
    try:
        response = requests.get(RSS_URL, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        response.raise_for_status()
        
        feed = feedparser.parse(response.content)
        print(f"Aktuelle Artikel: {len(feed.entries)}\n")

        news_data = []

        for entry in feed.entries:
            title = entry.title
            description = entry.summary
            link = entry.link

            # Nur wirtschaftlich relevante Artikel berücksichtigen
            if any(keyword.lower() in (title + " " + description).lower() for keyword in ECONOMIC_KEYWORDS):
                # Original-Zeit aus dem Artikel
                pub_date = datetime(*entry.published_parsed[:6])

                # Zeit auf die nächste volle Stunde runden
                rounded_date = round_time_to_hour(pub_date)

                sentiment = analyze_sentiment(title + " " + description)
                category = determine_category(title, description)

                print(f"\nArtikel: {title}")
                print(f"Link: {link}")
                print(f"Kategorie: {category}")
                print(f"Einschätzung: {sentiment}")
                print(f"Datum (gerundet): {rounded_date.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # ZeitID generieren
                zeitid = generate_zeitid(rounded_date)

                news_data.append((title, description, link, category, sentiment, zeitid))

        # Speichern der RSS-News
        if news_data:
            save_news_to_db(news_data)
        else:
            print("Keine wirtschaftsrelevanten Nachrichten gefunden.")

    except Exception as e:
        print(f"Fehler: {str(e)}")

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
        news_data = []
        for news in news_list:
            sentiment = analyze_sentiment(news['title'] + " " + news['summary'])
            category = determine_category(news['title'], news['summary'])

            # Datum konvertieren
            converted_date = convert_german_date(news['date'])

            try:
                # Versuchen, das Datum im englischen Format zu parsen
                pub_date = datetime.strptime(converted_date, "%d %B %Y")  # %d %B %Y passt zu englischem Datum
                rounded_date = round_time_to_hour(pub_date)

                # Generiere ZeitID
                zeitid = generate_zeitid(rounded_date)

                news_data.append((news['title'], news['summary'], news['link'], category, sentiment, zeitid))
            except ValueError as e:
                print(f"Fehler beim Verarbeiten des Datums '{news['date']}': {e}")

        # Speichern der analysierten Nachrichten in der DB
        if news_data:
            save_news_to_db(news_data)

    # Auch die Tagesschau-RSS-Feeds einholen
    get_economic_news()

if __name__ == "__main__":
    print("Starte den täglichen News-Scraper...")
    main()
