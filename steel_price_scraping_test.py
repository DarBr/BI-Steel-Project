import requests
from bs4 import BeautifulSoup
import mysql.connector
from datetime import datetime

# URL der Webseite mit dem Stahlpreis
url = "https://www.scrapethissite.com/pages/"

# HTTP-Request an die Webseite senden
response = requests.get(url)

# Inhalt der Webseite parsen
soup = BeautifulSoup(response.text, 'html.parser')

# Text der Klasse 'col-md-6 col-md-offset-3' extrahieren
element = soup.find_all(class_='page-title')

for e in element:
    print(e.text)
