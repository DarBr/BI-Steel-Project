import requests
import re
import json
from bs4 import BeautifulSoup

url = 'https://tradingeconomics.com/commodity/hrc-steel'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, wie Gecko) Chrome/110.0.0.0 Safari/537.36'
}
response = requests.get(url, headers=headers)

if response.status_code == 200:
    print('Anfrage erfolgreich')
     # Parsen des HTML-Inhalts
    soup = BeautifulSoup(response.content, 'html.parser')

    # Alle <script>-Tags durchsuchen
    script_tags = soup.find_all("script")

    # Initialisierung der Variablen für den Preis
    hrc_price = None

    for tag in script_tags:
        if "TEChartsMeta" in tag.text:
            json_match = re.search(r"TEChartsMeta\s*=\s*(\[.*?\]);", tag.text, re.DOTALL)
            
            if json_match:
                json_data = json.loads(json_match.group(1))  # JSON-Daten umwandeln
                hrc_price = json_data[0].get("value")  # Den Wert extrahieren
                break  # Falls gefunden, beenden wir die Schleife

    # Prüfen, ob ein Preis gefunden wurde
    if hrc_price:
        print(f"Aktueller HRC-Stahlpreis: {hrc_price} USD/T")
    else:
        print("Fehler: Konnte den Stahlpreis nicht finden.")

    # Debugging: Falls kein Script gefunden wurde
    if not script_tags:
        print("Fehler: Kein Script-Tag mit 'TEChartsMeta' gefunden.")
        exit()
else:
    print('Fehler bei der Anfrage:', response.status_code)