# BI-System für ein Stahlproduktionsunternehmen

## 📌 Projektübersicht
**Ziel:** Entwicklung eines Business-Intelligence-Systems zur Optimierung der Stahlproduktion durch Echtzeitanalyse von Marktdaten, Energiepreisen und Produktionskennzahlen.

### Key Features
- **Echtzeit-Dashboards** für Produktion & Vertrieb
- **ML-basierte Energiepreisprognosen** (24h-Vorhersage)
- **Automatisierte Datenpipelines** mit CronJobs
- **Snowflake-DWH** mit 6 Dimensionen & 3 Faktentabellen
- **Kostensimulator** für Produktionsplanung

## 🛠️ Kernkomponenten
### Datenquellen
| Kategorie           | Beispiele                                                                 |
|---------------------|---------------------------------------------------------------------------|
| **Energiepreise**   | EPEX Spot API (15-Minuten-Intervalle)                                     |
| **Rohstoffpreise**  | LME Eisenpreise, TradingEconomics Coal API                               |
| **Stahlmarkt**      | HRC Steel Preisdaten (Web Scraping)                                       |
| **News/Makrodaten** | WV Stahl Newsfeed, AlphaVantage News Sentiment API                       |

### Datenmodell (Snowflake-Schema)
```python
# Beispiel-Dimensionen
Dim_Zeit = {
    "ZeitID": "2025-03-23:14-00",
    "Wochentag": "Sonntag",
    "Feiertag": False
}

Dim_Maschine = {
    "MaschinenID": 102,
    "MaxKapazität": 15.2,  # Tonnen/Stunde
    "Energieverbrauch": 9.3  # MWh/Stunde
}

# Faktentabelle Produktion
Fakt_Produktion = {
    "Energiekosten/Tonne": 87.50,
    "Ausschussquote": 2.4  # %
}