# Steel Data Warehouse & Analytics Project

**Über dieses Projekt**
In diesem Projekt wird eine End-to-End-Datenplattform für die Stahlindustrie realisiert. Ziel ist es, operative Daten aus Fertigung, Vertrieb und Lagerbestand zu zentralisieren, aufzubereiten und für Analysen und Prognosen nutzbar zu machen. Hierzu werden sowohl interne Quellsysteme als auch externe Datenquellen integriert, um fundierte Entscheidungen entlang der gesamten Wertschöpfungskette zu unterstützen.

# Steel Data Warehouse & Analytics Project

## Projektübersicht
Dieses Projekt verfolgt das Ziel, ein skalierbares Data Warehouse für eine Stahlproduktionsumgebung aufzubauen und gleichzeitig fortgeschrittene Analysen und Prognosen zu ermöglichen. Im Kern werden Produktions-, Verkaufs- und Lagerbestanddaten aus operativen Quellsystemen extrahiert, transformiert und in ein Data Warehouse (DWH) geladen. Gemeinsam mit der Simulation historischer Produktionsaufträge und der Automatisierung täglicher Lagerbestands‑Snapshots bildet das DWH die Grundlage für konsolidierte Berichte und Dashboards.

Darüber hinaus werden externe Datenquellen angebunden:

- **Energiepreise**: Historische Spotmarktpreise werden explorativ analysiert und mittels LSTM-Zeitreihenmodell prognostiziert.
- **HRC-Stahlpreise**: Aktuelle Marktpreise von TradingEconomics werden automatisch abgeholt und im DWH gespeichert.
- **Wirtschaftsnachrichten**: RSS-Feeds und Fachportale werden gescraped, sentiment‑analysiert und kategorisiert, um wirtschaftliche Entwicklungen zu kontextualisieren.

Diese Kombination aus ETL-, Analyse‑ und Integrationskomponenten ermöglicht datengetriebene Entscheidungen entlang des gesamten Produktions‑ und Lieferprozesses.

---

## Inhaltsverzeichnis
1. [Voraussetzungen](#voraussetzungen)
2. [Konfiguration](#konfiguration)
3. [Installation](#installation)
4. [Skripte & Nutzung](#skripte--nutzung)
   - [Dimensionstabellen-ETL](#dimensionstabellen-etl)
   - [Fakten-ETL](#fakten-etl)
   - [Auftrags- & Positions-Import](#auftrags--positions-import)
   - [Lagerbestand-Update](#lagerbestand-update)
   - [Strompreisanalyse](#strompreisanalyse)
   - [Modelltraining & -eval](#modelltraining--eval)
   - [Prognosespeicherung](#prognosespeicherung)
   - [HRC-Stahlpreis-Fetcher](#hrc-stahlpreis-fetcher)
   - [Wirtschaftsnachrichten-Scraper](#wirtschaftsnachrichten-scraper)
5. [Projektstruktur](#projektstruktur)
6. [Beitrag](#beitrag)
7. [Lizenz](#lizenz)

---

## Voraussetzungen
- **Python 3.8+**
- MySQL-Datenbanken (Quell-DB und Data Warehouse)
- Netzwerkzugriff auf beide Datenbanken

## Konfiguration
Erstelle eine `.env`-Datei im Projektstamm mit folgenden Variablen:

```dotenv
# Datenbank-Verbindung (Quell-DB)
HOST=your_source_db_host
PORT=3306
DB_USER=your_user
PASSWORD=your_password
DATABASE_SOURCE=database-steel

# Data Warehouse (Ziel-DB)
DATABASE_DEST=database-dwh
```

## Installation
1. Projekt klonen:
```bash
git clone https://github.com/DarBr/BI-Steel-Project.git
cd BI-Steel-Project
```
2. Virtuelle Umgebung erstellen und aktivieren:
```bash
python -m venv venv
source venv/bin/activate    # Linux/Mac
venv\Scripts\activate.bat   # Windows
```
3. Abhängigkeiten installieren:
```bash
pip install -r requirements.txt
```

## Skripte & Nutzung
### Dimensionstabellen-ETL
Zieht Stammdaten aus `database-steel` und lädt sie in Dimensionstabellen im DWH.
```bash
python etl_dim_tables.py
```

### Fakten-ETL
Importiert Produktions‑ und Verkaufsaufträge und schreibt sie in Faktentabellen.
```bash
python etl_facts.py
```

### Auftrags- & Positions-Import
Lädt Kundenaufträge aus CSV, importiert zugehörige Positionen und erzeugt Materialbestellungen.
```bash
python import_orders_and_positions.py
```

### Lagerbestand-Update
Erzeugt heute-basierte Lagerbestandseinträge in `tb_Lagerbestand`.
```bash
python update_inventory.py
```

### Strompreisanalyse
Explorative Analyse historischer Spotmarktpreise.
```bash
python energy_price_analysis.py
```

### Modelltraining & -eval
Trainiert bzw. lädt das LSTM-Modell für Strompreisprognosen:
```bash
# Training (falls kein Modell vorhanden)
python train_model.py

# Auswertung
python evaluate_model.py
```

### Prognosespeicherung
Berechnet die nächsten 24 Stunden Prognose und speichert sie im DWH.
```bash
python forecast_energy_price.py
```

### HRC-Stahlpreis-Fetcher
Extrahiert aktuellen HRC‑Stahlpreis von TradingEconomics und speichert ihn.
```bash
python fetch_hrc_price.py
```

### Wirtschaftsnachrichten-Scraper
Sammelt wirtschaftsrelevante Artikel (Tagesschau & Fachportale) und speichert sie mit Sentiment.
```bash
python news_scraper.py
```

## Projektstruktur
```text
BI-STEEL-PROJECT/
├── .venv/                          # Virtuelle Umgebung
├── etl_pipelines_to_dwh/          # ETL-Skripts zum Laden ins DWH
│   ├── cronjob/                   # Cronjob-fähige Pipelines
│   │   ├── etl_dimensions.py
│   │   └── etl_fakts.py
│   └── standalone/                # Einzelne ETL-Module
│       ├── dim_kunden.py
│       ├── dim_maschinen.py
│       ├── dim_material.py
│       ├── dim_produkt.py
│       ├── fakt_lagerbestand.py
│       ├── fakt_produktionsaufträge.py
│       ├── fakt_sales.py
│       └── ZuordnungProduktMaterial.py
├── generate_data/                 # Skripte zur Datengenerierung
│   ├── data/                      # Beispiel-CSV-Dateien
│   │   ├── auftraege_6_monate.csv
│   │   └── Kundenauftragspositionen_exakt.csv
│   ├── insert_historical_production_data.py
│   ├── job_generate_production_data.py
│   └── job_generate_sales.py
├── machine_learning/              # ML-Module für Strompreisprognose
│   ├── analysis.py
│   ├── build_model.py
│   ├── energy_prices_data.csv
│   ├── evaluate_model.py
│   ├── generate_forecast.py
│   ├── model.ipynb
│   └── test.py
├── scraping/                      # Scraper für Stahlpreise & News
│   ├── scraping-steel-price.py
│   └── sentiment_analysis_news.py
├── .gitignore
├── README.md
└── requirements.txt
```

> Die Skripte sind funktionsorientiert in Ordnern gruppiert:
> - **etl_pipelines_to_dwh/**: Ladeprozesse für Dimensions- und Faktentabellen
> - **generate_data/**: Generierung und Import historischer Testdaten
> - **machine_learning/**: Analyse und Modellierung der Energiepreise
> - **scraping/**: Live-Datenintegration (Marktpreise & RSS-Feeds)

### Datenquellen
| Kategorie           | Beispiele                                                                 |
|---------------------|---------------------------------------------------------------------------|
| **Energiepreise**   | EPEX Spot API (15-Minuten-Intervalle)                                     |
| **Rohstoffpreise**  | LME Eisenpreise, TradingEconomics Coal API                               |
| **Stahlmarkt**      | HRC Steel Preisdaten (Web Scraping)                                       |
| **News/Makrodaten** | WV Stahl Newsfeed, AlphaVantage News Sentiment API                       |
