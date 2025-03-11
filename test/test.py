import mysql.connector
import random
import time

# Verbindung zur Datenbank herstellen
connection = mysql.connector.connect(
    host="3.142.199.164",   # Deine Host-IP
    port=3306,              # Standardport für MySQL
    user="user",            # Dein Benutzername
    password="clientserver",  # Dein Passwort
    database="database-dwh"  # Name deiner Datenbank
)
cursor = connection.cursor()

# Erstelle die Tabelle, falls sie nicht existiert
cursor.execute('''
CREATE TABLE IF NOT EXISTS test_tabelle (
    id INT AUTO_INCREMENT PRIMARY KEY,
    zahl INT
)
''')
connection.commit()

try:
    while True:
        # Zufallszahl zwischen 1 und 10 generieren
        zufallszahl = random.randint(1, 10)
        
        # Zahl in die Datenbank einfügen
        cursor.execute('''
            INSERT INTO test_tabelle (zahl)
            VALUES (%s)
        ''', (zufallszahl,))
        connection.commit()
        
        print(f"Neue Zahl eingefügt: {zufallszahl}")
        
        # 60 Sekunden warten
        time.sleep(60)
except KeyboardInterrupt:
    print("Script durch Benutzer beendet.")
finally:
    # Verbindung schließen
    connection.close()
