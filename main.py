import mysql.connector

def connect_to_db():
    try:
        # Verbindung aufbauen
        connection = mysql.connector.connect(
            host="3.142.199.164",   # Deine Host-IP
            port=3306,             # Standardport für MySQL
            user="user",           # Dein Benutzername
            password="clientserver",  # Dein Passwort
            database="database-steel" # Name deiner Datenbank
        )
        
        if connection.is_connected():
            print("Erfolgreich verbunden.")
            
            # Cursor erstellen
            cursor = connection.cursor()
            
            # Beispiel: Datensatz in T_Bestellung einfügen
            # Passen Sie die Spalten an die Struktur in Ihrer Tabelle an!
            insert_query = """
                INSERT INTO B_Bestellung (BestellNr, KundenID)
                VALUES (%s, %s)
            """
            # Beispielwerte
            data = (123, 300)
            
            # Abfrage ausführen und Änderungen speichern
            cursor.execute(insert_query, data)
            connection.commit()
            
            print("Datensatz erfolgreich eingefügt.")
            
            # Kontrolle: auslesen, ob Datensatz drin ist
            cursor.execute("SELECT * FROM T_Bestellung;")
            results = cursor.fetchall()
            for row in results:
                print(row)
            
            # Ressourcen freigeben
            cursor.close()
            connection.close()
    
    except mysql.connector.Error as err:
        print(f"Fehler: {err}")
        
def insert_kunde(name, plz, ort, telefon, email):
    try:
        connection = mysql.connector.connect(
            host="3.142.199.164",
            port=3306,
            user="user",
            password="clientserver",
            database="database-steel"
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            insert_query = """
                INSERT INTO K_Kunde (Name, PLZ, Ort, Telefon, Email)
                VALUES (%s, %s, %s, %s, %s)
            """
            data = (name, plz, ort, telefon, email)
            cursor.execute(insert_query, data)
            connection.commit()
            print("Kunde erfolgreich eingefügt.")
            cursor.close()
            connection.close()
    
    except mysql.connector.Error as err:
        print(f"Fehler: {err}")

if __name__ == "__main__":
    insert_kunde("Max Mustermann", "12345", "Musterstadt", "0123456789", "lienig@mail.com")
