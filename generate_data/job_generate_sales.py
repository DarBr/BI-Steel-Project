import os
import pandas as pd
import mysql.connector
import sys
from datetime import datetime, timedelta
import random
import math

def import_auftraege(conn, csv_path_orders):
    print("=== Starte Import der Aufträge ===")
    cursor = conn.cursor()
    df = pd.read_csv(csv_path_orders, parse_dates=['Bestelldatum', 'Lieferdatum'])
    today = pd.Timestamp.today().normalize()
    df_to_insert = df[df['Bestelldatum'] <= today]
    
    insert_query = """
        INSERT IGNORE INTO tb_Kundenauftrag
        (AuftragsID, KundenID, Bestelldatum, Auftragsvolumen, Lieferdatum)
        VALUES (%s, %s, %s, %s, %s)
    """
    
    for _, row in df_to_insert.iterrows():
        cursor.execute(insert_query, (
            int(row['AuftragsID']),
            int(row['KundenID']),
            row['Bestelldatum'].strftime('%Y-%m-%d'),
            float(row['Auftragsvolumen']),
            row['Lieferdatum'].strftime('%Y-%m-%d')
        ))
    
    conn.commit()
    df_remaining = df[df['Bestelldatum'] > today]
    df_remaining.to_csv(csv_path_orders, index=False)
    
    print("Import der Aufträge abgeschlossen.")
    cursor.close()

def import_positionen(conn, csv_path_positions):
    print("=== Starte Import der Auftragspositionen ===")
    cursor = conn.cursor()
    df = pd.read_csv(csv_path_positions)
    cursor.execute("SELECT AuftragsID FROM tb_Kundenauftrag")
    existing_orders = set(row[0] for row in cursor.fetchall())
    df_to_insert = df[df['AuftragsID'].isin(existing_orders)]
    
    insert_query_positions = """
        INSERT IGNORE INTO tb_Kundenauftragspositionen
        (ID, ProduktID, Menge, Preis, AuftragsID)
        VALUES (%s, %s, %s, %s, %s)
    """
    
    cursor2 = conn.cursor()
    inserted_indices = []
    
    for index, row in df_to_insert.iterrows():
        cursor.execute(insert_query_positions, (
            int(row['ID']),
            int(row['ProduktID']),
            int(row['Menge']),
            float(row['Preis']),
            int(row['AuftragsID'])
        ))
        
        if cursor.rowcount == 1:
            inserted_indices.append(index)
            cursor2.execute("""
                SELECT Bestelldatum FROM tb_Kundenauftrag
                WHERE AuftragsID = %s
            """, (int(row['AuftragsID']),))
            result = cursor2.fetchone()
            if not result:
                continue
            bestelldatum = result[0]
            lieferdatum = bestelldatum + timedelta(days=random.randint(3, 5))
            
            cursor2.execute("""
                SELECT MaterialID, Verhaeltnis FROM tb_MaterialZuProdukt
                WHERE ProduktID = %s
            """, (int(row['ProduktID']),))
            mat_rows = cursor2.fetchall()
            
            insert_bestellung = """
                INSERT IGNORE INTO tb_Bestellung
                (MaterialID, LieferantID, Bestellmenge, Bestelldatum, Lieferdatum)
                VALUES (%s, %s, %s, %s, %s)
            """
            for (mat_id, verh) in mat_rows:
                bestellmenge = math.ceil(int(row['Menge']) * verh)
                lieferant_id = random.randint(1, 4)
                cursor2.execute(insert_bestellung, (
                    mat_id, lieferant_id, bestellmenge, bestelldatum, lieferdatum
                ))
    
    conn.commit()
    cursor2.close()
    df_remaining = df.drop(index=inserted_indices)
    df_remaining.to_csv(csv_path_positions, index=False)
    print("Import der Positionen abgeschlossen.")
    cursor.close()

def main():
    try:
        conn = mysql.connector.connect(
            host="13.60.244.59",
            port=3306,
            user="user",
            password="clientserver",
            database="database-steel"
        )
        script_dir = os.path.dirname(__file__)
        data_dir = os.path.join(script_dir, "data")
        csv_path_orders = os.path.join(data_dir, "auftraege_6_monate.csv")
        csv_path_positions = os.path.join(data_dir, "Kundenauftragspositionen_exakt.csv")
        
        import_auftraege(conn, csv_path_orders)
        import_positionen(conn, csv_path_positions)
        
        conn.close()
        print("=== Gesamter Import erfolgreich abgeschlossen ===")
    except Exception as e:
        print(f"Fehler beim Import: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    main()