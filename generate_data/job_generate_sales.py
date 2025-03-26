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

    df = pd.read_csv(
        csv_path_orders,
        parse_dates=['Bestelldatum', 'Lieferdatum']
    )

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

            # Lieferdatum ableiten
            cursor2.execute("""
                SELECT Bestelldatum
                FROM tb_Kundenauftrag
                WHERE AuftragsID = %s
            """, (int(row['AuftragsID']),))
            result = cursor2.fetchone()
            if not result:
                continue
            bestelldatum = result[0]
            offset_days = random.randint(3, 5)
            lieferdatum = bestelldatum + timedelta(days=offset_days)

            # tb_MaterialZuProdukt abfragen
            cursor2.execute("""
                SELECT MaterialID, Verhaeltnis
                FROM tb_MaterialZuProdukt
                WHERE ProduktID = %s
            """, (int(row['ProduktID']),))
            mat_rows = cursor2.fetchall()

            # Bestellung anlegen
            insert_bestellung = """
                INSERT IGNORE INTO tb_Bestellung
                (MaterialID, LieferantID, Bestellmenge, Bestelldatum, Lieferdatum)
                VALUES (%s, %s, %s, %s, %s)
            """
            for (mat_id, verh) in mat_rows:
                bestellmenge = math.ceil(int(row['Menge']) * verh)
                lieferant_id = random.randint(1, 4)

                cursor2.execute(insert_bestellung, (
                    mat_id,
                    lieferant_id,
                    bestellmenge,
                    bestelldatum,
                    lieferdatum
                ))

    conn.commit()
    cursor2.close()

    df_remaining = df.drop(index=inserted_indices)
    df_remaining.to_csv(csv_path_positions, index=False)

    print("Import der Positionen abgeschlossen.")
    cursor.close()

def update_lagerbestand(conn):
    """
    Aktualisiert den Lagerbestand in tb_Lagerbestand, basierend auf:
      - Bestellungen (tb_Bestellung) mit Lieferdatum=heute -> Zugänge
      - Kundenaufträge (tb_Kundenauftrag) mit Lieferdatum=heute -> Abgänge
        (Anhand tb_Kundenauftragspositionen + tb_MaterialZuProdukt)

    Falls am selben Tag Zugänge + Abgänge für dasselbe Material auftreten,
    wird net addiert/subtrahiert und ein neuer Eintrag für heute angelegt.
    """
    print("=== Starte Lagerbestand-Update ===")

    cursor = conn.cursor()
    today_str = datetime.now().strftime('%Y-%m-%d')

    # 1) Zugänge aus tb_Bestellung (Lieferdatum = heute)
    cursor.execute("""
        SELECT MaterialID, SUM(Bestellmenge) AS Zugang
        FROM tb_Bestellung
        WHERE DATE(Lieferdatum) = %s
        GROUP BY MaterialID
    """, (today_str,))
    zugang_map = { row[0]: row[1] for row in cursor.fetchall() }

    # 2) Abgänge aus tb_Kundenauftrag + tb_Kundenauftragspositionen + tb_MaterialZuProdukt
    #    wo tb_Kundenauftrag.Lieferdatum = heute
    cursor.execute("""
        SELECT mzu.MaterialID,
               SUM(kpos.Menge * mzu.Verhaeltnis) AS Abgang
        FROM tb_Kundenauftragspositionen kpos
        JOIN tb_Kundenauftrag ka ON kpos.AuftragsID = ka.AuftragsID
        JOIN tb_MaterialZuProdukt mzu ON kpos.ProduktID = mzu.ProduktID
        WHERE DATE(ka.Lieferdatum) = %s
        GROUP BY mzu.MaterialID
    """, (today_str,))
    abgang_map = { row[0]: row[1] for row in cursor.fetchall() }

    # Alle betroffenen Materialien (Zugang oder Abgang)
    all_materials = set(zugang_map.keys()) | set(abgang_map.keys())
    if not all_materials:
        print("Heute keine Lagerbewegung (kein Zugang/Abgang).")
        return

    for mat_id in all_materials:
        # alten Bestand holen (letzter Eintrag in tb_Lagerbestand)
        cursor.execute("""
            SELECT Menge, Mindestbestand
            FROM tb_Lagerbestand
            WHERE MaterialID = %s
            ORDER BY LagerID DESC
            LIMIT 1
        """, (mat_id,))
        row = cursor.fetchone()
        if row:
            alter_bestand = row[0]
            alter_mindest = row[1]
        else:
            alter_bestand = 0
            alter_mindest = 0  # oder falls du anfangs was anderes willst

        zugang = zugang_map.get(mat_id, 0)
        abgang = abgang_map.get(mat_id, 0)

        # Falls Zugänge + Abgänge am gleichen Tag -> net
        neuer_bestand = alter_bestand + zugang - abgang
        # Du könntest abfangen, wenn neuer_bestand < 0 => Engpass?

        # Neuer Eintrag in tb_Lagerbestand
        insert_lager = """
            INSERT INTO tb_Lagerbestand
            (MaterialID, Menge, Mindestbestand, Bestandsdatum)
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_lager, (
            mat_id,
            neuer_bestand,
            alter_mindest,
            today_str
        ))

    conn.commit()
    cursor.close()
    print("Lagerbestand für heute aktualisiert.")

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
        data_dir = os.path.join(script_dir, "data")  # Verweis auf den "data"-Ordner

        csv_path_orders = os.path.join(data_dir, "auftraege_6_monate.csv")
        csv_path_positions = os.path.join(data_dir, "Kundenauftragspositionen_exakt.csv")


        # 1) Aufträge importieren
        import_auftraege(conn, csv_path_orders)

        # 2) Positionen importieren (inkl. Bestellungen)
        import_positionen(conn, csv_path_positions)

        # 3) Lagerbestand aktualisieren
        update_lagerbestand(conn)

        conn.close()
        print("=== Gesamter Import erfolgreich abgeschlossen ===")

    except Exception as e:
        print(f"Fehler beim Import: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    main()
