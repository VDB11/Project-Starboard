import os
import csv
import uuid
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "port":     os.getenv("DB_PORT"),
    "dbname":   os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS public.maritime_chokepoints (
    id                          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                        TEXT NOT NULL,
    lat                         DOUBLE PRECISION NOT NULL,
    lon                         DOUBLE PRECISION NOT NULL,
    vessel_count_total          INTEGER,
    vessel_count_container      INTEGER,
    vessel_count_dry_bulk       INTEGER,
    vessel_count_general_cargo  INTEGER,
    vessel_count_roro           INTEGER,
    vessel_count_tanker         INTEGER
);
"""

INSERT_ROW = """
INSERT INTO public.maritime_chokepoints (
    id,
    name,
    lat,
    lon,
    vessel_count_total,
    vessel_count_container,
    vessel_count_dry_bulk,
    vessel_count_general_cargo,
    vessel_count_roro,
    vessel_count_tanker
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO NOTHING;
"""

CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "Data", "chokepoints.csv")

def ingest():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                # Create table
                cur.execute(CREATE_TABLE)
                print("Table 'maritime_chokepoints' ready.")

                # Read and insert CSV
                with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    count = 0
                    for row in reader:
                        cur.execute(INSERT_ROW, (
                            str(uuid.uuid1()),
                            row["name"],
                            float(row["lat"]),
                            float(row["lon"]),
                            int(row["vessel_count_total"]),
                            int(row["vessel_count_container"]),
                            int(row["vessel_count_dry_bulk"]),
                            int(row["vessel_count_general_cargo"]),
                            int(row["vessel_count_RoRo"]),
                            int(row["vessel_count_tanker"]),
                        ))
                        count += 1

                print(f"Inserted {count} chokepoint(s) successfully.")
    finally:
        conn.close()

if __name__ == "__main__":
    ingest()