import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     os.getenv("DB_PORT", 5432),
    "dbname":   os.getenv("DB_NAME", "maritime"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

CSV_PATH = "Data/port_details.csv"

CREATE_TABLE_SQL = """
CREATE TABLE public.wpi (
    oid                             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wpi_number                      BIGINT,
    region_name                     TEXT,
    port_name                       TEXT,
    alt_name                        TEXT,
    port_code                       TEXT,
    country_code                    TEXT,
    water_body                      TEXT,
    harbor_size                     TEXT,
    harbor_type                     TEXT,
    lat                             DOUBLE PRECISION,
    lon                             DOUBLE PRECISION,
    sailing_direction               TEXT,
    standard_nautical_chart         TEXT,
    tidal_range_m                   DOUBLE PRECISION,
    entrance_width_m                DOUBLE PRECISION,
    channel_depth_m                 DOUBLE PRECISION,
    anchorage_depth_m               DOUBLE PRECISION,
    cargo_pier_depth_m              DOUBLE PRECISION,
    oil_terminal_depth_m            DOUBLE PRECISION,
    lng_terminal_depth_m            DOUBLE PRECISION,
    max_vessel_length_m             DOUBLE PRECISION,
    max_vessel_beam_m               DOUBLE PRECISION,
    max_vessel_draft_m              DOUBLE PRECISION,
    offshore_max_vessel_length_m    DOUBLE PRECISION,
    offshore_max_vessel_beam_m      DOUBLE PRECISION,
    offshore_max_vessel_draft_m     DOUBLE PRECISION,
    harbor_use                      TEXT,
    port_security                   TEXT,
    search_and_rescue               TEXT,
    medical_facilities              TEXT,
    dirty_ballast_disposal          TEXT,
    repairs                         TEXT,
    dry_dock                        TEXT
);
"""

COLUMN_MAP = {
    "wpi_number":                               "wpi_number",
    "region_name":                              "region_name",
    "port_name":                                "port_name",
    "alt_name":                                 "alt_name",
    "port_code":                                "port_code",
    "country_code":                             "country_code",
    "water_body":                               "water_body",
    "harbor_size":                              "harbor_size",
    "harbor_type":                              "harbor_type",
    "lat":                                      "lat",
    "lon":                                      "lon",
    "Sailing Direction or Publication":         "sailing_direction",
    "Standard Nautical Chart":                  "standard_nautical_chart",
    "Tidal Range (m)":                          "tidal_range_m",
    "Entrance Width (m)":                       "entrance_width_m",
    "Channel Depth (m)":                        "channel_depth_m",
    "Anchorage Depth (m)":                      "anchorage_depth_m",
    "Cargo Pier Depth (m)":                     "cargo_pier_depth_m",
    "Oil Terminal Depth (m)":                   "oil_terminal_depth_m",
    "Liquified Natural Gas Terminal Depth (m)": "lng_terminal_depth_m",
    "Maximum Vessel Length (m)":                "max_vessel_length_m",
    "Maximum Vessel Beam (m)":                  "max_vessel_beam_m",
    "Maximum Vessel Draft (m)":                 "max_vessel_draft_m",
    "Offshore Maximum Vessel Length (m)":       "offshore_max_vessel_length_m",
    "Offshore Maximum Vessel Beam (m)":         "offshore_max_vessel_beam_m",
    "Offshore Maximum Vessel Draft (m)":        "offshore_max_vessel_draft_m",
    "Harbor Use":                               "harbor_use",
    "Port Security":                            "port_security",
    "Search and Rescue":                        "search_and_rescue",
    "Medical Facilities":                       "medical_facilities",
    "Dirty Ballast Disposal":                   "dirty_ballast_disposal",
    "Repairs":                                  "repairs",
    "Dry Dock":                                 "dry_dock",
}

DB_COLUMNS = list(COLUMN_MAP.values())

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS public.wpi;")
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()

def load_csv(path):
    df = pd.read_csv(path, dtype=str, index_col=False)
    df.columns = df.columns.str.strip()
    df.drop(columns=["oid"], errors="ignore", inplace=True)
    return df

def prepare_rows(df):
    csv_cols = [c for c in COLUMN_MAP if c in df.columns]
    df = df[csv_cols].copy()
    df.rename(columns=COLUMN_MAP, inplace=True)

    numeric_cols = [
        "wpi_number", "lat", "lon",
        "tidal_range_m", "entrance_width_m", "channel_depth_m",
        "anchorage_depth_m", "cargo_pier_depth_m", "oil_terminal_depth_m",
        "lng_terminal_depth_m", "max_vessel_length_m", "max_vessel_beam_m",
        "max_vessel_draft_m", "offshore_max_vessel_length_m",
        "offshore_max_vessel_beam_m", "offshore_max_vessel_draft_m",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.where(pd.notnull(df), None)
    present_cols = [c for c in DB_COLUMNS if c in df.columns]
    df = df[present_cols]
    return present_cols, [tuple(row) for row in df.itertuples(index=False, name=None)]

def ingest(conn, columns, rows):
    insert_sql = sql.SQL(
        "INSERT INTO public.wpi ({fields}) VALUES %s"
    ).format(fields=sql.SQL(", ").join(map(sql.Identifier, columns)))

    with conn.cursor() as cur:
        execute_values(cur, insert_sql.as_string(conn), rows, page_size=500)
    conn.commit()
    print(f"Inserted {len(rows):,} rows into public.wpi")

def main():
    df = load_csv(CSV_PATH)
    conn = get_connection()
    try:
        create_table(conn)
        columns, rows = prepare_rows(df)
        ingest(conn, columns, rows)
    finally:
        conn.close()

if __name__ == "__main__":
    main()