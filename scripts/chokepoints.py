import os
import math
import uuid
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta, date

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "port":     os.getenv("DB_PORT"),
    "dbname":   os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

CHOKEPOINT_PROXIMITY_NMI = 100
ARCGIS_URL = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Chokepoints_Data/FeatureServer/0/query"

def get_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

def ensure_columns():
    cols = {
        "n_container":           "INTEGER",
        "n_dry_bulk":            "INTEGER",
        "n_general_cargo":       "INTEGER",
        "n_roro":                "INTEGER",
        "n_tanker":              "INTEGER",
        "n_cargo":               "INTEGER",
        "n_total":               "INTEGER",
        "capacity_container":    "BIGINT",
        "capacity_dry_bulk":     "BIGINT",
        "capacity_general_cargo":"BIGINT",
        "capacity_roro":         "BIGINT",
        "capacity_tanker":       "BIGINT",
        "capacity_cargo":        "BIGINT",
        "capacity":              "BIGINT",
        "data_date":             "DATE",
        "inserted_at":           "TIMESTAMPTZ",
        "last_api_check":        "DATE",
    }
    with get_conn() as conn:
        with conn.cursor() as cur:
            for col, dtype in cols.items():
                cur.execute(f"ALTER TABLE public.maritime_chokepoints ADD COLUMN IF NOT EXISTS {col} {dtype}")
        conn.commit()

def get_last_api_check():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(last_api_check) AS d FROM public.maritime_chokepoints")
            row = cur.fetchone()
            return row["d"] if row else None

def set_last_api_check(check_date):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE public.maritime_chokepoints SET last_api_check = %s", (check_date,))
        conn.commit()

def get_latest_api_date():
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": "date",
        "returnGeometry": "false",
        "resultRecordCount": 1,
        "orderByFields": "date DESC"
    }
    r = requests.get(ARCGIS_URL, params=params, timeout=30)
    r.raise_for_status()
    ts = r.json()["features"][0]["attributes"]["date"]
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date()

def get_last_ingested_date():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(data_date) AS d FROM public.maritime_chokepoints WHERE data_date IS NOT NULL")
            row = cur.fetchone()
            return row["d"] if row else None

def fetch_transit_for_date(date_str):
    params = {
        "f": "json",
        "where": f"date = DATE '{date_str}'",
        "outFields": "*",
        "returnGeometry": "false",
        "resultRecordCount": 1000,
        "resultOffset": 0
    }
    r = requests.get(ARCGIS_URL, params=params, timeout=30)
    r.raise_for_status()
    return [f["attributes"] for f in r.json().get("features", [])]

def get_base_chokepoints():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (name) id, name, lat, lon
                FROM public.maritime_chokepoints
                WHERE data_date IS NULL
                ORDER BY name
            """)
            return {row["name"].lower(): dict(row) for row in cur.fetchall()}

def ingest_transit_data(records, data_date):
    base = get_base_chokepoints()
    now = datetime.now(tz=timezone.utc)

    with get_conn() as conn:
        with conn.cursor() as cur:
            for rec in records:
                portname = rec.get("portname", "").strip().lower()
                base_cp = base.get(portname)
                if not base_cp:
                    print(f"No DB match for: {portname}")
                    continue

                cur.execute("""
                    INSERT INTO public.maritime_chokepoints (
                        id, name, lat, lon,
                        n_container, n_dry_bulk, n_general_cargo, n_roro, n_tanker,
                        n_cargo, n_total,
                        capacity_container, capacity_dry_bulk, capacity_general_cargo,
                        capacity_roro, capacity_tanker, capacity_cargo, capacity,
                        data_date, inserted_at, last_api_check
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s
                    )
                """, (
                    str(uuid.uuid1()),
                    base_cp["name"],
                    base_cp["lat"],
                    base_cp["lon"],
                    rec.get("n_container"),
                    rec.get("n_dry_bulk"),
                    rec.get("n_general_cargo"),
                    rec.get("n_roro"),
                    rec.get("n_tanker"),
                    rec.get("n_cargo"),
                    rec.get("n_total"),
                    rec.get("capacity_container"),
                    rec.get("capacity_dry_bulk"),
                    rec.get("capacity_general_cargo"),
                    rec.get("capacity_roro"),
                    rec.get("capacity_tanker"),
                    rec.get("capacity_cargo"),
                    rec.get("capacity"),
                    data_date,
                    now,
                    date.today()
                ))
                print(f"Inserted: {base_cp['name']} for {data_date}")
        conn.commit()

def refresh_if_needed():
    ensure_columns()
    today = date.today()
    last_check = get_last_api_check()

    if last_check and last_check >= today:
        print(f"API already checked today ({today}), skipping.")
        return

    print(f"Checking ArcGIS API for latest data...")
    api_date = get_latest_api_date()
    last_ingested = get_last_ingested_date()

    if last_ingested and last_ingested >= api_date:
        set_last_api_check(today)
        print(f"Data already up to date for {api_date}, skipping ingest.")
        return

    print(f"New data available: {api_date}, ingesting...")
    records = fetch_transit_for_date(str(api_date))

    if not records:
        for days_back in range(1, 5):
            fallback = api_date - timedelta(days=days_back)
            records = fetch_transit_for_date(str(fallback))
            if records:
                api_date = fallback
                print(f"Fell back to {api_date}")
                break

    if records:
        ingest_transit_data(records, api_date)
        print(f"Ingested {len(records)} records for {api_date}")
    else:
        set_last_api_check(today)
        print("No records found, skipping.")

def get_all_chokepoints():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (name)
                    id, name, lat, lon,
                    n_container, n_dry_bulk, n_general_cargo, n_roro, n_tanker,
                    n_cargo, n_total,
                    capacity_container, capacity_dry_bulk, capacity_general_cargo,
                    capacity_roro, capacity_tanker, capacity_cargo, capacity,
                    data_date
                FROM public.maritime_chokepoints
                WHERE data_date IS NOT NULL
                ORDER BY name, inserted_at DESC
            """)
            return [dict(r) for r in cur.fetchall()]

def _haversine_nmi(lat1, lon1, lat2, lon2):
    R = 3440.065
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def _point_to_segment_distance_nmi(px, py, ax, ay, bx, by):
    dx, dy = bx - ax, by - ay
    if dx == 0 and dy == 0:
        return _haversine_nmi(py, px, ay, ax)
    t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
    return _haversine_nmi(py, px, ay + t * dy, ax + t * dx)

def _min_distance_to_route_nmi(cp_lat, cp_lon, route_coords):
    min_dist = float("inf")
    for i in range(len(route_coords) - 1):
        a_lat, a_lon = route_coords[i]
        b_lat, b_lon = route_coords[i + 1]
        d = _point_to_segment_distance_nmi(cp_lon, cp_lat, a_lon, a_lat, b_lon, b_lat)
        if d < min_dist:
            min_dist = d
    return min_dist

def get_chokepoints_on_route(route_coords, threshold_nmi=CHOKEPOINT_PROXIMITY_NMI):
    if not route_coords or len(route_coords) < 2:
        return []

    refresh_if_needed()
    chokepoints = get_all_chokepoints()
    hits = []

    for cp in chokepoints:
        dist = _min_distance_to_route_nmi(cp["lat"], cp["lon"], route_coords)
        if dist <= threshold_nmi:
            hits.append({
                "id":                        str(cp["id"]),
                "name":                      cp["name"],
                "lat":                       cp["lat"],
                "lon":                       cp["lon"],
                "distance_nmi":              round(dist, 1),
                "data_date":                 str(cp["data_date"]),
                "n_container":               cp["n_container"],
                "n_dry_bulk":                cp["n_dry_bulk"],
                "n_general_cargo":           cp["n_general_cargo"],
                "n_roro":                    cp["n_roro"],
                "n_tanker":                  cp["n_tanker"],
                "n_cargo":                   cp["n_cargo"],
                "n_total":                   cp["n_total"],
                "capacity_container":        cp["capacity_container"],
                "capacity_dry_bulk":         cp["capacity_dry_bulk"],
                "capacity_general_cargo":    cp["capacity_general_cargo"],
                "capacity_roro":             cp["capacity_roro"],
                "capacity_tanker":           cp["capacity_tanker"],
                "capacity_cargo":            cp["capacity_cargo"],
                "capacity":                  cp["capacity"],
            })
            print(f"Chokepoint HIT: {cp['name']} ({round(dist, 1)} nmi from route)")
        else:
            print(f"Chokepoint MISS: {cp['name']} ({round(dist, 1)} nmi from route)")

    print(f"Found {len(hits)} chokepoint(s) on route (threshold: {threshold_nmi} nmi)")
    return hits