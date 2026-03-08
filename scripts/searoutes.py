import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import searoute as sr
from scripts.fuzzy_search import FuzzySearch

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "port":     os.getenv("DB_PORT"),
    "dbname":   os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

def get_water_bodies():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT water_body FROM public.wpi WHERE water_body IS NOT NULL ORDER BY water_body")
            return [r["water_body"] for r in cur.fetchall()]

def search_water_bodies(q):
    candidates = get_water_bodies()
    return FuzzySearch.search(q, candidates, limit=50, threshold=85)

def get_countries(water_body):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT country_code FROM public.wpi WHERE water_body = %s AND country_code IS NOT NULL ORDER BY country_code",
                (water_body,)
            )
            return [r["country_code"] for r in cur.fetchall()]

def search_countries(q, water_body):
    candidates = get_countries(water_body)
    return FuzzySearch.search(q, candidates, limit=50, threshold=85)

def get_ports(water_body, country_code):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT port_name, port_code, lat, lon FROM public.wpi WHERE water_body = %s AND country_code = %s AND lat IS NOT NULL AND lon IS NOT NULL ORDER BY port_name",
                (water_body, country_code)
            )
            return [dict(r) for r in cur.fetchall()]

def search_ports(q, water_body, country_code):
    ports = get_ports(water_body, country_code)
    candidates = [p["port_name"] for p in ports]
    return FuzzySearch.search(q, candidates, limit=50, threshold=85)

def get_port_coords(port_name, country_code, water_body):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT lat, lon, port_name, port_code FROM public.wpi WHERE port_name = %s AND country_code = %s AND water_body = %s LIMIT 1",
                (port_name, country_code, water_body)
            )
            result = cur.fetchone()
            return dict(result) if result else None

def calculate_segment(origin, destination):
    o = [origin["lon"], origin["lat"]]
    d = [destination["lon"], destination["lat"]]
    try:
        route = sr.searoute(o, d, units="naut", append_orig_dest=True)
        coords = [[c[1], c[0]] for c in route.geometry["coordinates"]]
        length = route.properties["length"]
        return {"coordinates": coords, "length": length}
    except Exception as e:
        print(f"Searoute segment error: {e}")
        return None

def calculate_full_route(stops):
    segments = []
    total_length = 0

    for i in range(len(stops) - 1):
        origin = get_port_coords(stops[i]["port_name"], stops[i]["country_code"], stops[i]["water_body"])
        destination = get_port_coords(stops[i+1]["port_name"], stops[i+1]["country_code"], stops[i+1]["water_body"])

        if not origin or not destination:
            print(f"Port not found for segment {i+1}")
            return None

        segment = calculate_segment(origin, destination)
        if not segment:
            print(f"Route calculation failed for segment {i+1}")
            return None

        segments.append({
            "from": origin,
            "to": destination,
            "coordinates": segment["coordinates"],
            "length": round(segment["length"], 2)
        })
        total_length += segment["length"]
        print(f"Segment {i+1}: {origin['port_name']} → {destination['port_name']} | {round(segment['length'], 2)} naut mi")

    print(f"Total: {round(total_length, 2)} nautical miles across {len(segments)} segment(s)")
    return {
        "segments": segments,
        "total_length": round(total_length, 2),
        "units": "nautical miles"
    }