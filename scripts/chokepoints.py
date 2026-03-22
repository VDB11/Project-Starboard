import os
import math
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "port":     os.getenv("DB_PORT"),
    "dbname":   os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# Detection threshold in nautical miles
CHOKEPOINT_PROXIMITY_NMI = 100

def get_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

def _haversine_nmi(lat1, lon1, lat2, lon2):
    """Great-circle distance between two points in nautical miles."""
    R = 3440.065  # Earth radius in nautical miles
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def _point_to_segment_distance_nmi(px, py, ax, ay, bx, by):
    """
    Minimum distance (nmi) from point P to line segment A→B.
    Works in lon/lat space — good enough at maritime scales.
    """
    dx, dy = bx - ax, by - ay
    if dx == 0 and dy == 0:
        # Segment is a single point
        return _haversine_nmi(py, px, ay, ax)
    t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
    closest_x = ax + t * dx
    closest_y = ay + t * dy
    return _haversine_nmi(py, px, closest_y, closest_x)

def _min_distance_to_route_nmi(cp_lat, cp_lon, route_coords):
    """
    Minimum distance from chokepoint to any segment of the route (nautical miles).
    route_coords: list of [lat, lon]
    """
    min_dist = float("inf")
    for i in range(len(route_coords) - 1):
        a_lat, a_lon = route_coords[i]
        b_lat, b_lon = route_coords[i + 1]
        d = _point_to_segment_distance_nmi(cp_lon, cp_lat, a_lon, a_lat, b_lon, b_lat)
        if d < min_dist:
            min_dist = d
    return min_dist

def get_all_chokepoints():
    """Fetch all chokepoints from DB."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
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
                FROM public.maritime_chokepoints
                ORDER BY name
            """)
            return [dict(r) for r in cur.fetchall()]

def get_chokepoints_on_route(route_coords, threshold_nmi=CHOKEPOINT_PROXIMITY_NMI):
    """
    Return chokepoints whose closest approach to the route is within threshold_nmi.
    Each result includes full vessel count data and the actual distance.
    """
    if not route_coords or len(route_coords) < 2:
        return []

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
                "vessel_count_total":        cp["vessel_count_total"],
                "vessel_count_container":    cp["vessel_count_container"],
                "vessel_count_dry_bulk":     cp["vessel_count_dry_bulk"],
                "vessel_count_general_cargo":cp["vessel_count_general_cargo"],
                "vessel_count_roro":         cp["vessel_count_roro"],
                "vessel_count_tanker":       cp["vessel_count_tanker"],
            })
            print(f"Chokepoint HIT: {cp['name']} ({round(dist, 1)} nmi from route)")
        else:
            print(f"Chokepoint MISS: {cp['name']} ({round(dist, 1)} nmi from route)")

    print(f"Found {len(hits)} chokepoint(s) on route (threshold: {threshold_nmi} nmi)")
    return hits