import os
import json
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from shapely.geometry import LineString, shape, Point
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "maritime"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

EVENTTYPE_NAMES = {
    "EQ": "Earthquake",
    "TC": "Tropical Cyclone",
    "FL": "Flood",
    "VO": "Volcano",
    "DR": "Drought"
}

ROUTE_EVENTTYPES = {"EQ", "TC", "VO"}

ALERT_COLORS = {
    "red":    "#e53935",
    "orange": "#FB8C00",
    "green":  "#43A047"
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)


def _build_route_line(segments):
    all_coords = []
    for seg in segments:
        for lat, lon in seg["coordinates"]:
            all_coords.append((lon, lat))
    if len(all_coords) < 2:
        return None
    return LineString(all_coords)


def _collect_port_coords(segments):
    ports = []
    for seg in segments:
        ports.append((seg["from"]["lon"], seg["from"]["lat"]))
    if segments:
        ports.append((segments[-1]["to"]["lon"], segments[-1]["to"]["lat"]))
    return ports


def _bbox_from_line(route_line, buffer_deg=2.0):
    return route_line.buffer(buffer_deg).bounds


def _fetch_candidates(cur, bbox, eventtypes):
    placeholders = ",".join(["%s"] * len(eventtypes))
    cur.execute(f"""
        SELECT
            eventtype, eventid, episodeid, name,
            alertlevel, alertscore,
            fromdate, todate,
            severity, severitytext, severityunit,
            country, iso3,
            longitude, latitude,
            event_polygon
        FROM disaster_events
        WHERE todate >= NOW() - INTERVAL '3 days'
          AND event_polygon IS NOT NULL
          AND eventtype IN ({placeholders})
          AND longitude BETWEEN %s AND %s
          AND latitude  BETWEEN %s AND %s
    """, (*eventtypes, bbox[0], bbox[2], bbox[1], bbox[3]))
    return cur.fetchall()


def _intersects(route_line, event_polygon_json):
    try:
        geojson = event_polygon_json if isinstance(event_polygon_json, dict) else json.loads(event_polygon_json)
        for feature in geojson.get("features", []):
            geom = shape(feature["geometry"])
            if route_line.intersects(geom):
                return True
    except Exception:
        pass
    return False


def _format_event(row):
    etype = row["eventtype"]
    alert = (row["alertlevel"] or "green").lower()
    return {
        "eventtype":      etype,
        "eventtype_name": EVENTTYPE_NAMES.get(etype, etype),
        "eventid":        row["eventid"],
        "episodeid":      row["episodeid"],
        "name":           row["name"],
        "alertlevel":     alert,
        "color":          ALERT_COLORS.get(alert, "#43A047"),
        "alertscore":     row["alertscore"],
        "fromdate":       row["fromdate"].strftime("%Y-%m-%d") if row["fromdate"] else None,
        "todate":         row["todate"].strftime("%Y-%m-%d") if row["todate"] else None,
        "severity":       row["severity"],
        "severitytext":   row["severitytext"],
        "severityunit":   row["severityunit"],
        "country":        row["country"],
        "iso3":           row["iso3"],
        "longitude":      row["longitude"],
        "latitude":       row["latitude"],
        "geojson":        row["event_polygon"] if isinstance(row["event_polygon"], dict) else json.loads(row["event_polygon"]),
    }


def get_disasters_for_route(segments):
    if not segments:
        return {"route_events": [], "port_events": []}

    route_line = _build_route_line(segments)
    if route_line is None:
        return {"route_events": [], "port_events": []}

    bbox        = _bbox_from_line(route_line, buffer_deg=2.0)
    port_coords = _collect_port_coords(segments)

    route_events = []
    port_events  = []
    seen_route   = set()
    seen_port    = set()

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:

                candidates = _fetch_candidates(cur, bbox, list(ROUTE_EVENTTYPES))
                for row in candidates:
                    key = (row["eventid"], row["episodeid"])
                    if key in seen_route:
                        continue
                    if _intersects(route_line, row["event_polygon"]):
                        seen_route.add(key)
                        route_events.append(_format_event(row))

                all_candidates = _fetch_candidates(cur, bbox, list(EVENTTYPE_NAMES.keys()))
                for row in all_candidates:
                    key = (row["eventid"], row["episodeid"])
                    if key in seen_port:
                        continue
                    try:
                        geojson = row["event_polygon"] if isinstance(row["event_polygon"], dict) else json.loads(row["event_polygon"])
                        for feature in geojson.get("features", []):
                            geom = shape(feature["geometry"])
                            for plon, plat in port_coords:
                                pt = Point(plon, plat)
                                if geom.contains(pt) or geom.distance(pt) <= 2.0:
                                    seen_port.add(key)
                                    port_events.append(_format_event(row))
                                    raise StopIteration
                    except StopIteration:
                        pass
                    except Exception:
                        pass

    except Exception as e:
        print(f"[disasters] DB error: {e}")
        return {"route_events": [], "port_events": []}

    print(f"[disasters] route_events={len(route_events)} port_events={len(port_events)}")
    return {"route_events": route_events, "port_events": port_events}