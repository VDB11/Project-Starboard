import os
import json
import time
import requests
import psycopg2
import threading
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2.pool import ThreadedConnectionPool
from shapely.geometry import LineString, shape, Point
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dotenv import load_dotenv
from io import StringIO
import csv

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

DISASTERS = {
    "EQ": "earthquake",
    "TC": "cyclone",
    "FL": "flood",
    "VO": "volcano",
    "DR": "drought"
}

BASE_URL        = "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
DATA_DIR        = os.path.join(BASE_DIR, "../Data/gdacs_disaster")
POLYGON_WORKERS = 10
API_WORKERS     = 5
CURRENT_YEAR    = datetime.now().year

_ingest_lock    = threading.Lock()
_ingest_running = False


def get_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)


def _ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS disaster_events (
                eventtype       TEXT,
                eventid         INTEGER,
                episodeid       INTEGER,
                name            TEXT,
                htmldescription TEXT,
                url_geometry    TEXT,
                alertlevel      TEXT,
                alertscore      FLOAT,
                iscurrent       BOOLEAN,
                country         TEXT,
                iso3            TEXT,
                fromdate        TIMESTAMP,
                todate          TIMESTAMP,
                severity        FLOAT,
                severitytext    TEXT,
                severityunit    TEXT,
                longitude       FLOAT,
                latitude        FLOAT,
                event_bbox      JSONB,
                event_polygon   JSONB,
                filename        TEXT,
                inserted_at     TIMESTAMP,
                PRIMARY KEY (eventid, episodeid)
            );
        """)
    conn.commit()


def _get_last_ingested_dates(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT eventtype, MAX(todate)
            FROM disaster_events
            WHERE filename LIKE %s
            GROUP BY eventtype
        """, (f"%{CURRENT_YEAR}%",))
        return {row[0]: row[1] for row in cur.fetchall()}


def _fetch_gdacs_page(code, name, fromdate, page):
    today = datetime.now().strftime("%Y-%m-%d")
    params = {
        "eventlist":  code,
        "fromdate":   fromdate,
        "todate":     today,
        "alertlevel": "red;orange;green",
        "pagesize":   100,
        "pagenumber": page
    }
    for attempt in range(3):
        try:
            r = requests.get(BASE_URL, params=params, timeout=30)
            return r.json().get("features", [])
        except Exception:
            time.sleep(3)
    return None


def _fetch_all_for_type(code, name, fromdate):
    all_features = []
    page = 1
    while True:
        features = _fetch_gdacs_page(code, name, fromdate, page)
        if features is None or not features:
            break
        all_features.extend(features)
        if len(features) < 100:
            break
        page += 1
        time.sleep(0.3)

    filepath = os.path.join(DATA_DIR, f"gdacs_events_{CURRENT_YEAR}_{name}.json")
    existing = []
    if os.path.exists(filepath):
        with open(filepath) as f:
            existing = json.load(f)

    existing_ids = {
        (ft.get("properties", {}).get("eventid"), ft.get("properties", {}).get("episodeid"))
        for ft in existing
    }
    new_features = [
        ft for ft in all_features
        if (ft.get("properties", {}).get("eventid"), ft.get("properties", {}).get("episodeid")) not in existing_ids
    ]
    merged = existing + new_features
    with open(filepath, "w") as f:
        json.dump(merged, f)

    print(f"[ingest] {name}: {len(new_features)} new events merged")
    return filepath


SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
})


def _fetch_polygon(url):
    for attempt in range(3):
        try:
            r = SESSION.get(url, timeout=15)
            if r.status_code == 200:
                data = r.json()
                if data.get("features"):
                    return data
            elif r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", 10)))
        except Exception:
            time.sleep(2)
    return None


def _parse_and_insert(conn, filepath):
    filename = os.path.basename(filepath)
    with open(filepath) as f:
        features = json.load(f)

    with conn.cursor() as cur:
        cur.execute("SELECT eventid, episodeid FROM disaster_events WHERE filename = %s", (filename,))
        existing_keys = {(r[0], r[1]) for r in cur.fetchall()}

    records = {}
    ingested_at = datetime.now()

    for feature in features:
        geom  = feature.get("geometry", {})
        props = feature.get("properties", {})
        bbox  = feature.get("bbox")
        eventid   = props.get("eventid")
        episodeid = props.get("episodeid")
        key = (eventid, episodeid)

        if key in existing_keys or geom.get("type") != "Point":
            continue

        coords = geom.get("coordinates", [None, None])
        records[key] = {
            "eventtype":       props.get("eventtype"),
            "eventid":         eventid,
            "episodeid":       episodeid,
            "name":            props.get("name"),
            "htmldescription": props.get("htmldescription"),
            "url_geometry":    props.get("url", {}).get("geometry"),
            "alertlevel":      props.get("alertlevel"),
            "alertscore":      props.get("alertscore"),
            "iscurrent":       props.get("iscurrent", "false").lower() == "true",
            "country":         props.get("country"),
            "iso3":            props.get("iso3"),
            "fromdate":        props.get("fromdate"),
            "todate":          props.get("todate"),
            "severity":        props.get("severitydata", {}).get("severity"),
            "severitytext":    props.get("severitydata", {}).get("severitytext"),
            "severityunit":    props.get("severitydata", {}).get("severityunit"),
            "longitude":       coords[0],
            "latitude":        coords[1],
            "event_bbox":      json.dumps(bbox) if bbox else None,
            "event_polygon":   None,
            "filename":        filename,
            "inserted_at":     ingested_at,
        }

    if not records:
        return

    def fetch(key_record):
        key, record = key_record
        url = record.get("url_geometry")
        if not url:
            return key, record
        data = _fetch_polygon(url)
        if data:
            record["event_polygon"] = json.dumps(data)
        return key, record

    with ThreadPoolExecutor(max_workers=POLYGON_WORKERS) as executor:
        futures = [executor.submit(fetch, (k, v)) for k, v in records.items()]
        for future in as_completed(futures):
            key, record = future.result()
            records[key] = record

    cols = [
        "eventtype", "eventid", "episodeid", "name", "htmldescription",
        "url_geometry", "alertlevel", "alertscore", "iscurrent", "country",
        "iso3", "fromdate", "todate", "severity", "severitytext",
        "severityunit", "longitude", "latitude", "event_bbox", "event_polygon",
        "filename", "inserted_at"
    ]

    rows = list(records.values())
    buf  = StringIO()
    writer = csv.writer(buf, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)
    for r in rows:
        writer.writerow([
            r["eventtype"] or "", r["eventid"], r["episodeid"],
            r["name"] or "", r["htmldescription"] or "", r["url_geometry"] or "",
            r["alertlevel"] or "", r["alertscore"] if r["alertscore"] is not None else "",
            r["iscurrent"], r["country"] or "", r["iso3"] or "",
            r["fromdate"] or "", r["todate"] or "",
            r["severity"] if r["severity"] is not None else "",
            r["severitytext"] or "", r["severityunit"] or "",
            r["longitude"] if r["longitude"] is not None else "",
            r["latitude"] if r["latitude"] is not None else "",
            r["event_bbox"] or "", r["event_polygon"] or "",
            r["filename"] or "", r["inserted_at"],
        ])
    buf.seek(0)

    try:
        with conn.cursor() as cur:
            cur.copy_expert(
                f"COPY disaster_events ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', QUOTE '\"', NULL '')",
                buf
            )
        conn.commit()
        print(f"[ingest] {filename}: inserted {len(rows)} records")
    except Exception as e:
        conn.rollback()
        print(f"[ingest] COPY failed: {e}, falling back")
        values = [tuple(r[c] for c in cols) for r in rows]
        with conn.cursor() as cur:
            execute_values(cur, f"""
                INSERT INTO disaster_events ({', '.join(cols)}) VALUES %s
                ON CONFLICT (eventid, episodeid) DO NOTHING
            """, values, page_size=500)
        conn.commit()


def run_ingest():
    global _ingest_running
    with _ingest_lock:
        if _ingest_running:
            return
        _ingest_running = True

    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        conn = psycopg2.connect(**DB_CONFIG)
        _ensure_table(conn)
        last_dates = _get_last_ingested_dates(conn)
        conn.close()

        tasks = []
        for code, name in DISASTERS.items():
            last = last_dates.get(code)
            fromdate = last.strftime("%Y-%m-%d") if last else f"{CURRENT_YEAR}-01-01"
            tasks.append((code, name, fromdate))

        filepaths = []
        with ThreadPoolExecutor(max_workers=API_WORKERS) as executor:
            futures = {executor.submit(_fetch_all_for_type, code, name, fromdate): name
                       for code, name, fromdate in tasks}
            for future in as_completed(futures):
                try:
                    filepaths.append(future.result())
                except Exception as e:
                    print(f"[ingest] fetch failed: {e}")

        conn = psycopg2.connect(**DB_CONFIG)
        for filepath in filepaths:
            try:
                _parse_and_insert(conn, filepath)
            except Exception as e:
                print(f"[ingest] insert failed for {filepath}: {e}")
        conn.close()
        print("[ingest] completed")

    except Exception as e:
        print(f"[ingest] pipeline error: {e}")
    finally:
        with _ingest_lock:
            _ingest_running = False


def trigger_ingest_background():
    with _ingest_lock:
        if _ingest_running:
            return
    t = threading.Thread(target=run_ingest, daemon=True)
    t.start()


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