import json
import logging
import os
import re
import requests
import time
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dotenv import load_dotenv
from io import StringIO
import csv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     os.getenv("DB_PORT", 5432),
    "dbname":   os.getenv("DB_NAME", "maritime"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

BASE_URL  = "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.path.join(BASE_DIR, "../Data/gdacs_disaster")
LOG_DIR   = os.path.join(BASE_DIR, "../Logs")

DISASTERS = {
    "EQ": "earthquake",
    "TC": "cyclone",
    "FL": "flood",
    "VO": "volcano",
    "DR": "drought"
}

POLYGON_WORKERS  = 5
API_WORKERS      = 5
CURRENT_YEAR     = datetime.now().year

CREATE_TABLE = """
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
"""

logger = None


def setup_logger():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_filename = os.path.join(LOG_DIR, f"gdacs_ingest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    _logger = logging.getLogger("gdacs")
    _logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(log_filename)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    _logger.addHandler(fh)
    _logger.addHandler(ch)
    _logger.info(f"Logging to: {log_filename}")
    return _logger


def get_last_ingested_dates(pool):
    """Get the most recent todate per event type for current year files."""
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT eventtype, MAX(todate)
                FROM disaster_events
                WHERE filename LIKE %s
                GROUP BY eventtype
            """, (f"%{CURRENT_YEAR}%",))
            result = {row[0]: row[1] for row in cur.fetchall()}
    finally:
        pool.putconn(conn)
    logger.info(f"[REFRESH] Last ingested dates for {CURRENT_YEAR}: {result}")
    return result


def fetch_gdacs_data(args):
    """Fetch events from GDACS API from fromdate to today, merge into current year file."""
    year, code, name, fromdate = args
    all_features = []
    page = 1
    today = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"[FETCH] Starting — disaster={name} | fromdate={fromdate} | todate={today}")

    while True:
        params = {
            "eventlist":  code,
            "fromdate":   fromdate,
            "todate":     today,
            "alertlevel": "red;orange;green",
            "pagesize":   100,
            "pagenumber": page
        }
        features = None
        for attempt in range(3):
            try:
                logger.debug(f"[FETCH] {name} | page={page} | attempt={attempt+1}")
                response = requests.get(BASE_URL, params=params, timeout=30)
                features = response.json().get("features", [])
                logger.debug(f"[FETCH] {name} | page={page} | got {len(features)} features")
                break
            except Exception as e:
                logger.warning(f"[FETCH] {name} | page={page} | attempt={attempt+1} failed: {e}")
                time.sleep(5)

        if features is None:
            logger.error(f"[FETCH] {name} | page={page} | all attempts failed, stopping")
            break
        if not features:
            logger.info(f"[FETCH] {name} | page={page} | empty, pagination complete")
            break

        all_features.extend(features)
        logger.info(f"[FETCH] {name} | page={page} | cumulative={len(all_features)}")

        if len(features) < 100:
            logger.info(f"[FETCH] {name} | last page (got {len(features)} < 100)")
            break

        page += 1
        time.sleep(0.5)

    # Merge into current year file — never overwrite existing events
    filepath = os.path.join(DATA_DIR, f"gdacs_events_{year}_{name}.json")
    existing_features = []
    if os.path.exists(filepath):
        with open(filepath) as f:
            existing_features = json.load(f)

    existing_ids = {
        (ft.get("properties", {}).get("eventid"), ft.get("properties", {}).get("episodeid"))
        for ft in existing_features
    }
    new_features = [
        ft for ft in all_features
        if (ft.get("properties", {}).get("eventid"), ft.get("properties", {}).get("episodeid")) not in existing_ids
    ]
    merged = existing_features + new_features
    with open(filepath, "w") as f:
        json.dump(merged, f, indent=2)

    logger.info(f"[FETCH] Merged {len(new_features)} new events into {filepath} (total={len(merged)})")
    return filepath


def refresh_current_year(pool):
    """Fetch only new events since last ingested date for each event type."""
    os.makedirs(DATA_DIR, exist_ok=True)
    last_dates = get_last_ingested_dates(pool)

    tasks = []
    for code, name in DISASTERS.items():
        last_date = last_dates.get(code)
        if last_date:
            fromdate = last_date.strftime("%Y-%m-%d")
            logger.info(f"[REFRESH] {name} — incremental fetch from {fromdate} (last ingested todate)")
        else:
            fromdate = f"{CURRENT_YEAR}-01-01"
            logger.info(f"[REFRESH] {name} — no DB record for {CURRENT_YEAR}, fetching from {fromdate}")
        tasks.append((CURRENT_YEAR, code, name, fromdate))

    logger.info(f"[REFRESH] Fetching {len(tasks)} event types concurrently")
    with ThreadPoolExecutor(max_workers=API_WORKERS) as executor:
        futures = {executor.submit(fetch_gdacs_data, t): t for t in tasks}
        for future in as_completed(futures):
            year, code, name, fromdate = futures[future]
            try:
                future.result()
                logger.info(f"[REFRESH] Done — {name}")
            except Exception as e:
                logger.error(f"[REFRESH] Failed — {name}: {e}")

    logger.info("[REFRESH] All event types refreshed")


def get_current_year_files():
    """Return only current year JSON files for all 5 disaster types."""
    files = []
    for name in DISASTERS.values():
        filepath = os.path.join(DATA_DIR, f"gdacs_events_{CURRENT_YEAR}_{name}.json")
        if os.path.exists(filepath):
            files.append(filepath)
        else:
            logger.warning(f"[SCAN] File not found, will be created after fetch: {filepath}")
    logger.info(f"[SCAN] Found {len(files)} current-year files to process")
    return files


def get_db_counts(pool):
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT filename, COUNT(*) FROM disaster_events GROUP BY filename")
            result = {row[0]: row[1] for row in cur.fetchall()}
    finally:
        pool.putconn(conn)
    logger.info(f"[DB] Fetched existing counts for {len(result)} filenames")
    return result


def get_existing_keys(pool):
    """Only load keys from current year to keep memory footprint small."""
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT eventid, episodeid
                FROM disaster_events
                WHERE filename LIKE %s
            """, (f"%{CURRENT_YEAR}%",))
            keys = set(cur.fetchall())
    finally:
        pool.putconn(conn)
    logger.info(f"[DB] Fetched {len(keys)} existing keys for {CURRENT_YEAR}")
    return keys


def parse_files(files, db_counts, existing_keys):
    all_records = {}
    ingested_at = datetime.now()

    for filepath in files:
        filename = os.path.basename(filepath)

        with open(filepath) as f:
            features = json.load(f)

        file_count = len([ft for ft in features if ft.get("geometry", {}).get("type") == "Point"])
        db_count = db_counts.get(filename, 0)

        logger.info(f"[PARSE] {filename} — file_count={file_count} | db_count={db_count}")

        if db_count >= file_count and file_count > 0:
            logger.info(f"[PARSE] {filename} — SKIP: already complete in DB")
            continue

        skipped = 0
        for feature in features:
            geom = feature.get("geometry", {})
            props = feature.get("properties", {})
            bbox = feature.get("bbox")
            eventid = props.get("eventid")
            episodeid = props.get("episodeid")
            key = (eventid, episodeid)

            if key in existing_keys:
                continue

            if geom.get("type") == "Point":
                coords = geom.get("coordinates", [None, None])
                all_records[key] = {
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
            else:
                skipped += 1

        logger.info(f"[PARSE] {filename} — queued: {len(all_records)} | skipped {skipped} non-Point")

    logger.info(f"[PARSE] Total new records to ingest: {len(all_records)}")
    return all_records


SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json"
})


def fetch_polygon(url):
    for attempt in range(3):
        try:
            response = SESSION.get(url, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if data.get("features"):
                    time.sleep(0.1)
                    return data
            elif response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 10))
                logger.warning(f"[POLYGON] Rate limited — waiting {wait}s")
                time.sleep(wait)
            else:
                logger.debug(f"[POLYGON] HTTP {response.status_code} for {url}")
                return None
        except Exception as e:
            logger.debug(f"[POLYGON] Attempt {attempt+1} failed {url}: {e}")
            time.sleep(2)
    return None


def fetch_all_polygons(all_records):
    total = len(all_records)
    success = 0
    failed = 0
    skipped = 0
    logger.info(f"[POLYGON] Fetching polygons for {total} records with {POLYGON_WORKERS} workers")

    def fetch(key_record):
        key, record = key_record
        url = record.get("url_geometry")
        if not url:
            return key, record, None
        data = fetch_polygon(url)
        if data:
            record["event_polygon"] = json.dumps(data)
            return key, record, True
        return key, record, False

    with ThreadPoolExecutor(max_workers=POLYGON_WORKERS) as executor:
        futures = [executor.submit(fetch, (k, v)) for k, v in all_records.items()]
        for i, future in enumerate(as_completed(futures)):
            key, record, ok = future.result()
            all_records[key] = record
            if ok is None:
                skipped += 1
            elif ok:
                success += 1
            else:
                failed += 1
            if (i + 1) % 100 == 0 or (i + 1) == total:
                logger.info(f"[POLYGON] Progress {i+1}/{total} | success={success} | failed={failed} | skipped={skipped}")

    logger.info(f"[POLYGON] Done | success={success} | failed={failed} | skipped={skipped}")
    return all_records


def bulk_insert(pool, records):
    cols = [
        "eventtype", "eventid", "episodeid", "name", "htmldescription",
        "url_geometry", "alertlevel", "alertscore", "iscurrent", "country",
        "iso3", "fromdate", "todate", "severity", "severitytext",
        "severityunit", "longitude", "latitude", "event_bbox", "event_polygon",
        "filename", "inserted_at"
    ]

    rows = list(records.values())
    total = len(rows)
    logger.info(f"[DB] Bulk inserting {total} rows using COPY")

    buf = StringIO()
    writer = csv.writer(buf, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)

    for r in rows:
        writer.writerow([
            r["eventtype"] or "",
            r["eventid"],
            r["episodeid"],
            r["name"] or "",
            r["htmldescription"] or "",
            r["url_geometry"] or "",
            r["alertlevel"] or "",
            r["alertscore"] if r["alertscore"] is not None else "",
            r["iscurrent"],
            r["country"] or "",
            r["iso3"] or "",
            r["fromdate"] or "",
            r["todate"] or "",
            r["severity"] if r["severity"] is not None else "",
            r["severitytext"] or "",
            r["severityunit"] or "",
            r["longitude"] if r["longitude"] is not None else "",
            r["latitude"] if r["latitude"] is not None else "",
            r["event_bbox"] or "",
            r["event_polygon"] or "",
            r["filename"] or "",
            r["inserted_at"],
        ])

    buf.seek(0)
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.copy_expert(
                f"""
                COPY disaster_events ({', '.join(cols)})
                FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', QUOTE '"', NULL '')
                """,
                buf
            )
        conn.commit()
        logger.info(f"[DB] COPY committed — {total} rows inserted")
    except Exception as e:
        conn.rollback()
        logger.error(f"[DB] COPY failed: {e} — falling back to execute_values")
        values = [tuple(r[c] for c in cols) for r in rows]
        query = f"""
            INSERT INTO disaster_events ({', '.join(cols)})
            VALUES %s
            ON CONFLICT (eventid, episodeid) DO NOTHING;
        """
        with conn.cursor() as cur:
            execute_values(cur, query, values, page_size=1000)
        conn.commit()
        logger.info(f"[DB] Fallback insert committed — {total} rows")
    finally:
        pool.putconn(conn)


def main():
    global logger
    logger = setup_logger()

    logger.info("=" * 60)
    logger.info(f"GDACS INGEST PIPELINE STARTED — year={CURRENT_YEAR}")
    logger.info("=" * 60)

    os.makedirs(DATA_DIR, exist_ok=True)

    logger.info("STEP 1: Initializing DB connection pool")
    pool = ThreadedConnectionPool(minconn=2, maxconn=10, **DB_CONFIG)
    logger.info(f"[DB] Connected to {DB_CONFIG['dbname']} at {DB_CONFIG['host']}:{DB_CONFIG['port']}")

    init_conn = pool.getconn()
    with init_conn.cursor() as cur:
        cur.execute(CREATE_TABLE)
    init_conn.commit()
    pool.putconn(init_conn)
    logger.info("[DB] Table disaster_events ensured")

    logger.info("STEP 2: Fetching new events from GDACS API (incremental, current year only)")
    refresh_current_year(pool)

    logger.info("STEP 3: Loading current year DB state")
    db_counts = get_db_counts(pool)
    existing_keys = get_existing_keys(pool)

    logger.info("STEP 4: Parsing current year files")
    files = get_current_year_files()
    all_records = parse_files(files, db_counts, existing_keys)

    if not all_records:
        logger.info("Nothing to ingest — DB is up to date")
    else:
        logger.info(f"STEP 5: Fetching polygons for {len(all_records)} new records")
        all_records = fetch_all_polygons(all_records)

        logger.info(f"STEP 6: Bulk inserting {len(all_records)} records into DB")
        bulk_insert(pool, all_records)

    pool.closeall()
    logger.info("[DB] Connection pool closed")
    logger.info("=" * 60)
    logger.info("GDACS INGEST PIPELINE COMPLETED")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()