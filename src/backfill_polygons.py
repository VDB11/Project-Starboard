import json
import logging
import os
import subprocess
import time
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     os.getenv("DB_PORT", 5432),
    "dbname":   os.getenv("DB_NAME", "maritime"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

POLYGON_WORKERS = 100
LOG_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../Logs")
FAILED_LOG = os.path.join(LOG_DIR, "failed_urls.jsonl")
EVENT_TYPES = None


def setup_logger():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_filename = os.path.join(LOG_DIR, f"backfill_polygons_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logger = logging.getLogger("backfill")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", "%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(log_filename)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def fetch_polygon(url, logger):
    for attempt in range(5):
        try:
            result = subprocess.run(
                [
                    "curl", "-s", "-m", "30",
                    "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    "-H", "Accept: application/json, text/plain, */*",
                    "-H", "Accept-Language: en-US,en;q=0.9",
                    "-H", "Referer: https://www.gdacs.org/",
                    "-H", "Connection: keep-alive",
                    url
                ],
                capture_output=True, text=True, timeout=35
            )
            if not result.stdout.strip():
                logger.debug(f"[POLYGON] Empty response attempt {attempt+1} | {url}")
                time.sleep(2 ** attempt)
                continue
            data = json.loads(result.stdout)
            if data.get("features"):
                return data
            logger.debug(f"[POLYGON] No features attempt {attempt+1} | {url}")
            time.sleep(2 ** attempt)
        except subprocess.TimeoutExpired:
            logger.debug(f"[POLYGON] Timeout attempt {attempt+1} | {url}")
            time.sleep(2 ** attempt)
        except json.JSONDecodeError:
            logger.debug(f"[POLYGON] Bad JSON attempt {attempt+1} | {url}")
            time.sleep(2 ** attempt)
        except Exception as e:
            logger.debug(f"[POLYGON] Error attempt {attempt+1}: {e} | {url}")
            time.sleep(2)
    return None


def fetch_missing_records(pool, logger, event_types=None):
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            if event_types:
                placeholders = ",".join(["%s"] * len(event_types))
                cur.execute(f"""
                    SELECT eventid, episodeid, url_geometry
                    FROM disaster_events
                    WHERE event_polygon IS NULL
                      AND url_geometry IS NOT NULL
                      AND url_geometry != ''
                      AND eventtype IN ({placeholders})
                    ORDER BY fromdate DESC
                """, event_types)
            else:
                cur.execute("""
                    SELECT eventid, episodeid, url_geometry
                    FROM disaster_events
                    WHERE event_polygon IS NULL
                      AND url_geometry IS NOT NULL
                      AND url_geometry != ''
                    ORDER BY fromdate DESC
                """)
            rows = cur.fetchall()
    finally:
        pool.putconn(conn)
    logger.info(f"[DB] Found {len(rows)} records missing polygons")
    return rows


def save_polygon(pool, eventid, episodeid, data, logger):
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE disaster_events
                SET event_polygon = %s
                WHERE eventid = %s AND episodeid = %s
            """, (json.dumps(data), eventid, episodeid))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"[DB] Save failed {eventid}/{episodeid}: {e}")
    finally:
        pool.putconn(conn)


def log_failure(eventid, episodeid, url):
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(FAILED_LOG, "a") as f:
        f.write(json.dumps({"eventid": eventid, "episodeid": episodeid, "url": url}) + "\n")


def run_backfill(pool, logger):
    records = fetch_missing_records(pool, logger, event_types=EVENT_TYPES)
    total = len(records)

    if not total:
        logger.info("Nothing to backfill.")
        return

    success = 0
    failed  = 0

    logger.info(f"[BACKFILL] Starting — {total} records | {POLYGON_WORKERS} workers")

    def fetch_task(row):
        eventid, episodeid, url = row
        data = fetch_polygon(url, logger)
        return eventid, episodeid, url, data

    with ThreadPoolExecutor(max_workers=POLYGON_WORKERS) as executor:
        futures = {executor.submit(fetch_task, row): row for row in records}
        for i, future in enumerate(as_completed(futures)):
            eventid, episodeid, url, data = future.result()
            if data:
                save_polygon(pool, eventid, episodeid, data, logger)
                success += 1
            else:
                log_failure(eventid, episodeid, url)
                failed += 1
            if (i + 1) % 100 == 0 or (i + 1) == total:
                logger.info(f"[BACKFILL] {i+1}/{total} | success={success} | failed={failed}")

    logger.info(f"DONE | total={total} | success={success} | failed={failed}")


def main():
    logger = setup_logger()
    logger.info("POLYGON BACKFILL STARTED")
    pool = ThreadedConnectionPool(minconn=5, maxconn=120, **DB_CONFIG)
    try:
        run_backfill(pool, logger)
    finally:
        pool.closeall()


if __name__ == "__main__":
    main()