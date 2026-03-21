import os
import sys
import logging
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(r"C:\Users\visha\OneDrive\Desktop\VB_Projects\Starboard\Project-Starboard")
sys.path.insert(0, str(PROJECT_ROOT))

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "ingest.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 60)
    logger.info("GDACS ingest started at %s", datetime.now().isoformat(timespec="seconds"))
    logger.info("Project root : %s", PROJECT_ROOT)

    try:
        from scripts.disasters import run_ingest        

        run_ingest()
        logger.info("Ingest completed successfully.")

    except ModuleNotFoundError as exc:
        logger.error("Could not import ingest module: %s", exc)
        logger.error(
            "Make sure 'scripts/ingest.py' exists and all dependencies are installed "
            "in the active Python environment."
        )
        sys.exit(1)

    except Exception as exc:
        logger.exception("Ingest failed with an unexpected error: %s", exc)
        sys.exit(1)

    logger.info("=" * 60)

if __name__ == "__main__":
    main()