import json
import logging
import os
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

API_URL = os.environ.get(
    "OPEN_METEO_URL", "https://api.open-meteo.com/v1/forecast"
)
FIELDS = "temperature_2m,relative_humidity_2m,wind_speed_10m"
ARCHIVE_DIR = Path("/opt/airflow/archive")

CITY_COORDS = {
    "Paris":     {"latitude": 48.8566, "longitude":  2.3522},
    "Lyon":      {"latitude": 45.7640, "longitude":  4.8357},
    "Marseille": {"latitude": 43.2965, "longitude":  5.3698},
    "Bordeaux":  {"latitude": 44.8378, "longitude": -0.5792},
    "Lille":     {"latitude": 50.6292, "longitude":  3.0573},
    "Nantes":    {"latitude": 47.2184, "longitude": -1.5536},
}


def fetch_and_archive(city_name: str, run_id: str) -> dict:
    coords = CITY_COORDS[city_name]
    url = (
        f"{API_URL}"
        f"?latitude={coords['latitude']}&longitude={coords['longitude']}"
        f"&current={FIELDS}&timezone=Europe/Paris"
    )
    logger.info("[%s] Appel API Open-Meteo", city_name)
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    raw = response.json()["current"]
    logger.info(
        "[%s] Reponse brute recue : %s",
        city_name,
        json.dumps(raw, indent=2),
    )

    safe_run_id = (
        run_id.replace(":", "-").replace("+", "_").replace("/", "_")
    )
    city_dir = ARCHIVE_DIR / city_name.lower()
    city_dir.mkdir(parents=True, exist_ok=True)
    archive_path = city_dir / f"{safe_run_id}.json"
    with open(archive_path, "w") as f:
        json.dump(raw, f, indent=2)
    logger.info("[%s] Archive sauvegardee : %s", city_name, archive_path)

    return raw
