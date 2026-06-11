import logging
import os

logger = logging.getLogger(__name__)

FORCE_FAILURE = (
    os.environ.get("FORCE_QUALITY_FAILURE", "false").lower() == "true"
)
FAILURE_CITY = os.environ.get("FAILURE_CITY", "Paris")


def transform(city_name: str, raw: dict) -> dict:
    prepared = {
        "city":           city_name,
        "timestamp":      raw["time"],
        "temperature_c":  raw["temperature_2m"],
        "humidity_pct":   raw["relative_humidity_2m"],
        "wind_speed_kmh": raw["wind_speed_10m"],
    }

    if FORCE_FAILURE and city_name == FAILURE_CITY:
        prepared["temperature_c"] = 999.0
        logger.warning(
            "[%s] SIMULATION : temperature forcee a 999.0 pour test anomalie",
            city_name,
        )

    logger.info("[%s] Donnees transformees : %s", city_name, prepared)
    return prepared
