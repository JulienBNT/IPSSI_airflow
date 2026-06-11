import logging

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = [
    "city", "timestamp", "temperature_c", "humidity_pct", "wind_speed_kmh"
]

FIELD_RANGES = {
    "temperature_c":  (-50.0, 60.0),
    "humidity_pct":   (0, 100),
    "wind_speed_kmh": (0.0, 300.0),
}


def check(city_name: str, prepared: dict) -> tuple:
    errors = []

    for field in REQUIRED_FIELDS:
        if field not in prepared or prepared[field] is None:
            errors.append(f"Champ manquant ou null : {field}")

    for field, (min_val, max_val) in FIELD_RANGES.items():
        value = prepared.get(field)
        if value is not None and not (min_val <= float(value) <= max_val):
            errors.append(
                f"{field} hors plage [{min_val}, {max_val}] : valeur={value}"
            )

    if errors:
        logger.warning(
            "[%s] Controle qualite KO (%d erreur(s)) : %s",
            city_name,
            len(errors),
            errors,
        )
    else:
        logger.info("[%s] Controle qualite OK", city_name)

    return len(errors) == 0, errors
