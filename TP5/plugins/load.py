import logging
import os

import psycopg2

logger = logging.getLogger(__name__)


def get_conn():
    return psycopg2.connect(
        host=os.environ["DB_METEO_HOST"],
        port=int(os.environ.get("DB_METEO_PORT", 5432)),
        user=os.environ["DB_METEO_USER"],
        password=os.environ["DB_METEO_PASSWORD"],
        dbname=os.environ["DB_METEO_NAME"],
    )


def load_weather(city_name: str, prepared: dict) -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO weather_data
                    (city, timestamp, temperature_c, humidity_pct, wind_speed_kmh)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (city, timestamp) DO NOTHING
                """,
                (
                    prepared["city"],
                    prepared["timestamp"],
                    prepared["temperature_c"],
                    prepared["humidity_pct"],
                    prepared["wind_speed_kmh"],
                ),
            )
            affected = cur.rowcount
        conn.commit()
        if affected == 0:
            logger.info(
                "[%s] Ligne deja presente — aucun doublon cree (idempotence).",
                city_name,
            )
        else:
            logger.info("[%s] Ligne inseree dans weather_data.", city_name)
    finally:
        conn.close()


def flag_anomaly(
    city_name: str,
    dag_run_id: str,
    timestamp: str,
    errors: list,
) -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quality_anomalies
                    (city, dag_run_id, timestamp, errors)
                VALUES (%s, %s, %s, %s)
                """,
                (city_name, dag_run_id, timestamp, errors),
            )
        conn.commit()
        logger.warning(
            "[%s] Anomalie tracee dans quality_anomalies : %s",
            city_name,
            errors,
        )
    finally:
        conn.close()


def write_ingestion_log(
    dag_run_id: str,
    execution_date: str,
    cities_ok: int,
    cities_ko: int,
) -> None:
    status = "success" if cities_ko == 0 else "partial"
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_log
                    (dag_run_id, execution_date, cities_ok, cities_ko, status)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (dag_run_id) DO NOTHING
                """,
                (dag_run_id, execution_date, cities_ok, cities_ko, status),
            )
        conn.commit()
        logger.info(
            "Ingestion loggee : %d OK, %d KO, statut=%s",
            cities_ok,
            cities_ko,
            status,
        )
    finally:
        conn.close()
