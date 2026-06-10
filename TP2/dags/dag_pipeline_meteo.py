import json
import os
import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


CITIES_RAW = os.environ.get("CITIES", "Paris,Lyon,Marseille")
CITIES = [c.strip() for c in CITIES_RAW.split(",")]

CITY_COORDS = {
    "Paris":     {"latitude": 48.8566, "longitude":  2.3522},
    "Lyon":      {"latitude": 45.7640, "longitude":  4.8357},
    "Marseille": {"latitude": 43.2965, "longitude":  5.3698},
    "Bordeaux":  {"latitude": 44.8378, "longitude": -0.5792},
    "Lille":     {"latitude": 50.6292, "longitude":  3.0573},
    "Nantes":    {"latitude": 47.2184, "longitude": -1.5536},
}

API_URL = os.environ.get(
    "OPEN_METEO_URL", "https://api.open-meteo.com/v1/forecast"
)
FIELDS = "temperature_2m,relative_humidity_2m,wind_speed_10m"


def get_db_conn():
    return psycopg2.connect(
        host=os.environ["DB_METEO_HOST"],
        port=int(os.environ.get("DB_METEO_PORT", 5432)),
        user=os.environ["DB_METEO_USER"],
        password=os.environ["DB_METEO_PASSWORD"],
        dbname=os.environ["DB_METEO_NAME"],
    )


def fetch_city(city_name, **context):
    coords = CITY_COORDS[city_name]
    url = (
        f"{API_URL}"
        f"?latitude={coords['latitude']}&longitude={coords['longitude']}"
        f"&current={FIELDS}&timezone=Europe/Paris"
    )
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    raw = response.json()["current"]
    print(f"[{city_name}] Reponse brute : {json.dumps(raw, indent=2)}")
    context["ti"].xcom_push(key=f"raw_{city_name}", value=raw)


def transform_city(city_name, **context):
    raw = context["ti"].xcom_pull(
        key=f"raw_{city_name}",
        task_ids=f"fetch_{city_name.lower()}",
    )
    prepared = {
        "city":          city_name,
        "timestamp":     raw["time"],
        "temperature_c": raw["temperature_2m"],
        "humidity_pct":  raw["relative_humidity_2m"],
        "wind_speed_kmh": raw["wind_speed_10m"],
    }
    print(
        f"[{city_name}] Donnees transformees : "
        f"{json.dumps(prepared, indent=2)}"
    )
    context["ti"].xcom_push(key=f"prepared_{city_name}", value=prepared)


def load_city(city_name, **context):
    data = context["ti"].xcom_pull(
        key=f"prepared_{city_name}",
        task_ids=f"transform_{city_name.lower()}",
    )
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO weather_data
                    (city, timestamp, temperature_c, humidity_pct, wind_speed_kmh)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    data["city"],
                    data["timestamp"],
                    data["temperature_c"],
                    data["humidity_pct"],
                    data["wind_speed_kmh"],
                ),
            )
        conn.commit()
        print(f"[{city_name}] Ligne inseree dans weather_data.")
    finally:
        conn.close()


def log_ingestion(**context):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_log
                    (dag_run_id, execution_date, cities_count, rows_inserted, status)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    context["run_id"],
                    context["ts"],
                    len(CITIES),
                    len(CITIES),
                    "success",
                ),
            )
            cur.execute(
                "SELECT * FROM ingestion_log ORDER BY logged_at DESC LIMIT 5"
            )
            rows = cur.fetchall()
        conn.commit()
        print("Suivi d'ingestion (5 derniers runs) :")
        header = f"{'id':<4} {'dag_run_id':<45} {'villes':>6} {'lignes':>6}"
        print(header)
        print("-" * 65)
        for row in rows:
            print(
                f"{row[0]:<4} {str(row[1]):<45} "
                f"{row[4]:>6} {row[5]:>6}"
            )
    finally:
        conn.close()


with DAG(
    dag_id="dag_pipeline_meteo",
    description="Pipeline complet : Open-Meteo -> transform -> PostgreSQL",
    start_date=datetime(2026, 6, 9),
    schedule_interval=None,
    catchup=False,
) as dag:

    load_tasks = []

    for city_name in CITIES:
        city_slug = city_name.lower()

        fetch = PythonOperator(
            task_id=f"fetch_{city_slug}",
            python_callable=fetch_city,
            op_kwargs={"city_name": city_name},
        )

        transform = PythonOperator(
            task_id=f"transform_{city_slug}",
            python_callable=transform_city,
            op_kwargs={"city_name": city_name},
        )

        load = PythonOperator(
            task_id=f"load_{city_slug}",
            python_callable=load_city,
            op_kwargs={"city_name": city_name},
        )

        fetch >> transform >> load
        load_tasks.append(load)

    log = PythonOperator(
        task_id="log_ingestion",
        python_callable=log_ingestion,
    )

    load_tasks >> log
