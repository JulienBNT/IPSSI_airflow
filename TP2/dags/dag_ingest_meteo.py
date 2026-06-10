"""
Champs retenus depuis l'API Open-Meteo

temperature_2m        
relative_humidity_2m  
wind_speed_10m     
time
"""

import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


CITIES = [
    {"name": "Paris",     "latitude": 48.8566, "longitude": 2.3522},
    {"name": "Lyon",      "latitude": 45.7640, "longitude": 4.8357},
    {"name": "Marseille", "latitude": 43.2965, "longitude": 5.3698},
]

API_URL = "https://api.open-meteo.com/v1/forecast"
FIELDS  = "temperature_2m,relative_humidity_2m,wind_speed_10m"


def fetch_city(city, **context):
    url = (
        f"{API_URL}"
        f"?latitude={city['latitude']}&longitude={city['longitude']}"
        f"&current={FIELDS}"
        "&timezone=Europe/Paris"
    )
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    raw = response.json()["current"]
    print(f"[{city['name']}] Reponse brute : {json.dumps(raw, indent=2)}")
    context["ti"].xcom_push(key=f"raw_{city['name']}", value=raw)


def transform_city(city, **context):
    raw = context["ti"].xcom_pull(
        key=f"raw_{city['name']}",
        task_ids=f"fetch_{city['name'].lower()}",
    )
    prepared = {
        "city":            city["name"],
        "timestamp":       raw["time"],
        "temperature_c":   raw["temperature_2m"],
        "humidity_pct":    raw["relative_humidity_2m"],
        "wind_speed_kmh":  raw["wind_speed_10m"],
    }
    print(f"[{city['name']}] Donnees preparees : {json.dumps(prepared, indent=2)}")
    context["ti"].xcom_push(key=f"prepared_{city['name']}", value=prepared)


def collect_all(**context):
    rows = []
    for city in CITIES:
        row = context["ti"].xcom_pull(
            key=f"prepared_{city['name']}",
            task_ids=f"transform_{city['name'].lower()}",
        )
        rows.append(row)

    print(f"{'Ville':<12} {'Horodatage':<22} {'Temp (C)':>8} {'Humidite (%)':>13} {'Vent (km/h)':>12}")
    print("-" * 72)
    for row in rows:
        print(
            f"{row['city']:<12} {row['timestamp']:<22}"
            f" {row['temperature_c']:>8}"
            f" {row['humidity_pct']:>13}"
            f" {row['wind_speed_kmh']:>12}"
        )


with DAG(
    dag_id="dag_ingestion_meteo",
    description="Ingestion Open-Meteo fetch, transform, collect",
    start_date=datetime(2026, 6, 8),
    schedule_interval=None,
    catchup=False,
) as dag:

    fetch_tasks     = []
    transform_tasks = []

    for city in CITIES:
        city_lower = city["name"].lower()

        fetch = PythonOperator(
            task_id=f"fetch_{city_lower}",
            python_callable=fetch_city,
            op_kwargs={"city": city},
        )

        transform = PythonOperator(
            task_id=f"transform_{city_lower}",
            python_callable=transform_city,
            op_kwargs={"city": city},
        )

        fetch >> transform
        fetch_tasks.append(fetch)
        transform_tasks.append(transform)

    collect = PythonOperator(
        task_id="collect_all",
        python_callable=collect_all,
    )

    transform_tasks >> collect
