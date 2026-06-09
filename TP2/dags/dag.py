import json
import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


CITY = os.environ.get("CITY", "Paris")
LATITUDE = os.environ.get("LATITUDE", "48.8566")
LONGITUDE = os.environ.get("LONGITUDE", "2.3522")


def fetch_weather(**context):
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={LATITUDE}&longitude={LONGITUDE}"
        "&current=temperature_2m,relative_humidity_2m,wind_speed_10m"
        "&timezone=Europe/Paris"
    )
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    print(f"Donnees brutes recues pour {CITY} : {json.dumps(data['current'], indent=2)}")
    context["ti"].xcom_push(key="raw_weather", value=data["current"])


def validate_weather(**context):
    raw = context["ti"].xcom_pull(key="raw_weather", task_ids="fetch_weather")
    required_fields = ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"]
    for field in required_fields:
        if field not in raw or raw[field] is None:
            raise ValueError(f"Champ manquant ou invalide : {field}")
    print(f"Validation OK - champs presents : {required_fields}")
    context["ti"].xcom_push(key="validated_weather", value=raw)


def load_weather(**context):
    data = context["ti"].xcom_pull(key="validated_weather", task_ids="validate_weather")
    print(f"Chargement des donnees meteo pour {CITY} :")
    print(f"  Temperature  : {data['temperature_2m']} C")
    print(f"  Humidite     : {data['relative_humidity_2m']} %")
    print(f"  Vent         : {data['wind_speed_10m']} km/h")
    print("Chargement termine avec succes.")


with DAG(
    dag_id="dag_meteo",
    description="Pipeline meteo : fetch API Open-Meteo, validation, chargement",
    start_date=datetime(2026, 6, 8),
    schedule_interval=None,
    catchup=False,
) as dag:

    task_fetch = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )

    task_validate = PythonOperator(
        task_id="validate_weather",
        python_callable=validate_weather,
    )

    task_load = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather,
    )

    task_fetch >> task_validate >> task_load
