import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from fetch import fetch_and_archive
from quality import check
from transform import transform
from load import flag_anomaly, load_weather, write_ingestion_log

logger = logging.getLogger(__name__)

CITIES_RAW = os.environ.get("CITIES", "Paris,Lyon,Marseille")
CITIES = [c.strip() for c in CITIES_RAW.split(",")]

DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
}


def ingest_data(city_name, **context):
    raw = fetch_and_archive(city_name, context["run_id"])
    context["ti"].xcom_push(key=f"raw_{city_name}", value=raw)


def transform_data(city_name, **context):
    raw = context["ti"].xcom_pull(
        key=f"raw_{city_name}",
        task_ids=f"ingest_data_{city_name.lower()}",
    )
    prepared = transform(city_name, raw)
    context["ti"].xcom_push(key=f"prepared_{city_name}", value=prepared)


def quality_check(city_name, **context):
    prepared = context["ti"].xcom_pull(
        key=f"prepared_{city_name}",
        task_ids=f"transform_data_{city_name.lower()}",
    )
    is_valid, errors = check(city_name, prepared)
    context["ti"].xcom_push(key=f"quality_ok_{city_name}", value=is_valid)
    context["ti"].xcom_push(key=f"errors_{city_name}", value=errors)
    city_slug = city_name.lower()
    if is_valid:
        logger.info("[%s] Qualite OK -> chargement", city_name)
        return f"load_data_{city_slug}"
    logger.warning("[%s] Qualite KO -> anomalie tracee", city_name)
    return f"flag_anomaly_{city_slug}"


def load_data(city_name, **context):
    prepared = context["ti"].xcom_pull(
        key=f"prepared_{city_name}",
        task_ids=f"transform_data_{city_name.lower()}",
    )
    load_weather(city_name, prepared)


def flag_anomaly_data(city_name, **context):
    prepared = context["ti"].xcom_pull(
        key=f"prepared_{city_name}",
        task_ids=f"transform_data_{city_name.lower()}",
    )
    errors = context["ti"].xcom_pull(
        key=f"errors_{city_name}",
        task_ids=f"quality_check_{city_name.lower()}",
    )
    flag_anomaly(
        city_name,
        context["run_id"],
        prepared.get("timestamp", "unknown"),
        errors or [],
    )


def log_execution(**context):
    cities_ok = 0
    cities_ko = 0
    for city_name in CITIES:
        is_valid = context["ti"].xcom_pull(
            key=f"quality_ok_{city_name}",
            task_ids=f"quality_check_{city_name.lower()}",
        )
        if is_valid is True:
            cities_ok += 1
        else:
            cities_ko += 1
    write_ingestion_log(
        context["run_id"],
        context["ts"],
        cities_ok,
        cities_ko,
    )


with DAG(
    dag_id="pipeline_meteo",
    description=(
        "Pipeline industrialise : "
        "ingest Open-Meteo -> archive -> transform -> qualite -> PostgreSQL"
    ),
    start_date=datetime(2026, 6, 9),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["meteo", "open-meteo", "postgres"],
) as dag:

    all_branch_ends = []

    for city_name in CITIES:
        city_slug = city_name.lower()

        ingest = PythonOperator(
            task_id=f"ingest_data_{city_slug}",
            python_callable=ingest_data,
            op_kwargs={"city_name": city_name},
        )

        trans = PythonOperator(
            task_id=f"transform_data_{city_slug}",
            python_callable=transform_data,
            op_kwargs={"city_name": city_name},
        )

        quality = BranchPythonOperator(
            task_id=f"quality_check_{city_slug}",
            python_callable=quality_check,
            op_kwargs={"city_name": city_name},
        )

        load = PythonOperator(
            task_id=f"load_data_{city_slug}",
            python_callable=load_data,
            op_kwargs={"city_name": city_name},
        )

        flag = PythonOperator(
            task_id=f"flag_anomaly_{city_slug}",
            python_callable=flag_anomaly_data,
            op_kwargs={"city_name": city_name},
        )

        ingest >> trans >> quality >> [load, flag]
        all_branch_ends.extend([load, flag])

    log = PythonOperator(
        task_id="log_execution",
        python_callable=log_execution,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    all_branch_ends >> log
