from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def extract_data():
    print("Extraction des données depuis la source")
    print("extrait: [1, 2, 3, 4, 5]")


def transform_data():
    print("Transformation des données")
    print("transform: [2, 4, 6, 8, 10]")


def load_data():
    print("Chargement des données dans la destination")
    print("Chargement terminé avec succès")


with DAG(
    dag_id="dag_etl",
    description="3 tâches: extraction, transformation, chargement",
    start_date=datetime(2026, 6, 8),
    schedule_interval=None,
    catchup=False,
) as dag:

    task_extraction = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    task_transformation = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    task_chargement = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    task_extraction >> task_transformation >> task_chargement
