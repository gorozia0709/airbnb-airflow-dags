from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator


PROJECT_ID = "vocal-sight-476612-r8"
REGION = "europe-west1"
CLUSTER_NAME = "cluster-5a89"
CODE_BUCKET = "airbnb-code-bc"

PYSPARK_PATH = f"gs://{CODE_BUCKET}/pyspark-jobs"


default_args = {
    "owner": "airbnb-etl",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "airbnb_full_etl",
    default_args=default_args,
    description="Airbnb full ETL pipeline",
    schedule_interval=None,     # run manually
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["airbnb", "dataproc", "etl"],
) as dag:


    csv_to_parquet = DataprocSubmitJobOperator(
        task_id="csv_to_parquet",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{PYSPARK_PATH}/csv_to_parquet.py",
            },
        },
    )


    dim_load = DataprocSubmitJobOperator(
        task_id="dim_load",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{PYSPARK_PATH}/final_dim_load.py",
            },
        },
    )


    fact_load = DataprocSubmitJobOperator(
        task_id="fact_load",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{PYSPARK_PATH}/final_fact_load.py",
            },
        },
    )


    csv_to_parquet >> dim_load >> fact_load
