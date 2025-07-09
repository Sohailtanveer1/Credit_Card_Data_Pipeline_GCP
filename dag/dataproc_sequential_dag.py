from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# Environment variables
PROJECT_ID = '' # your project id
REGION = '' # region of dataproc cluster
CLUSTER_NAME = '' # dataproc cluster name
BUCKET = 'cred_config' # configuration bucket

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='dataproc_sequential_spark_jobs',
    default_args=default_args,
    description='Run three sequential PySpark jobs on Dataproc',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dataproc', 'composer'],
) as dag:

    # PySpark Job Configs
    job1 = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET}/job_scripts/bronze.py",
                        "args": ["--config_path", f"gs://{BUCKET}/configuration_files/source_list.json"]
                        }
        
    }

    job2 = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET}/job_scripts/silver.py",
                        "args": ["--config_path", f"gs://{BUCKET}/configuration_files/validation_rules.json"]
                    }
    }

    job3 = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET}/job_scripts/gold.py"},
    }

    # Tasks
    t1 = DataprocSubmitJobOperator(
        task_id="run_task1",
        job=job1,
        region=REGION,
        project_id=PROJECT_ID,
    )

    t2 = DataprocSubmitJobOperator(
        task_id="run_task2",
        job=job2,
        region=REGION,
        project_id=PROJECT_ID,
    )

    t3 = DataprocSubmitJobOperator(
        task_id="run_task3",
        job=job3,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Task dependencies
    t1 >> t2 >> t3
