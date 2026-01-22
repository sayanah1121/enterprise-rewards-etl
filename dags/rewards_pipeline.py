from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# --- CONFIGURATION ---
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG (The Pipeline)
with DAG(
    'enterprise_rewards_etl',
    default_args=default_args,
    description='End-to-end Rewards ETL running in Docker',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # We need to map the local data folder so it persists between steps
    # HOST path (Your D: drive) -> CONTAINER path (/app/data)
    # Note: In a real server, this would be an S3 bucket or HDFS.
    
    # STEP 1: BRONZE
    t_bronze = DockerOperator(
        task_id='ingest_bronze',
        image='rewards-etl:v1',
        container_name='task_bronze',
        api_version='auto',
        auto_remove=True,
        command='python scripts/processing/ingest_to_bronze.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts=[Mount(source="D:/enterprise-rewards-etl/data", target="/app/data", type="bind")]
    )

    # STEP 2: SILVER
    t_silver = DockerOperator(
        task_id='process_silver',
        image='rewards-etl:v1',
        container_name='task_silver',
        api_version='auto',
        auto_remove=True,
        command='python scripts/processing/process_silver.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts=[Mount(source="D:/enterprise-rewards-etl/data", target="/app/data", type="bind")]
    )

    # STEP 3: GOLD
    t_gold = DockerOperator(
        task_id='process_gold',
        image='rewards-etl:v1',
        container_name='task_gold',
        api_version='auto',
        auto_remove=True,
        command='python scripts/processing/process_gold.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts=[Mount(source="D:/enterprise-rewards-etl/data", target="/app/data", type="bind")]
    )

    # Define Dependencies (The Flow)
    t_bronze >> t_silver >> t_gold