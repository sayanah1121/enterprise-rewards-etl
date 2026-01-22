from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'enterprise_rewards_etl',
    default_args=default_args,
    description='End-to-end Rewards ETL running in Docker',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # 1. BRONZE
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

    # 2. SILVER
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

    # 3. GOLD
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

    # 4. PUBLISH (New Step)
    # Important: Network mode must match the docker-compose network name
    t_publish = DockerOperator(
        task_id='publish_to_db',
        image='rewards-etl:v1',
        container_name='task_publish',
        api_version='auto',
        auto_remove=True,
        command='python scripts/processing/publish_to_db.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='enterprise-rewards-etl_default',
        mounts=[Mount(source="D:/enterprise-rewards-etl/data", target="/app/data", type="bind")]
    )

    t_bronze >> t_silver >> t_gold >> t_publish