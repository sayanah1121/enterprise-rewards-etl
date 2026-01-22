from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from docker.types import Mount

default_args = {
    'owner': 'data_engineer',
    'start_date': days_ago(1),
    'retries': 0,
}

# The Docker network where your Postgres DB lives
NETWORK_NAME = 'enterprise-rewards-etl_default'

VENDORS = ["amazon", "paypal", "flipkart", "blackhawk"]

with DAG(
    dag_id='enterprise_rewards_butterfly',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # 1. GENERATE
    generate_task = DockerOperator(
        task_id='generate_multi_source_data',
        image='rewards-etl:v1',
        command='python /app/scripts/generate_mock_data.py',
        network_mode=NETWORK_NAME,  # <--- CHANGED
        mounts=[Mount(source="D:/enterprise-rewards-etl/data", target="/opt/airflow/data", type="bind")],
        auto_remove=True
    )

    # 2. CLEAN
    clean_task = DockerOperator(
        task_id='merge_and_clean_silver',
        image='rewards-etl:v1',
        command='python /app/scripts/processing/clean_data.py',
        network_mode=NETWORK_NAME,  # <--- CHANGED
        mounts=[Mount(source="D:/enterprise-rewards-etl/data", target="/opt/airflow/data", type="bind")],
        auto_remove=True
    )

    # 3. PARALLEL PROCESSING
    calculate_customer_task = DockerOperator(
        task_id='calculate_customer_rewards',
        image='rewards-etl:v1',
        command='python /app/scripts/processing/calculate_rewards.py',
        network_mode=NETWORK_NAME,  # <--- CHANGED
        mounts=[Mount(source="D:/enterprise-rewards-etl/data", target="/opt/airflow/data", type="bind")],
        auto_remove=True
    )

    calculate_vendor_task = DockerOperator(
        task_id='calculate_vendor_stats',
        image='rewards-etl:v1',
        command='python /app/scripts/processing/calculate_vendor_stats.py',
        network_mode=NETWORK_NAME,  # <--- CHANGED
        mounts=[Mount(source="D:/enterprise-rewards-etl/data", target="/opt/airflow/data", type="bind")],
        auto_remove=True
    )

    # 4. PUBLISH
    publish_db_task = DockerOperator(
        task_id='publish_to_postgres',
        image='rewards-etl:v1',
        command='python /app/scripts/processing/publish_gold_to_db.py',
        network_mode=NETWORK_NAME,  # <--- CHANGED
        mounts=[Mount(source="D:/enterprise-rewards-etl/data", target="/opt/airflow/data", type="bind")],
        auto_remove=True
    )

    # --- WIRING ---
    
    for vendor in VENDORS:
        ingest_task = DockerOperator(
            task_id=f'ingest_{vendor}',
            image='rewards-etl:v1',
            command=f'python /app/scripts/processing/ingest_data.py {vendor}',
            network_mode=NETWORK_NAME,  # <--- CHANGED
            mounts=[Mount(source="D:/enterprise-rewards-etl/data", target="/opt/airflow/data", type="bind")],
            auto_remove=True
        )
        generate_task >> ingest_task >> clean_task

    clean_task >> [calculate_customer_task, calculate_vendor_task]
    [calculate_customer_task, calculate_vendor_task] >> publish_db_task

    for vendor in VENDORS:
        report_task = DockerOperator(
            task_id=f'report_{vendor}',
            image='rewards-etl:v1',
            command=f'python /app/scripts/processing/generate_vendor_reports.py {vendor}',
            network_mode=NETWORK_NAME,  # <--- CHANGED
            mounts=[Mount(source="D:/enterprise-rewards-etl/data", target="/opt/airflow/data", type="bind")],
            auto_remove=True
        )
        calculate_customer_task >> report_task