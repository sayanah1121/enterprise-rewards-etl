# Enterprise Rewards Lakehouse: The "Butterfly" Architecture 🦋

A high-scale **Data Lakehouse** solution that consolidates transaction data from multiple external vendors, processes complex loyalty logic in parallel, and distributes financial settlement reports.

## 🏗️ Architectural Patterns
This project implements the **"Fan-In / Fan-Out"** and **"Diamond Dependency"** patterns to handle scale and complexity.

**1. Ingestion (Fan-In):**
* Parallel ingestion of raw CSV data from 4 distinct vendors: **Amazon, PayPal, Flipkart, Blackhawk**.
* Orchestrated via Dynamic Task Mapping in **Apache Airflow**.

**2. Processing (The Diamond):**
* **Bronze:** Raw data ingestion with source tagging.
* **Silver:** Merging disparate sources, deduplication, and PII masking (SHA-256).
* **Gold (Parallel Streams):** Business logic is decoupled into two parallel Spark jobs:
    * **Customer 360:** Aggregates points and spend per user for the App.
    * **Vendor Analytics:** Aggregates revenue and liability per partner for Finance.

**3. Distribution (Fan-Out):**
* **Serving Layer:** Publishes Gold tables to **PostgreSQL** for real-time dashboards.
* **Reverse ETL:** Generates and sends distinct **Settlement Reports (CSV)** back to each vendor.

## 🛠️ Tech Stack
* **Orchestration:** Apache Airflow 2.7 (DockerOperator)
* **Processing:** Apache Spark 3.5 & Delta Lake 3.0
* **Storage:** Local Data Lake (Delta Format)
* **Serving:** PostgreSQL 13
* **Containerization:** Docker & Docker Compose
* **Language:** Python 3.11, SQL

## 🚀 How to Run
1.  **Start Infrastructure:** `docker-compose up -d`
2.  **Build Image:** `docker build -t rewards-etl:v1 .`
3.  **Trigger DAG:** Run `enterprise_rewards_butterfly` in Airflow.
4.  **Verify Outputs:**
    * **Database:** Query `vendor_analytics` table in Postgres.
    * **Reports:** Check `data/exports/` for generated settlement files.