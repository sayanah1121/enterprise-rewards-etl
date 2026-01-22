# Enterprise Rewards ETL Pipeline 🚀

An end-to-end Big Data pipeline that processes high-volume customer transaction data to calculate loyalty rewards.
Built with **Apache Spark**, **Delta Lake**, **PostgreSQL**, and **Docker**, orchestrated by **Apache Airflow**.

## 🏗️ Architecture (Lakehouse-to-Warehouse)
**Bronze (Raw) → Silver (Cleaned) → Gold (Aggregated) → Serving (SQL DB)**

* **Ingestion:** Python scripts ingest raw CSV data into a Delta Lake (Bronze Layer).
* **Processing:** PySpark jobs clean PII, deduplicate transactions, and validate schemas (Silver Layer).
* **Analytics:** Complex 3-way joins calculate reward points based on dynamic business rules (Gold Layer).
* **Serving:** Final aggregated metrics are published to a **PostgreSQL Data Warehouse** for BI consumption.
* **Orchestration:** Fully automated DAG managed by Apache Airflow running in Docker.

## 🛠️ Tech Stack
* **Compute:** Apache Spark 3.5.0
* **Storage:** Delta Lake 3.0.0 (ACID Transactions)
* **Warehouse:** PostgreSQL 13 (Serving Layer)
* **Orchestration:** Apache Airflow 2.7.1
* **Containerization:** Docker & Docker Compose
* **Language:** Python 3.11, SQL

## 🚀 How to Run
This project is containerized. You do not need to install Spark or Postgres manually.

### Prerequisites
* Docker Desktop (Running)

### Steps
1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/YOUR_USERNAME/enterprise-rewards-etl.git](https://github.com/YOUR_USERNAME/enterprise-rewards-etl.git)
    cd enterprise-rewards-etl
    ```

2.  **Build the Docker Image:**
    ```bash
    docker build -t rewards-etl:v1 .
    ```

3.  **Initialize Airflow & Database:**
    ```bash
    docker-compose run --rm airflow airflow db init
    docker-compose run --rm airflow airflow users create --username admin --password admin --firstname Data --lastname Engineer --role Admin --email admin@example.com
    ```

4.  **Start the Pipeline:**
    ```bash
    docker-compose up -d
    ```

5.  **Run the Job:**
    * Access Airflow UI: `http://localhost:8085` (User: `admin` / `admin`)
    * Trigger the `enterprise_rewards_etl` DAG.
    * Wait for the **publish_to_db** task to turn Green.

6.  **Check the Data (SQL Analysis):**
    Connect to the Data Warehouse using any SQL Client (DBeaver, VS Code):
    * **Host:** `localhost`
    * **Port:** `5433`
    * **Database:** `rewards_db`
    * **User:** `admin` / `password`

## 📊 Data Flow & Logic
1.  **Bronze:** Raw ingestion of `transactions`, `customers`, and `merchants` (Delta Format).
2.  **Silver:**
    * **PII Masking:** Customer emails hashed using SHA-256.
    * **Data Quality:** Removing negative transaction amounts and duplicates.
3.  **Gold:**
    * **Business Logic:** `Points = Amount * Multiplier` (Only if `Amount > Min_Spend`).
    * **Aggregations:** Generates Customer Leaderboards and Category Performance stats.
4.  **Publish (Serving):**
    * Writes Gold tables to **PostgreSQL** via JDBC.
    * Enables low-latency SQL querying for business analysts.