# Enterprise Rewards ETL Pipeline 🚀

An end-to-end Big Data pipeline that processes customer transaction data to calculate loyalty rewards.
Built with **Apache Spark**, **Delta Lake**, and **Docker**, orchestrated by **Apache Airflow**.

## 🏗️ Architecture
**Bronze (Raw) → Silver (Cleaned) → Gold (Business Aggregates)**

* **Ingestion:** Python scripts ingest raw CSV data into a Delta Lake (Bronze Layer).
* **Processing:** PySpark jobs clean PII, deduplicate transactions, and validate schemas (Silver Layer).
* **Analytics:** Complex 3-way joins calculate reward points based on dynamic business rules (Gold Layer).
* **Orchestration:** Fully automated DAG managed by Apache Airflow running in Docker.

## 🛠️ Tech Stack
* **Compute:** Apache Spark 3.5.0
* **Storage:** Delta Lake 3.0.0
* **Orchestration:** Apache Airflow 2.7.1
* **Containerization:** Docker & Docker Compose
* **Language:** Python 3.11

## 🚀 How to Run
This project is containerized. You do not need to install Spark or Airflow manually.

### Prerequisites
* Docker Desktop (Running)

### Steps
1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/YOUR_USERNAME/enterprise-rewards-etl.git](https://github.com/YOUR_USERNAME/enterprise-rewards-etl.git)
    cd enterprise-rewards-etl
    ```

2.  **Initialize Airflow (First Run Only):**
    ```bash
    docker-compose run --rm airflow airflow db init
    docker-compose run --rm airflow airflow users create --username admin --password admin --firstname Data --lastname Engineer --role Admin --email admin@example.com
    ```

3.  **Start the Pipeline:**
    ```bash
    docker-compose up
    ```

4.  **Access the UI:**
    * Go to `http://localhost:8085`
    * Login: `admin` / `admin`
    * Trigger the `enterprise_rewards_etl` DAG.

## 📊 Data Flow
1.  **Bronze:** Raw ingestion of `transactions`, `customers`, and `merchants`.
2.  **Silver:**
    * Masking customer emails (PII security).
    * Removing negative transaction amounts.
    * Deduplicating records.
3.  **Gold:**
    * Joining Transactions + Merchants + Reward Rules.
    * Calculating points: `Amount * Multiplier` (if `Amount > Min_Spend`).
    * Aggregating top customers and category performance.