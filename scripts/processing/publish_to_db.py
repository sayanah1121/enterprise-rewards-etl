import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --- SMART ENVIRONMENT SETUP ---
if os.name == 'nt':
    print("[INFO] Windows Detected. Setting paths...")
    os.environ['JAVA_HOME'] = r'D:\BigData\Java'
    os.environ['HADOOP_HOME'] = r'D:\BigData\Hadoop'
    os.environ['SPARK_LOCAL_DIRS'] = r'D:\tmp'
    if r'D:\BigData\Hadoop\bin' not in os.environ['PATH']:
        os.environ['PATH'] += r';D:\BigData\Hadoop\bin'

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

# Connection to the Docker Database
# Note: 'postgres_dw' is the container name defined in docker-compose
DB_URL = "jdbc:postgresql://postgres_dw:5432/rewards_db" 
DB_PROPERTIES = {
    "user": "admin",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

def create_spark_session():
    # FIX: We list BOTH the Delta Lake driver AND the Postgres driver (separated by a comma)
    return SparkSession.builder \
        .appName("Enterprise_Rewards_Publishing") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master("local[*]") \
        .getOrCreate()
def publish_data(spark):
    print("[INFO] Publishing Gold Data to SQL Database...")

    # 1. Customer Leaderboard
    print("[PUBLISH] Writing table: customer_rewards")
    cust_df = spark.read.format("delta").load(os.path.join(GOLD_DIR, "agg_customer_rewards"))
    
    # Mode 'overwrite' drops the table and recreates it with new data
    cust_df.write.jdbc(url=DB_URL, table="customer_rewards", mode="overwrite", properties=DB_PROPERTIES)
    
    # 2. Category Stats
    print("[PUBLISH] Writing table: category_stats")
    cat_df = spark.read.format("delta").load(os.path.join(GOLD_DIR, "agg_category_stats"))
    
    cat_df.write.jdbc(url=DB_URL, table="category_stats", mode="overwrite", properties=DB_PROPERTIES)
    print("[SUCCESS] Publishing Complete.")

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Logic to switch URL if running on Windows (Local) vs Docker
    if os.name == 'nt':
        print("[INFO] Running locally on Windows. Connecting to localhost:5433...")
        DB_URL = "jdbc:postgresql://localhost:5433/rewards_db"
    
    publish_data(spark)
    spark.stop()