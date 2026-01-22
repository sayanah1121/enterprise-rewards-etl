import sys
import os
from pyspark.sql import SparkSession

# --- SETUP ENVIRONMENT ---
os.environ['JAVA_HOME'] = r'D:\BigData\Java'
os.environ['HADOOP_HOME'] = r'D:\BigData\Hadoop'
os.environ['SPARK_LOCAL_DIRS'] = r'D:\tmp' 
if r'D:\BigData\Hadoop\bin' not in os.environ['PATH']:
    os.environ['PATH'] += r';D:\BigData\Hadoop\bin'

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

def run_analytics():
    # Initialize Spark
    builder = SparkSession.builder \
        .appName("Enterprise_Rewards_Analytics") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master("local[*]")
    
    from delta import configure_spark_with_delta_pip
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "="*50)
    print("   ENTERPRISE REWARDS ANALYTICS DASHBOARD")
    print("="*50)

    # --- QUERY 1: TOP 5 CUSTOMERS ---
    print("\n[REPORT] Top 5 Customers by Loyalty Points:")
    cust_df = spark.read.format("delta").load(os.path.join(GOLD_DIR, "agg_customer_rewards"))
    cust_df.select("customer_id", "total_points", "total_spend", "transaction_count") \
           .show(5, truncate=False)

    # --- QUERY 2: CATEGORY PERFORMANCE ---
    print("\n[REPORT] Rewards Distributed by Category:")
    cat_df = spark.read.format("delta").load(os.path.join(GOLD_DIR, "agg_category_stats"))
    cat_df.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    run_analytics()