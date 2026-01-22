import sys
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, when, round, desc, current_timestamp
from delta.tables import *

# --- 1. SETUP ENVIRONMENT ---
# --- SMART ENVIRONMENT SETUP (Works on Windows AND Linux/Docker) ---
if os.name == 'nt':  # 'nt' means Windows
    print("[INFO] Detected Windows Environment. Setting manual paths...")
    os.environ['JAVA_HOME'] = r'D:\BigData\Java'
    os.environ['HADOOP_HOME'] = r'D:\BigData\Hadoop'
    os.environ['SPARK_LOCAL_DIRS'] = r'D:\tmp'
    if r'D:\BigData\Hadoop\bin' not in os.environ['PATH']:
        os.environ['PATH'] += r';D:\BigData\Hadoop\bin'
else:
    print("[INFO] Detected Linux/Docker Environment. Using system paths...")

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

def create_spark_session():
    builder = SparkSession.builder \
        .appName("Enterprise_Rewards_Gold_Processing") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.local.dir", r"D:\tmp") \
        .master("local[*]")
    
    from delta import configure_spark_with_delta_pip
    return configure_spark_with_delta_pip(builder).getOrCreate()

def calculate_rewards(spark):
    print("[INFO] Calculating Rewards (Gold)...")
    
    # 1. Load Data & DROP DUPLICATE TIMESTAMPS from Silver
    # We drop 'ingestion_timestamp' here to prevent the join error
    trans_df = spark.read.format("delta").load(os.path.join(SILVER_DIR, "fact_transactions")) \
        .drop("ingestion_timestamp")
        
    merch_df = spark.read.format("delta").load(os.path.join(SILVER_DIR, "dim_merchants")) \
        .drop("ingestion_timestamp")
        
    rules_df = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "ref_reward_rules"))

    # 2. Join Transactions -> Merchants
    trans_merch_df = trans_df.join(merch_df, "merchant_id", "left")

    # 3. Join with Rules
    full_df = trans_merch_df.join(rules_df, "category", "left")

    # 4. Apply Business Logic
    gold_df = full_df.withColumn("points_earned", 
        when(col("amount") >= col("min_spend"), round(col("amount") * col("multiplier"), 2))
        .otherwise(0)
    ).withColumn("processed_date", current_timestamp())

    # 5. Save
    gold_df.write.format("delta").mode("overwrite").partitionBy("transaction_date").save(os.path.join(GOLD_DIR, "fact_rewards_enriched"))
    print("[SUCCESS] Saved fact_rewards_enriched")

    return gold_df

def create_aggregations(spark, gold_df):
    print("[INFO] Creating Business Aggregates (Gold)...")

    # Aggregation 1: Customer Leaderboard
    cust_agg = gold_df.groupBy("customer_id") \
        .agg(
            sum("points_earned").alias("total_points"),
            sum("amount").alias("total_spend"),
            count("transaction_id").alias("transaction_count")
        ) \
        .orderBy(desc("total_points"))
    
    cust_agg.write.format("delta").mode("overwrite").save(os.path.join(GOLD_DIR, "agg_customer_rewards"))
    print("[SUCCESS] Saved agg_customer_rewards")

    # Aggregation 2: Category Performance
    cat_agg = gold_df.groupBy("category") \
        .agg(
            sum("points_earned").alias("total_points_distributed"),
            avg("amount").alias("avg_transaction_value")
        ) \
        .orderBy(desc("total_points_distributed"))

    cat_agg.write.format("delta").mode("overwrite").save(os.path.join(GOLD_DIR, "agg_category_stats"))
    print("[SUCCESS] Saved agg_category_stats")

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    enriched_df = calculate_rewards(spark)
    create_aggregations(spark, enriched_df)
    
    print("\n[COMPLETE] Gold Layer Processing Finished.")
    spark.stop()