import sys
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, sha2, lit, when
from delta.tables import *

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

def create_spark_session():
    builder = SparkSession.builder \
        .appName("Enterprise_Rewards_Silver_Processing") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master("local[*]")
    
    # We use the pip configuration helper
    from delta import configure_spark_with_delta_pip
    return configure_spark_with_delta_pip(builder).getOrCreate()

def process_customers(spark):
    print("[INFO] Processing Customers (Silver)...")
    df = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "dim_customers"))
    
    # Transformation: Mask Email PII and Standardize Phone
    silver_df = df.withColumn("email_masked", sha2(col("email"), 256)) \
                  .withColumn("phone", when(col("phone").isNull(), "UNKNOWN").otherwise(col("phone"))) \
                  .drop("email") \
                  .withColumn("ingestion_timestamp", current_timestamp())
    
    save_to_silver(silver_df, "dim_customers")

def process_merchants(spark):
    print("[INFO] Processing Merchants (Silver)...")
    df = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "dim_merchants"))
    
    # Transformation: Clean Names (UpperCase)
    silver_df = df.withColumn("merchant_name", when(col("merchant_name").isNull(), "N/A").otherwise(col("merchant_name"))) \
                  .withColumn("city", col("city")) \
                  .withColumn("ingestion_timestamp", current_timestamp())
                  
    save_to_silver(silver_df, "dim_merchants")

def process_transactions(spark):
    print("[INFO] Processing Transactions (Silver)...")
    df = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "fact_transactions"))
    
    # Quality Check 1: Remove negative amounts
    # Quality Check 2: Deduplicate (Project Requirement)
    silver_df = df.filter(col("amount") > 0) \
                  .dropDuplicates(["transaction_id"]) \
                  .withColumn("ingestion_timestamp", current_timestamp())
    
    save_to_silver(silver_df, "fact_transactions", partition_col="transaction_date")

def save_to_silver(df, table_name, partition_col=None):
    output_path = os.path.join(SILVER_DIR, table_name)
    writer = df.write.format("delta").mode("overwrite")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.save(output_path)
    print(f"[SUCCESS] Saved {table_name} to Silver.")

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    process_customers(spark)
    process_merchants(spark)
    process_transactions(spark)
    
    print("\n[COMPLETE] Silver Layer Processing Finished.")
    spark.stop()