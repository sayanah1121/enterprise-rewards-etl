import sys
import os
import shutil

# --- 1. CLEAN ENVIRONMENT SETUP ---
# Prevent "Double Spark" conflicts
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
    
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from delta import *

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
TEMP_DIR = r"D:\tmp"  # Safe temp dir to prevent Windows crashes

def create_spark_session():
    print("[INFO] Initializing Spark Session...")
    
    builder = SparkSession.builder \
        .appName("Enterprise_Rewards_Ingestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.local.dir", TEMP_DIR) \
        .master("local[*]")

    return configure_spark_with_delta_pip(builder).getOrCreate()

def ingest_table(spark, table_name, schema, partition_col=None):
    input_path = os.path.join(RAW_DIR, f"{table_name}.csv")
    output_path = os.path.join(BRONZE_DIR, table_name)

    print(f"[INFO] Ingesting {table_name}...")

    try:
        df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(schema) \
            .load(input_path)
            
        writer = df.write \
            .format("delta") \
            .mode("overwrite")
        
        if partition_col:
            writer = writer.partitionBy(partition_col)
            
        writer.save(output_path)
        print(f"[SUCCESS] Wrote {table_name} to {output_path}")
        
    except Exception as e:
        print(f"[ERROR] Failed to ingest {table_name}: {e}")

def run_ingestion():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # --- DEFINE SCHEMAS ---
    schema_transactions = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("transaction_date", DateType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True)
    ])

    schema_customers = StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("registration_date", DateType(), True)
    ])

    schema_merchants = StructType([
        StructField("merchant_id", StringType(), False),
        StructField("merchant_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ])

    schema_rules = StructType([
        StructField("category", StringType(), True),
        StructField("min_spend", IntegerType(), True),
        StructField("reward_points", IntegerType(), True),
        StructField("multiplier", DoubleType(), True)
    ])

    schema_date = StructType([
        StructField("date_key", IntegerType(), False),
        StructField("full_date", DateType(), True),
        StructField("day_of_week", StringType(), True),
        StructField("month", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("quarter", IntegerType(), True),
        StructField("is_weekend", StringType(), True),
        StructField("is_holiday", StringType(), True)
    ])

    # --- EXECUTE INGESTION ---
    ingest_table(spark, "dim_customers", schema_customers)
    ingest_table(spark, "dim_merchants", schema_merchants)
    ingest_table(spark, "ref_reward_rules", schema_rules)
    ingest_table(spark, "dim_date", schema_date)
    ingest_table(spark, "fact_transactions", schema_transactions, partition_col="transaction_date")

    print("\n[COMPLETE] Bronze Layer Ingestion Finished.")
    spark.stop()

if __name__ == "__main__":
    run_ingestion()