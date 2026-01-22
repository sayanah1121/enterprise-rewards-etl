import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, lit

def create_spark_session():
    return SparkSession.builder \
        .appName("Vendor_Report_Generation") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def generate_report(vendor_name):
    spark = create_spark_session()
    gold_path = "/opt/airflow/data/delta/gold/agg_customer_rewards"
    output_path = f"/opt/airflow/data/exports/{vendor_name}"
    
    print(f" Generating Report for: {vendor_name}")
    
    try:
        df = spark.read.format("delta").load(gold_path)
        
        # Create a report slice
        report = df.select(
            col("customer_hash").alias("User_ID"),
            col("total_points").alias("Points_To_Credit"),
            col("total_spend").alias("Verified_Spend"),
            current_date().alias("Report_Date"),
            lit(vendor_name).alias("Partner_Name")
        ).limit(50)
        
        # Write CSV
        os.makedirs(output_path, exist_ok=True)
        report.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path)
        print(f" Report Sent: {output_path}")

    except Exception as e:
        print(f" Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        generate_report(sys.argv[1])