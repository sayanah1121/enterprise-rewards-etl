import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sha2
from functools import reduce

VENDORS = ["amazon", "paypal", "flipkart", "blackhawk"]

def create_spark_session():
    return SparkSession.builder \
        .appName("Enterprise_Rewards_Cleaning") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def clean_data():
    spark = create_spark_session()
    base_bronze_path = "/opt/airflow/data/delta/bronze"
    silver_path = "/opt/airflow/data/delta/silver/cleaned_transactions"

    print(" Merging Bronze Tables...")

    dataframes = []
    for vendor in VENDORS:
        path = os.path.join(base_bronze_path, vendor)
        try:
            df = spark.read.format("delta").load(path)
            dataframes.append(df)
        except:
            pass

    if not dataframes:
        print(" No data found!")
        return

    # UNION ALL: Combine 4 tables into 1
    full_df = reduce(DataFrame.unionAll, dataframes)

    # Standard Cleaning
    cleaned_df = full_df.filter(col("amount") > 0) \
        .dropDuplicates(["transaction_id"]) \
        .withColumn("customer_hash", sha2(col("customer_id"), 256))

    cleaned_df.write.format("delta").mode("overwrite").save(silver_path)
    print(" Silver Layer Merged & Cleaned.")

if __name__ == "__main__":
    clean_data()