import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

def create_spark_session():
    return SparkSession.builder \
        .appName("Enterprise_Rewards_Ingestion") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def ingest_data(vendor_name):
    spark = create_spark_session()
    
    # Input: Specific vendor folder
    input_path = f"/opt/airflow/data/landing/{vendor_name}/*.csv"
    # Output: Specific Bronze table for that vendor
    output_path = f"/opt/airflow/data/delta/bronze/{vendor_name}"
    
    print(f" Ingesting Data for: {vendor_name}")
    
    try:
        df = spark.read.option("header", "true").csv(input_path)
        
        # Tag the data with the source
        df_enriched = df.withColumn("ingestion_time", current_timestamp()) \
                        .withColumn("vendor_source", lit(vendor_name))

        df_enriched.write.format("delta").mode("overwrite").save(output_path)
        print(f" Written to Bronze: {output_path}")
        
    except Exception as e:
        print(f" No data found for {vendor_name}, skipping.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        ingest_data(sys.argv[1])
    else:
        print(" Error: Vendor name required")
        sys.exit(1)