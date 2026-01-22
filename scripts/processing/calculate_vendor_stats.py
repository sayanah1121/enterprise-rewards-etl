from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, countDistinct, when

def create_spark_session():
    return SparkSession.builder \
        .appName("Enterprise_Vendor_Stats") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def calculate_stats():
    spark = create_spark_session()
    
    # Input: Silver Data
    silver_path = "/opt/airflow/data/delta/silver/cleaned_transactions"
    # Output: Gold Data (Vendor Stats)
    gold_path = "/opt/airflow/data/delta/gold/agg_vendor_stats"
    
    print(" Starting Vendor Stats Calculation...")
    
    try:
        df = spark.read.format("delta").load(silver_path)
        
        # Calculate Points Logic (Duplicate logic is okay for decoupling, or move to shared lib)
        df_enriched = df.withColumn("points", 
            when(col("category") == "Electronics", col("amount") * 2)
            .when(col("category") == "Grocery", col("amount") * 0.5)
            .otherwise(col("amount") * 1)
        )
        
        # Aggregation: Group by Vendor
        vendor_stats = df_enriched.groupBy("vendor_source") \
            .agg(
                countDistinct("customer_hash").alias("unique_users"),
                _sum("amount").alias("total_revenue"),
                _sum("points").alias("total_points_issued")
            )
            
        vendor_stats.write.format("delta").mode("overwrite").save(gold_path)
        print(f" Vendor Stats Updated: {gold_path}")
        
    except Exception as e:
        print(f" Error calculating vendor stats: {e}")

if __name__ == "__main__":
    calculate_stats()