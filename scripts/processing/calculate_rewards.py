from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when

def create_spark_session():
    return SparkSession.builder \
        .appName("Enterprise_Rewards_Calculation") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def calculate_rewards():
    spark = create_spark_session()
    
    # Input: Silver Layer (Cleaned Data)
    silver_path = "/opt/airflow/data/delta/silver/cleaned_transactions"
    # Output: Gold Layer (Aggregated Rewards)
    gold_path = "/opt/airflow/data/delta/gold/agg_customer_rewards"
    
    print(" Starting Rewards Calculation (Gold Layer)...")
    
    try:
        df = spark.read.format("delta").load(silver_path)
        
        # Business Logic:
        # 1. Electronics = 2 points per $
        # 2. Grocery = 0.5 points per $
        # 3. Everything else = 1 point per $
        df_with_points = df.withColumn("points", 
            when(col("category") == "Electronics", col("amount") * 2)
            .when(col("category") == "Grocery", col("amount") * 0.5)
            .otherwise(col("amount") * 1)
        )
        
        # Aggregation: Sum points per Customer
        customer_rewards = df_with_points.groupBy("customer_hash") \
            .agg(
                _sum("points").alias("total_points"),
                _sum("amount").alias("total_spend")
            )
            
        # Write to Gold
        customer_rewards.write.format("delta").mode("overwrite").save(gold_path)
        print(f" Gold Layer Updated: {gold_path}")
        
    except Exception as e:
        print(f" Error in Gold Layer: {e}")
        # If silver table doesn't exist yet, just exit safely
        pass

if __name__ == "__main__":
    calculate_rewards()