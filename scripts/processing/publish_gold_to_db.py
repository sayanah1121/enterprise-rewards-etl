from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("Enterprise_Rewards_Publishing") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def publish_data():
    spark = create_spark_session()
    
    # Input Paths
    gold_customer_path = "/opt/airflow/data/delta/gold/agg_customer_rewards"
    gold_vendor_path = "/opt/airflow/data/delta/gold/agg_vendor_stats"
    
    # DB Config (Corrected for your environment)
    db_url = "jdbc:postgresql://rewards_warehouse:5432/rewards_db"
    db_properties = {
        "user": "admin",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    
    print(" Publishing Gold Data to PostgreSQL...")
    
    try:
        # 1. Publish Customer Table
        print("Uploading Customer Rewards...")
        df_cust = spark.read.format("delta").load(gold_customer_path)
        df_cust.write.jdbc(url=db_url, table="customer_rewards", mode="overwrite", properties=db_properties)
        print(" Customer Rewards Uploaded.")
        
        # 2. Publish Vendor Stats Table (The one you need)
        print("Uploading Vendor Analytics...")
        df_vend = spark.read.format("delta").load(gold_vendor_path)
        df_vend.write.jdbc(url=db_url, table="vendor_analytics", mode="overwrite", properties=db_properties)
        print(" Vendor Analytics Uploaded.")
        
    except Exception as e:
        print(f" Error publishing to DB: {e}")

if __name__ == "__main__":
    publish_data()