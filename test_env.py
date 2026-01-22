import os
import sys

print("--- ENVIRONMENT CHECK ---")
print(f"Python Version: {sys.version}")
print(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")
print(f"HADOOP_HOME: {os.environ.get('HADOOP_HOME')}")
print(f"SPARK_HOME: {os.environ.get('SPARK_HOME')}")

print("\n--- LIBRARY CHECK ---")
try:
    import pyspark
    print(f"PySpark Version: {pyspark.__version__}")
    from delta import configure_spark_with_delta_pip
    print("Delta Lake: INSTALLED")
except ImportError as e:
    print(f"FAIL: {e}")
    sys.exit(1)

print("\n--- SPARK EXECUTION TEST ---")
try:
    from pyspark.sql import SparkSession
    builder = SparkSession.builder.master("local").appName("Test")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    print("SUCCESS: Spark Session Created!")
    spark.stop()
except Exception as e:
    print(f"FAIL: Spark Crash -> {e}")