# init_spark.py

import os
import findspark
from pyspark.sql import SparkSession

def start_spark(app_name="SynapsePySparkNotebook"):
    # Set environment variables (modify these if paths change)
    os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.27.6-hotspot"
    os.environ["SPARK_HOME"] = r"D:\spark\spark-3.5.6-bin-hadoop3"
    os.environ["PYSPARK_PYTHON"] = r"D:\Rajendra-Tech\PySparkProjects\.venv\Scripts\python.exe"

    # Initialize Spark path
    findspark.init()

    # Create and return SparkSession
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
    
    print(f"✅ Spark session started: {app_name}")
    return spark
