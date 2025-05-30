from pyspark.sql import SparkSession

# ✅ Create SparkSession
spark = SparkSession.builder \
    .appName("Simple Test") \
    .master("local[*]") \
    .getOrCreate()

# ✅ Create test DataFrame
data = [("Apple", 10), ("Banana", 5), ("Orange", 8)]
df = spark.createDataFrame(data, ["Fruit", "Quantity"])

# ✅ Show DataFrame
df.show()

# ✅ Stop Spark
spark.stop()
