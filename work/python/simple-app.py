from pyspark.sql import SparkSession

app_name = "simple-app"

spark = SparkSession.builder.appName(app_name).getOrCreate()

df = spark.createDataFrame([{"id": 1, "name": "Alice"}])
df.show()

spark.stop()