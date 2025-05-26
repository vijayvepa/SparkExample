from pyspark.sql import SparkSession

app_name = "logs-app"

spark = SparkSession.builder.appName(app_name).getOrCreate()

df = spark.read.csv("/opt/data/people.csv")
df.show()
df.printSchema()
spark.stop()
