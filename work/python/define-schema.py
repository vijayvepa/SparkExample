from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("define-schema").getOrCreate()
df = spark.read.text("/opt/data/people.txt")
df.show()

# +-----------+
# |      value|
# +-----------+
# |Michael, 29|
# |   Andy, 30|
# | Justin, 19|
# +-----------+

sc = spark.sparkContext

lines = sc.textFile("/opt/data/people.txt")

parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name = p[0], age=int(p[1])))

schemaPeople = spark.createDataFrame(people)
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 and age <= 19")

teenagers.show()

#
#+------+
#|  name|
#+------+
#|Justin|
#+------+

teenNames = teenagers.rdd.map(lambda  p: "Name: " + p.name).collect()

for name in teenNames :
    print(name)

# 25/05/26 10:31:09 INFO DAGScheduler: Job 2 finished: collect at /opt/bitnami/spark/work/python/define-schema.py:21, took 1.322263 s
# Name: Justin
