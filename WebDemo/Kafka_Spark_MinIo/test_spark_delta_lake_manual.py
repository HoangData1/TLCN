from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row
from delta import *

spark = SparkSession.builder \
    .appName("SimpleApp") \
    .master('spark://ubuntu:7077') \
    .config('spark.executor.cores', '2') \
    .config('spark.executor.instances', '1') \
    .config('spark.driver.cores', '2') \
    .config('spark.cores.max', '3') \
    .config('spark.executor.memory', '2G') \
    .config("spark.hadoop.fs.s3a.access.key", "knSN4neLpvbCAaWedDPS") \
    .config("spark.hadoop.fs.s3a.secret.key","qQEYztGdx0PjdZVkq1GK2zZ1F6btgP3W4EQaXcrk") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


df_read = spark.read.format("delta").load("s3a://haha/Amazon_Bronze")
print("Show dataframe from S3")
df_read.show()
spark.stop()

# from delta.tables import *
# deltaTable = DeltaTable.forPath(spark, 's3a://haha/person')

# # Declare the predicate by using a SQL-formatted string.
# deltaTable.update(
#   condition = "firstname = 'Michael'",
#   set = { "gender": "'F'" }
# )

# df_read = spark.read.format("delta").load("s3a://haha/person")
# print("Show dataframe from S3")
# df_read.show()

# Declare the predicate by using Spark SQL functions.
# deltaTable.update(
#   condition = col('gender') == 'M',
#   set = { 'gender': lit('Male') }
# )