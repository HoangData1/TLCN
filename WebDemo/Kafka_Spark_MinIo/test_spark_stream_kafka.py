from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row
from delta import *
import json, os, re

from delta.tables import *

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

spark = SparkSession.builder \
    .appName("SimpleApp") \
    .master('spark://ubuntu:7077') \
    .config('spark.executor.cores', '1') \
    .config('spark.executor.instances', '1') \
    .config('spark.driver.cores', '1') \
    .config('spark.cores.max', '3') \
    .config('spark.executor.memory', '512m') \
    .config("spark.hadoop.fs.s3a.access.key", "knSN4neLpvbCAaWedDPS") \
    .config("spark.hadoop.fs.s3a.secret.key", "qQEYztGdx0PjdZVkq1GK2zZ1F6btgP3W4EQaXcrk") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


def infer_topic_schema_json(topic):
    df_json = (spark.read
               .format("kafka")
               .option("kafka.bootstrap.servers", kafka_broker)
               .option("subscribe", topic)
               .option("startingOffsets", "earliest")
               .option("endingOffsets", "latest")
               .option("failOnDataLoss", "false")
               .load()
               # filter out empty values
               .withColumn("value", expr("string(value)"))
               .filter(col("value").isNotNull())
               # get latest version of each record
               .select("key", expr("struct(offset, value) r"))
               .groupBy("key").agg(expr("max(r) r")) 
               .select("r.value"))
    
    # decode the json values
    df_read = spark.read.json(
      df_json.rdd.map(lambda x: x.value), multiLine=True)
    
    # drop corrupt records
    if "_corrupt_record" in df_read.columns:
        df_read = (df_read
                   .filter(col("_corrupt_record").isNotNull())
                   .drop("_corrupt_record"))
 
    return df_read.schema.json()



def read_stream_kafka_topic(topic, schema):
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_broker)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
            # filter out empty values
            .withColumn("value", expr("string(value)"))
            .filter(col("value").isNotNull())
            .select(
              # offset must be the first field, due to aggregation
              expr("offset as kafka_offset"),
              expr("timestamp as kafka_ts"),
              expr("string(key) as kafka_key"),
              "value"
            )
            # get latest version of each record
            .select("kafka_key", expr("struct(*) as r"))
            .groupBy("kafka_key")
            .agg(expr("max(r) r"))
            # convert to JSON with schema
            .withColumn('value', 
                        from_json(col("r.value"), topic_schema))
            .select('r.kafka_key', 
                    'r.kafka_offset', 
                    'r.kafka_ts', 
                    'value.*'
            ))




def upsertToDelta(df, batch_id): 
  df.show()
  (DeltaTable
   .forPath(spark, delta_location)
   .alias("t")
   .merge(df.alias("s"), "s.kafka_key = t.kafka_key")
   .whenMatchedUpdateAll()
   .whenNotMatchedInsertAll()
   .execute())
  

delta_location = "s3a://haha/Amazon_Bronze"
topic = "kafka.amazon.amazon_products"
kafka_broker = "localhost:9092"
topic_schema_txt = infer_topic_schema_json(topic)
topic_schema = StructType.fromJson(json.loads(topic_schema_txt))
def main():
    

    df = read_stream_kafka_topic(topic, topic_schema)
    (spark
    .createDataFrame([], df.schema)
    .write
    .option("mergeSchema", "true")
    .format("delta")
    .mode("append")
    .save(delta_location))

    w = df.writeStream

    (w.format("delta")
    .foreachBatch(upsertToDelta) 
    .outputMode("update") 
    .start(delta_location)
    .awaitTermination())

main()