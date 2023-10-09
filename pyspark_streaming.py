#when launching pypark use:
#./spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.hudi:hudi-spark3-bundle_2.12:0.12.3 --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" --conf "spark.sql.hive.convertMetastoreParquet=false" /home/ec2-user/final_project/pyspark_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# NOTE: This variable needs to be reviewed if we are working with a new MSK
#BOOTSTRAP_SERVERS='b-1.finalproject.m5cy8k.c3.kafka.ca-central-1.amazonaws.com:9092,b-3.finalproject.m5cy8k.c3.kafka.ca-central-1.amazonaws.com:9092,b-2.finalproject.m5cy8k.c3.kafka.ca-central-1.amazonaws.com:9092'
BOOTSTRAP_SERVERS='localhost:9092'
if __name__ == "__main__":
   spark = SparkSession.builder.getOrCreate()

   # NOTE: we cant load the schema file from the local machine anymore, so we have to pull it from s3
   schema = spark.read.json('s3a://final-project-wcd0/artifacts/bus_status_schema.json').schema

 # We have to connect to the bootstrap servers, instead of kafka:9092
   df = spark \
       .readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
       .option("subscribe", "dbserver1.final.bus_status") \
       .option("startingOffsets", "latest") \
       .load()

   transform_df = df.select(col("value").cast("string")).alias("value").withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.payload.after.*")

   # NOTE: We cannot checkpoint to a local machine because we are working on the cloud. S3 is a reliable location for the cluster
   checkpoint_location = "s3a://final-project-wcd0/checkpoints"

   table_name = 'bus_status'
   hudi_options = {
       'hoodie.table.name': table_name,
       "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
       'hoodie.datasource.write.recordkey.field': 'record_id',
       'hoodie.datasource.write.partitionpath.field': 'routeId',
       'hoodie.datasource.write.table.name': table_name,
       'hoodie.datasource.write.operation': 'upsert',
       'hoodie.datasource.write.precombine.field': 'event_time',
       'hoodie.upsert.shuffle.parallelism': 100,
       'hoodie.insert.shuffle.parallelism': 100
   }

   s3_path = "s3a://final-project-wcd0/output"

   def write_batch(batch_df, batch_id):
       batch_df.write.format("org.apache.hudi") \
       .options(**hudi_options) \
       .mode("append") \
       .save(s3_path)

   transform_df.writeStream.option("checkpointLocation", checkpoint_location).queryName("wcd-bus-streaming").foreachBatch(write_batch).start().awaitTermination()