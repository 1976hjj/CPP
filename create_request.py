import schema
import transformation
from pyspark.sql import SparkSession
import os
import tempfile
from pyspark.sql import Row
import argparse
import glob
from pyspark.sql.functions import monotonically_increasing_id 

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Parse data interval  (default interval: 5 days)
parser = argparse.ArgumentParser()
parser.add_argument("--interval", help="Insert interval to integrate data (days)")
parser.add_argument("--timestamp", help="Insert timestamp to get requests")
args = parser.parse_args()
data_int = 10 #default value
data_time = 1776 #default value
if args.interval:
    num = args.interval
    if schema.toInt(num) is not None:
        data_int = int(num)
if args.timestamp:
    timest = args.interval
    if schema.toInt(timest) is not None:
        data_time = int(timest)


# Load a text file to RDD and convert each line to a Row.

filepath = glob.glob("./dataround_{}_days_interval/*.csv".format(data_int))
lines = sc.textFile(filepath[0])
parsed_data = lines.map(lambda l: l.split(";"))
record = parsed_data.map(lambda r: Row(timestamp=schema.toInt(r[0]), content_id=schema.toInt(r[1]), counter=schema.toInt(r[2]), timestamp_=schema.toFloat(r[3])))

# Create dataframe from RDD
data_df = spark.createDataFrame(record, schema=schema.df_rounded_schema).na.drop()


# Group data follow time interval
df_filter = data_df.filter(data_df.timestamp == data_time)\
                    .select("timestamp", "timestamp_", "content_id")\
                    .sort(data_df["timestamp_"].asc())
df_indexed = df_filter.withColumn("id", monotonically_increasing_id())
df_cache_indexed = df_indexed.withColumn("id", df_indexed["id"] % 55)

df_cache_indexed.repartition(1).write.csv("datacache_indexed_{}_days_interval".format(data_int), sep=";")
