import schema
import transformation
from pyspark.sql import SparkSession
import os
import tempfile
from pyspark.sql import Row
import argparse
import glob
from pyspark.sql.functions import monotonically_increasing_id, lit

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Parse data interval  (default interval: 5 days)
parser = argparse.ArgumentParser()
parser.add_argument("--interval", type=int, default=10, help="Insert interval to integrate data (days)")
parser.add_argument("--timestamp", type=int, default=1776, help="Insert timestamp to get requests")
args = parser.parse_args()
if args.interval:
    num = args.interval
    if schema.toInt(num) is not None:
        data_int = int(num)
if args.timestamp:
    timest = args.timestamp
    if schema.toInt(timest) is not None:
        data_time = int(timest)


# Load a text file to RDD and convert each line to a Row.

filepath = glob.glob("./preprocess/dataround_{}_days_interval/*.csv".format(data_int))
lines = sc.textFile(filepath[0])
parsed_data = lines.map(lambda l: l.split(";"))
record = parsed_data.map(lambda r: Row(timestamp=schema.toFloat(r[0]), content_id=schema.toInt(r[1]), counter=schema.toInt(r[2]), timestamp_=schema.toInt(r[3])))

# Create dataframe from RDD
data_df = spark.createDataFrame(record, schema=schema.df_rounded_schema).na.drop()


# Group data follow time interval
df_filter = data_df.filter(data_df["timestamp"] == data_time)\
                .select("timestamp", "content_id", "timestamp_")\
                .sort(data_df["timestamp_"].asc())
df_indexed = df_filter.withColumn("id", monotonically_increasing_id())
df_cache_indexed = df_indexed.withColumn("id", df_indexed["id"] % 55)

df_cache_indexed.repartition(1).write.csv("./preprocess/datacache_indexed_{}_days_interval".format(data_int), sep=";")


###########
filepath_list = glob.glob("./preprocess/datalist_{}_days_interval/*.csv".format(data_int))
lines_list = sc.textFile(filepath_list[0])
parsed_data_list = lines_list.map(lambda l: l.split(";"))
record_list = parsed_data_list.map(lambda r: Row(timestamp=schema.toFloat(r[0]), content_id=schema.toInt(r[1]), counter=schema.toInt(r[2]), const=schema.toInt(r[3])))

# Create dataframe from RDD
data_df_list = spark.createDataFrame(record_list, schema=schema.df_list_schema).na.drop()


# Group data follow time interval
df_filter_list = data_df_list.filter(data_df_list["timestamp"] == data_time)\
                .select("content_id", "counter")

df_filter_list.repartition(1).write.csv("./preprocess/content_list_{}_days_interval".format(data_int), sep=";")