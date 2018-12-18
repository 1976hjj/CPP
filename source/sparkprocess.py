import schema
import transformation
from pyspark.sql import SparkSession
import os
import tempfile
from pyspark.sql import Row
import argparse
from pyspark.sql.functions import lit

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

filepath =("./data/ratings.csv")

# Parse data interval  (default interval: 10 days)
parser = argparse.ArgumentParser()
parser.add_argument("--interval", type=int, default=10, help="Insert interval to integrate data (days)")
parser.add_argument("--minor", type=int, default=12, help="Set minor")
args = parser.parse_args()
if args.interval:
    num = args.interval
    if schema.toInt(num) is not None:
        data_int = int(num)
if args.minor:
    minor = args.minor
    if schema.toInt(minor) is not None:
        data_minor = int(minor)

# Load a text file to RDD and convert each line to a Row.
lines = sc.textFile(filepath)
parsed_data = lines.map(lambda l: l.split(","))
record = parsed_data.map(lambda r: Row(user_id=schema.toInt(r[0]), content_id=schema.toInt(r[1]), rating=schema.toFloat(r[2]), timestamp=schema.toInt(r[3])))

# Create dataframe from RDD
data_df = spark.createDataFrame(record, schema=schema.df_schema).na.drop()
data_df = transformation.add_counter(data_df)

# # Round time follow the interval
# df_rounded = transformation.round_timestamp(data_df, data_int)
# df_rounded.persist()

# # Group data follow time interval
# df_content_popularity = transformation.cal_popularity(df_rounded)
# df_grouped = transformation.group(df_rounded)
# df_list_request = transformation.export_list_request(df_rounded)
# df_count = transformation.count_num_content_request(df_list_request)

# # Lable data
# data_label = transformation.der_lab_data(df_grouped)
# data_label.repartition(1).write.csv("./preprocess/dataset_{}_days_interval".format(data_int), sep=";", header=False)
# df_count.repartition(1).write.csv("./preprocess/datacount_{}_days_interval".format(data_int), sep=";", header=False)
# df_list_request.repartition(1).write.csv("./preprocess/datalist_{}_days_interval".format(data_int), sep=";", header=False)
# df_rounded.repartition(1).write.csv("./preprocess/dataround_{}_days_interval".format(data_int), sep=";", header=False)
# #df_content_popularity.repartition(1).write.csv("./preprocess/content_popularity_all", sep=";", header=False)
# df_rounded.unpersist()

###################################################################################
# New prediction data
###################################################################################
df_rounded = transformation.round_timestamp_minor(data_df, data_minor)
df_rounded.persist()

df_grouped = transformation.group(df_rounded)

data_label = transformation.count_by_sliding_window(df_grouped, data_int*24/data_minor)
data_label.repartition(1).write.csv("./preprocess/newdataset_{}_days_interval".format(data_int), sep=";", header=False)


