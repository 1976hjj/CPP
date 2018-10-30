import schema
import transformation
from pyspark.sql import SparkSession
import os
import tempfile
from pyspark.sql import Row
import argparse

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

filepath =("./data_filtered_5k/data_filtered.txt")

# Parse data interval  (default interval: 5 days)
parser = argparse.ArgumentParser()
parser.add_argument("--interval", help="Insert interval to integrate data (days)")
args = parser.parse_args()
if args.interval:
    data_int = args.interval
else:
    data_int = 5

# Load a text file to RDD and convert each line to a Row.
lines = sc.textFile(filepath)
parsed_data = lines.map(lambda l: l.split(";"))
record = parsed_data.map(lambda r: Row(timestamp=schema.toInt(r[0]), content_id=schema.toInt(r[1]), counter=schema.toInt(r[2]), rating=schema.toFloat(r[3])))

# Create dataframe from RDD
data_df = spark.createDataFrame(record, schema=schema.data_schema).na.drop()

# Round time follow the interval
df_rounded = transformation.round_timestamp(data_df,data_int)

# Group data follow time interval
df_grouped = transformation.group(df_rounded)

# Calculate the derivative level 1 and 2
#df_derivative = transformation.derivative(df_grouped)
#df_derivative_2 = transformation.derivative_2(df_derivative)

# Lable data
data_label = transformation.der_lab_data(df_grouped)
data_label.coalesce(1).write.csv("dataset_"+data_int+"_days_interval", sep=";")
