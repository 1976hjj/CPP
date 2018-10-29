import schema
import transformation
from pyspark.sql import SparkSession
import os
import tempfile
from pyspark.sql import Row
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

filepath =("./data_filtered_5k/data_filtered.txt")

# Load a text file and convert each line to a Row.
lines = sc.textFile(filepath)
parsed_data = lines.map(lambda l: l.split(";"))
record = parsed_data.map(lambda r: Row(timestamp=int(r[0]), content_id=int(r[1]), counter=int(r[2]), rating=float(r[3])))
schema_record = spark.createDataFrame(record)

df_rounded = transformation.round_timestamp(schema_record,24)
df_grouped = transformation.group(df_rounded)
df_derivative = transformation.derivative(df_grouped)
df_derivative_2 = transformation.derivative_2(df_derivative)
#df_filtered = transformation.filter_by_contentid(df_delta, 590)
df_sorted = transformation.sort_df(df_derivative_2)

#df_rounded = transformation.round_timestamp(df_joined,240)
#df_grouped_1 = transformation.group(df_rounded)
#df_clean = transformation.clean(df_grouped_1)
#df_delta = transformation.delta(df_clean)
#df_sorted = transformation.sort_df(df_delta)
#df_sorted = transformation.sort_df_by_count(df_filtered)

df_sorted.repartition(1344, df_sorted["content_id"]).write.csv("data_partitioned_1day_derivative_2", sep=";", header=True)

