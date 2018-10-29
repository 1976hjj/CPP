import schema
import transformation
from pyspark.sql import SparkSession
import os
import tempfile
from pyspark.sql import Row
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

filepath =("./data/rating_subfile/ratings.txt")

# Load a text file and convert each line to a Row.
lines = sc.textFile(filepath)
parsed_data = lines.map(lambda l: l.split(","))
record = parsed_data.map(lambda r: Row(user_id=int(r[0]), content_id=int(r[1]), rating=float(r[2]), timestamp=int(r[3])))
schema_record = spark.createDataFrame(record)
schema_record.count()
df_counter = transformation.add_counter(schema_record)

#df_rounded = transformation.round_timestamp(df_counter,240)
#df_grouped = transformation.group(df_rounded)
#df_clean = transformation.clean(df_grouped)
#df_delta = transformation.delta(df_clean)
#df_filtered = transformation.filter_by_contentid(df_delta, 590)
#df_sorted = transformation.sort_df(df_filtered)
df_counter.persist()

df_grouped = transformation.group_by_contentid(df_counter)
df_filtered = transformation.filter_by_count(df_grouped, 5000)

df_joined = df_counter.join(df_filtered, df_counter.content_id == df_filtered.content_id).select(df_counter["timestamp"], df_counter["content_id"], df_counter["count"], df_counter["rating"])
#df_rounded = transformation.round_timestamp(df_joined,240)
#df_grouped_1 = transformation.group(df_rounded)
#df_clean = transformation.clean(df_grouped_1)
#df_delta = transformation.delta(df_clean)
#df_sorted = transformation.sort_df(df_delta)
#df_sorted = transformation.sort_df_by_count(df_filtered)

df_joined.coalesce(1).write.csv("data_filtered_5k.csv", sep=";", header=True)
df_counter.unpersist()

