import schema
import transformation
from pyspark.sql import SparkSession
import os
import tempfile
from pyspark.sql import Row
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

filepath =("./data_8day_derivative_2/data_8day_derivative_2.txt")

# Load a text file and convert each line to a Row.
lines = sc.textFile(filepath)
parsed_data = lines.map(lambda l: l.split(";"))
record = parsed_data.map(lambda r: Row(timestamp=int(r[0]), content_id=int(r[1]), counts=int(r[2]), avg_rating=float(r[3]), d1=int(r[4]), d2=int(r[5])))
schema_record = spark.createDataFrame(record)

df_filtered = transformation.filter_by_contentid(schema_record, 737)
df_sorted = transformation.sort_df(df_filtered)

df_sorted.coalesce(1).write.csv("data_content737_8day_derivative_2", sep=";")

