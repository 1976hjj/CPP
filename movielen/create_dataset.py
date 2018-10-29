import schema
import transformation
from pyspark.sql import SparkSession
import os
import tempfile
from pyspark.sql import Row
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

filepath =("./data_content737_8day_derivative_2/data_content737_8day_derivative_2.txt")

# Load a text file and convert each line to a Row.
lines = sc.textFile(filepath)
parsed_data = lines.map(lambda l: l.split(";"))
record = parsed_data.map(lambda r: Row(avg_rating=float(r[0]), content_id=int(r[1]), counts=int(r[2]), d1=int(r[3]), d2=int(r[4]), timestamp=int(r[5])))
schema_record = spark.createDataFrame(record)

data_label = transformation.label(schema_record)

data_label.coalesce(1).write.csv("dataset_content737_8day_derivative_2", sep=";")

