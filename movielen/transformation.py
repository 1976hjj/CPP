from pyspark.sql.functions import lit, udf, col, lag, when
from pyspark.sql.types import IntegerType
from pyspark.sql import Window

def add_counter(df):
    return df.withColumn("count", lit(1))


def round_timestamp(df, hours):
    def rounded(num):
        try:
            res = num / (3600*hours)
        except TypeError as e:
            res = num
        return res
    my_udf = udf(lambda x: rounded(x), IntegerType())
    return df.withColumn("rounded_timestamp", my_udf(df["timestamp"]))\
            .drop("timestamp")\
            .withColumnRenamed("rounded_timestamp", "timestamp")

def group(df):
    return df.groupBy("content_id", "timestamp")\
            .sum("counter", "rating")\
            .withColumnRenamed("sum(counter)", "counts")\
            .withColumn("avg_rating", col("sum(rating)")/col("counts"))\
            .select("timestamp", "content_id", "counts", "avg_rating")

def clean(df):
    return df.drop("timestamp")\
            .drop("count")\
            .withColumnRenamed("rounded_timestamp", "timestamp")\
            .withColumnRenamed("sum(count)", "count")\
            .select("timestamp", "content_id", "count", "avg_rating")


def sort_df(df):
    return df.sort(df["content_id"].asc(),df["timestamp"].asc())

def group_by_contentid(df):
    return df.groupBy("content_id")\
            .sum("count", "rating")\
            .withColumn("avg_rating", col("sum(rating)")/col("sum(count)"))\
            .drop("count")\
            .withColumnRenamed("sum(count)", "count")\
            .select("content_id", "count", "avg_rating")

def sort_df_by_count(df):
    return df.sort(df["count"].desc())

def derivative(df):
    window = Window.partitionBy("content_id").orderBy("timestamp")
    return df.withColumn("derivative", df["counts"] - when((lag(df["counts"], 1).over(window)).isNull(), 0).otherwise(lag(df["counts"], 1).over(window)))

def label(df):
    window = Window.partitionBy("content_id").orderBy("timestamp")
    return df.withColumn("label", when((lag(df["counts"], -1).over(window)).isNull(), 0).otherwise(lag(df["counts"], -1).over(window)))\
            .select("counts", "d1", "d2", "label")\

def derivative_2(df):
    window = Window.partitionBy("content_id").orderBy("timestamp")
    return df.withColumn("derivative_2", df["derivative"] - when((lag(df["derivative"], 1).over(window)).isNull(), 0).otherwise(lag(df["derivative"], 1).over(window)))

def filter_by_contentid(df, content_id):
    return df.filter(df.content_id == content_id)

def filter_by_count(df, threshold):
    return df.filter(df["count"] >= threshold)