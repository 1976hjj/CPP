from pyspark.sql.functions import lit, udf, col, lag, when
from pyspark.sql.types import IntegerType
from pyspark.sql import Window

def add_counter(df):
    return df.withColumn("counter", lit(1))\
            .select("timestamp", "content_id", "counter")

def round_timestamp(df, days):
    df = df.withColumnRenamed("timestamp", "timestamp_")
    return df.withColumn("timestamp", (df["timestamp_"] - df["timestamp_"] % (3600*24*days))/(3600*24*days))\
            .select("timestamp", "content_id", "counter", "timestamp_")

def round_timestamp_minor(df, hours):
    df = df.withColumnRenamed("timestamp", "timestamp_")
    return df.withColumn("timestamp", (df["timestamp_"] - df["timestamp_"] % (3600*hours))/(3600*hours))\
            .select("timestamp", "content_id", "counter", "timestamp_")
    
def group(df):
    return df.groupBy("content_id", "timestamp")\
            .sum("counter")\
            .withColumnRenamed("sum(counter)", "counts")\
            .select("timestamp", "content_id", "counts")

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
            .select("counts", "d1", "d2", "label")

def derivative_2(df):
    window = Window.partitionBy("content_id").orderBy("timestamp")
    return df.withColumn("derivative_2", df["derivative"] - when((lag(df["derivative"], 1).over(window)).isNull(), 0).otherwise(lag(df["derivative"], 1).over(window)))

def filter_by_contentid(df, content_id):
    return df.filter(df.content_id == content_id)

def filter_by_count(df, threshold):
    return df.filter(df["count"] >= threshold)

def filter_by_timestamp(df, start, end):
    if(start > 0 & end > 0):
        return df.filter((df["timestamp"] >= start) & (df["timestamp"] <= end))
    elif(start > 0):
        return df.filter(df["timestamp"] >= start)
    elif(end > 0):
        return df.filter(df["timestamp"] <= end)
    else:
        return df

def der_lab_data(df):
    window = Window.partitionBy("content_id").orderBy("timestamp")
    df_der_1 = df.withColumn("d1", df["counts"] - when((lag(df["counts"], 1).over(window)).isNull(), 0).otherwise(lag(df["counts"], 1).over(window)))\
                .withColumn("label", when((lag(df["counts"], -1).over(window)).isNull(), 0).otherwise(lag(df["counts"], -1).over(window)))
    return df_der_1.withColumn("d2", df_der_1["d1"] - when((lag(df_der_1["d1"], 1).over(window)).isNull(), 0).otherwise(lag(df_der_1["d1"], 1).over(window)))\
                .select("timestamp","content_id","counts", "d1", "d2", "label")

def export_list_request(df):
    return df.groupBy("timestamp", "content_id")\
            .sum("counter")\
            .withColumnRenamed("sum(counter)", "counts")\
            .withColumn("new_counter", lit(1))\
            .select("timestamp", "content_id", "counts", "new_counter")\
            .sort(df["timestamp"].asc(), df["content_id"].asc())

def count_num_content_request(df):
    return df.groupBy("timestamp")\
            .sum("new_counter", "counts")\
            .withColumnRenamed("sum(new_counter)", "num_content")\
            .withColumnRenamed("sum(counts)", "num_request")\
            .select("timestamp", "num_content", "num_request")\
            .sort(df["timestamp"].asc())

def cal_popularity(df):
    df = df.groupBy("content_id")\
            .sum("counter")\
            .withColumnRenamed("sum(counter)", "counts")
    return df.select("counts", "content_id")\
            .sort(df["counts"].desc())

def count_by_sliding_window(df, window_size):
    window = Window.partitionBy("content_id").orderBy("timestamp")
    df = df.withColumn("count_by_window", df["counts"])
    for i in range(1, window_size):
        df = df.withColumn("count_by_window", df["counts"] + when((lag(df["count_by_window"], 1).over(window)).isNull(), df["counts"]).otherwise(lag(df["count_by_window"], 1).over(window)))
    df = df.withColumn("label", when((lag(df["count_by_window"], -window_size).over(window)).isNull(), 0).otherwise(lag(df["count_by_window"], -window_size).over(window)))\
                     .withColumn("d1", df["count_by_window"] - when((lag(df["count_by_window"], 1).over(window)).isNull(), 0).otherwise(lag(df["count_by_window"], 1).over(window)))
    return df.withColumn("d2", df["d1"] - when((lag(df["d1"], 1).over(window)).isNull(), 0).otherwise(lag(df["d1"], 1).over(window)))\
                .select("timestamp","content_id","counts","count_by_window","d1","d2","label")