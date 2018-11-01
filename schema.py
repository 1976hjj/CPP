from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

data_schema = StructType(
    [
        StructField("timestamp", IntegerType(), True),
        StructField("content_id", IntegerType(), True),
        StructField("counter", IntegerType(), True),
        StructField("rating", FloatType(), True)
    ]
)

df_schema = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("content_id", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", IntegerType(), True)
    ]
)

def toInt(num):
    try:
        return int(num)
    except ValueError:
        return None
def toFloat(num):
    try:
        return float(num)
    except ValueError:
        return None