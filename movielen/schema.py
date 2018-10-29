from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

data_schema = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("content_id", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", IntegerType(), True)
    ]
)
