from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Initialize SparkSession
spark = SparkSession.builder.appName("PySpark DataFrame Subset Example").getOrCreate()

schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
    ]
)
schema_smaller = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]
)


data_smaller = [
    (1, "Alice", 30),
    (2, "Bob", 25),
    (3, "Charlie", 35),
    (4, "David", 40),
]

df_smaller = spark.createDataFrame(data_smaller, schema_smaller)
empty_df = spark.createDataFrame([], schema)

df_smaller.show()
