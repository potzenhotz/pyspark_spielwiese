from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from pyspark.sql.types import StructType
from pathlib import Path
import json

spark = SparkSession.builder.appName("PySpark DataFrame Subset Example").getOrCreate()

current_file_path = Path(__file__).parent.resolve()
file_1 = f"{current_file_path}/resources/pdu_test.csv"

df = spark.read.option("header", True).option("inferSchema", True).csv([file_1])

schema_to_write = df.schema.json()
print(df.show(n=2))
print(df.printSchema())
print(schema_to_write)

# with open("schema.json", "w") as f:
#    f.write(schema_to_write)

# schema_path = f"{current_file_path}/resources/pdu_test_schema.json"
# with open(schema_path) as f:
#    d = json.load(f)
#    schema_df = StructType.fromJson(d)
#
# df_schema = spark.read.option("header", True).schema(schema_df).csv([file_1])
# print("####################################################################")
# print(df_schema.show(n=2))
# print(df_schema.printSchema())
