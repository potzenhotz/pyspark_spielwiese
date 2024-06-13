from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from pathlib import Path

spark = SparkSession.builder.appName("PySpark DataFrame Subset Example").getOrCreate()

current_file_path = Path(__file__).parent.resolve()
file_1 = f"{current_file_path}/resources/file1.csv"
file_2 = f"{current_file_path}/resources/file2.csv"

df = spark.read.csv([file_1, file_2])
# df = spark.read.csv(file_1)
df = df.withColumn("test", input_file_name())

print(df.show(truncate=False))
