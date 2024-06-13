from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from pathlib import Path

spark = SparkSession.builder.appName("PySpark DataFrame Subset Example").getOrCreate()

current_file_path = Path(__file__).parent.resolve()
df = spark.read.json(f"{current_file_path}/resources/test.json")
# df = spark.read.csv(file_1)

print(df.printSchema())


def flatten_df(
    df: DataFrame, concat_column_names: bool = True, sep: str = "_"
) -> DataFrame:
    # logger.debug(f"Using function: {sys._getframe().f_code.co_name}")

    nested_cols = [c[0] for c in df.dtypes if c[1].startswith("struct")]

    # Flatten struct type columns
    while nested_cols:
        flat_cols = [c[0] for c in df.dtypes if not c[1].startswith("struct")]

        if concat_column_names:
            flattened_cols = [
                f.col(f"{nc}.{c}").alias(f"{nc}{sep}{c}")
                for nc in nested_cols
                for c in df.select(f"{nc}.*").columns
            ]
        else:
            flattened_cols = [
                f.col(f"{nc}.{c}").alias(c)
                for nc in nested_cols
                for c in df.select(f"{nc}.*").columns
            ]

        df = df.select(flat_cols + flattened_cols)

        nested_cols = [c[0] for c in df.dtypes if c[1].startswith("struct")]

    return df
