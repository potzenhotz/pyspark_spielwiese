from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name
from pathlib import Path
from pyspark.sql.types import StructType

spark = SparkSession.builder.appName("PySpark DataFrame Subset Example").getOrCreate()

current_file_path = Path(__file__).parent.resolve()
# df = spark.read.json(f"{current_file_path}/resources/test.json")
df_raw = (
    spark.read.option("multiline", "true")
    .option("mode", "PERMISSIVE")
    .json(f"{current_file_path}/resources/postMeasurementInfo2.json")
)

print("here 2")

df2 = (
    spark.read.option("multiline", "true")
    # .option("mode", "DROPMALFORMED")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(
        StructType.fromJson(
            {
                "type": "struct",
                "fields": [
                    {
                        "name": "DAQ",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "CALIBRATION_DATE",
                                    "type": "string",
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "DAQ_CONFIG",
                                    "type": "string",
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "FW",
                                    "type": "string",
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "RUNTIME",
                                    "type": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "METHOD",
                                                "type": "string",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                            {
                                                "name": "RUNTIME",
                                                "type": "long",
                                                "nullable": True,
                                                "metadata": {},
                                            },
                                        ],
                                    },
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "SN",
                                    "type": "long",
                                    "nullable": True,
                                    "metadata": {},
                                },
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "TIME",
                        "type": "string",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "USER_INFORMATION",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "COMMENT",
                                    "type": "string",
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "EDB_NUMBER",
                                    "type": "string",
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "TOOL_ID1",
                                    "type": "long",
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "TOOL_ID2",
                                    "type": "long",
                                    "nullable": True,
                                    "metadata": {},
                                },
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                ],
            }
        )
    )
    .json(f"{current_file_path}/resources/postMeasurementInfo2.json")
).cache()
df2.count()

print(df2.show())
# def flatten_df(
#    df: DataFrame, concat_column_names: bool = True, sep: str = "_"
# ) -> DataFrame:
#    # logger.debug(f"Using function: {sys._getframe().f_code.co_name}")
#
#    nested_cols = [c[0] for c in df.dtypes if c[1].startswith("struct")]
#
#    # Flatten struct type columns
#    while nested_cols:
#        flat_cols = [c[0] for c in df.dtypes if not c[1].startswith("struct")]
#
#        if concat_column_names:
#            flattened_cols = [
#                f.col(f"{nc}.{c}").alias(f"{nc}{sep}{c}")
#                for nc in nested_cols
#                for c in df.select(f"{nc}.*").columns
#            ]
#        else:
#            flattened_cols = [
#                f.col(f"{nc}.{c}").alias(c)
#                for nc in nested_cols
#                for c in df.select(f"{nc}.*").columns
#            ]
#
#        df = df.select(flat_cols + flattened_cols)
#
#        nested_cols = [c[0] for c in df.dtypes if c[1].startswith("struct")]
#
#    return df
