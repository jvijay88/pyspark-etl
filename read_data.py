"""
Module Name: read_data.py
Description: Initializes a local Spark session to inspect, print the schema
Execute in Spark Shell :
    $env:PYSPARK_PYTHON = (py -3.11 -c "import sys; print(sys.executable)")
    $env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
    pyspark
"""

from pathlib import Path
from pyspark.sql import DataFrame
from Spark_Initializer import SparkInitializer


def read_file(spark, path, options) -> DataFrame:

    if options is None:
        options = {}

    file_path = Path(str(path))
    ext = file_path.suffix.lower()

    if ext == ".parquet":
        return spark.read.options(**options).parquet(path)
    elif ext == ".json":
        return spark.read.options(**options).json(path)
    else:
        raise ValueError(f"Unsupported file extension or type: {ext}")


spark = SparkInitializer.get_spark("read_data")

path = r"transform\final.parquet"
options = {"header": "true", "inferSchema": "true"}
df = read_file(spark, path, options)
df.printSchema()
df.show()
