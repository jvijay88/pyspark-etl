"""
Module Name: read_data.py
Description: Reads a dataset (parquet, json, csv, orc) and prints its schema and
             a preview. What to read is configured in config/read_data.json:

    {
      "path": "data/transform/final.parquet",   # local path or s3a:// abfss:// gs:// URI
      "format": "parquet",                      # optional when the path has an extension
      "options": {}                             # optional Spark reader options
    }

    String values may contain references such as ${env:ETL_BUCKET}
    (see src/config_loader.py).

Usage:
    python -m src.read_data

Execute in Spark Shell (Windows PowerShell):
    $env:PYSPARK_PYTHON = (py -3.11 -c "import sys; print(sys.executable)")
    $env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
    pyspark
"""

from pathlib import PurePosixPath

from pyspark.sql import DataFrame

from src.config_loader import load_json_config, resolve_config
from src.exceptions import ConfigurationError, DataSourceError, short_error
from src.logging_config import configure_logging
from src.Spark_Initializer import SparkInitializer

DEFAULT_CONFIG_PATH = "config/read_data.json"
SUPPORTED_FORMATS = ("parquet", "json", "csv", "orc")
FORMAT_BY_EXTENSION = {f".{file_format}": file_format for file_format in SUPPORTED_FORMATS}


def resolve_format(path, file_format=None) -> str:
    """Return the reader format: explicit ``file_format`` first, else the path extension."""
    if file_format is not None:
        normalized = file_format.strip().lower() if isinstance(file_format, str) else file_format
        if normalized not in SUPPORTED_FORMATS:
            raise ConfigurationError(
                f"read_file: unsupported format {file_format!r}. Supported: {', '.join(SUPPORTED_FORMATS)}."
            )
        return normalized

    # Works for local paths (either separator) and URIs such as s3a://bucket/x.parquet
    name = PurePosixPath(str(path).replace("\\", "/").rstrip("/")).name
    extension = PurePosixPath(name).suffix.lower()
    if extension in FORMAT_BY_EXTENSION:
        return FORMAT_BY_EXTENSION[extension]
    raise ConfigurationError(
        f"read_file: cannot infer the format of '{path}' from its extension '{extension}'. "
        f"Set 'format' to one of: {', '.join(SUPPORTED_FORMATS)}."
    )


def read_file(spark, path, options=None, file_format=None) -> DataFrame:
    """Read ``path`` as ``file_format`` (or the format implied by its extension).

    Folders and object-store prefixes without an extension
    (e.g. s3a://bucket/output/employees_parquet/) need ``file_format``.
    """
    reader_format = resolve_format(path, file_format)
    try:
        return spark.read.format(reader_format).options(**(options or {})).load(str(path))
    except Exception as err:
        raise DataSourceError(
            f"read_file: could not read '{path}' as {reader_format}: {short_error(err)}"
        ) from err


def load_read_config(config_path: str) -> dict:
    """Read and validate config/read_data.json."""
    config = resolve_config(load_json_config(config_path, "read_data"), source=f"read_data: '{config_path}'")
    if not isinstance(config.get("path"), str) or not config["path"].strip():
        raise ConfigurationError(f"read_data: '{config_path}' must define a non-empty 'path'.")
    if not isinstance(config.get("options", {}), dict):
        raise ConfigurationError(f"read_data: 'options' in '{config_path}' must be an object.")
    # Fail early on an unknown format or an extension that cannot be inferred.
    config["format"] = resolve_format(config["path"], config.get("format"))
    return config


def main(config_path: str = DEFAULT_CONFIG_PATH, spark=None) -> DataFrame:
    config = load_read_config(config_path)
    spark = spark or SparkInitializer.get_spark("read_data")
    df = read_file(spark, config["path"], config.get("options"), config["format"])
    df.printSchema()
    df.show()
    return df


if __name__ == "__main__":
    configure_logging()
    main()
