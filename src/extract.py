"""
Module Name: extract.py
Description: Extracts the configured MySQL tables and stores each one as a
             snappy parquet dataset: <parquet_folder_path>/<table>.parquet

Usage:
    python -m src.extract
"""

import logging

from src.config_loader import ALLOW_PLAINTEXT_KEY, load_json_config, resolve_config
from src.exceptions import ConfigurationError, ExtractError, short_error
from src.logging_config import configure_logging
from src.Spark_Initializer import SparkInitializer

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "config/extract.json"
REQUIRED_KEYS = (
    "mysql_host",
    "mysql_port",
    "driver",
    "mysql_database",
    "mysql_user",
    "mysql_password",
    "table_names",
    "parquet_folder_path",
)


def read_extract_config(extract_json: str) -> dict:
    """Read, validate and secret-resolve the extract configuration file."""
    config = load_json_config(extract_json, "extract_tables")

    missing = [key for key in REQUIRED_KEYS if key not in config]
    if missing:
        raise ConfigurationError(
            f"extract_tables: '{extract_json}' is missing required key(s): {', '.join(missing)}."
        )
    if not isinstance(config["table_names"], list) or not config["table_names"]:
        raise ConfigurationError(
            f"extract_tables: 'table_names' in '{extract_json}' must be a non-empty list."
        )
    return resolve_config(
        config,
        source=f"extract_tables: '{extract_json}'",
        allow_plaintext=config.get(ALLOW_PLAINTEXT_KEY, False) is True,
    )


def extract_tables(extract_json: str = DEFAULT_CONFIG_PATH) -> None:
    config = read_extract_config(extract_json)

    spark = SparkInitializer.get_spark()

    mysql_url = f"jdbc:mysql://{config['mysql_host']}:{config['mysql_port']}/{config['mysql_database']}"
    properties = {
        "url": mysql_url,
        "driver": config["driver"],
        "user": config["mysql_user"],
        "password": config["mysql_password"],
    }

    for table_name in config["table_names"]:
        parquet_file_path = f"{config['parquet_folder_path']}/{table_name}.parquet"
        logger.info("Extracting table '%s' from %s", table_name, mysql_url)
        try:
            df = spark.read.format("jdbc").options(**properties).option("dbtable", table_name).load()

            df.write.mode("overwrite").format("parquet").option("compression", "snappy").save(
                parquet_file_path
            )
        except Exception as err:
            raise ExtractError(
                f"extract_tables: failed to extract table '{table_name}' from {mysql_url} "
                f"to '{parquet_file_path}': {short_error(err)}"
            ) from err
        logger.info("Table '%s' written to '%s'", table_name, parquet_file_path)


if __name__ == "__main__":
    configure_logging()
    extract_tables(DEFAULT_CONFIG_PATH)
