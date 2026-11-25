"""
Module Name: load.py
Description: Loads the transformed parquet data (data/transform/final.parquet)
             into the target selected by the LOAD_TARGET environment variable.

Usage:
    LOAD_TARGET=postgres python -m src.load

Supported LOAD_TARGET values: mysql (default), postgres, snowflake, s3, s3_iceberg
"""

import logging
import os

from pyspark.sql import DataFrame

from src.config_loader import ALLOW_PLAINTEXT_KEY, load_json_config, resolve_config
from src.exceptions import (
    ConfigurationError,
    LoadError,
    UnsupportedLoadTargetError,
    short_error,
)
from src.logging_config import configure_logging
from src.Spark_Initializer import SparkInitializer

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "config/config.json"
DEFAULT_LOAD_TARGET = "mysql"
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"


def load_data_mysql(df: DataFrame, config: dict) -> None:
    mysql_url = f"jdbc:mysql://{config['mysql_host']}:{config['mysql_port']}/{config['mysql_database']}"
    jdbc_properties = {
        "url": mysql_url,
        "driver": config["driver"],
        "user": config["mysql_user"],
        "password": config["mysql_password"],
        "dbtable": config["target_table"],
    }

    df.write.format("jdbc").options(**jdbc_properties).mode("append").save()

    logger.info("Successfully loaded data into MySQL table: %s", config["target_table"])


def load_to_postgres(df: DataFrame, config: dict) -> None:
    pg_url = f"jdbc:postgresql://{config['pg_host']}:{config['pg_port']}/{config['pg_database']}"
    jdbc_properties = {
        "url": pg_url,
        "driver": config["driver"],
        "user": config["pg_user"],
        "password": config["pg_password"],
        "dbtable": config["target_table"],
    }

    df.write.format("jdbc").options(**jdbc_properties).mode("append").save()

    logger.info("Successfully loaded data into Postgres table: %s", config["target_table"])


def load_to_snowflake(df: DataFrame, config: dict) -> None:
    sf_options = {
        "sfURL": config["sf_url"],
        "sfUser": config["sf_user"],
        "sfPassword": config["sf_password"],
        "sfDatabase": config["sf_database"],
        "sfSchema": config["sf_schema"],
        "sfWarehouse": config["sf_warehouse"],
    }

    (
        df.write.format(SNOWFLAKE_SOURCE_NAME)
        .options(**sf_options)
        .option("dbtable", config["target_table"])
        .mode(config["write_mode"])
        .save()
    )

    logger.info("Successfully loaded data into Snowflake table: %s", config["target_table"])


def load_to_s3(df: DataFrame, config: dict) -> None:
    s3_path = config["s3_parquet_path"]

    df.write.mode(config["write_mode"]).parquet(s3_path)

    logger.info("Successfully loaded data into S3: %s", s3_path)


def load_to_s3_iceberg(df: DataFrame, config: dict) -> None:
    table_identifier = f"{config['catalog_name']}.{config['database_name']}.{config['table_name']}"

    df.write.format("iceberg").mode(config["write_mode"]).save(table_identifier)

    logger.info("Successfully loaded data into Iceberg table: %s", table_identifier)


# target name -> (config section, loader function, keys the loader needs)
LOAD_TARGETS = {
    "mysql": (
        "mysql_config",
        load_data_mysql,
        (
            "mysql_host",
            "mysql_port",
            "driver",
            "mysql_database",
            "mysql_user",
            "mysql_password",
            "target_table",
            "parquet_folder_path",
        ),
    ),
    "postgres": (
        "postgres_config",
        load_to_postgres,
        (
            "pg_host",
            "pg_port",
            "driver",
            "pg_database",
            "pg_user",
            "pg_password",
            "target_table",
            "parquet_folder_path",
        ),
    ),
    "snowflake": (
        "snowflake_config",
        load_to_snowflake,
        (
            "sf_url",
            "sf_user",
            "sf_password",
            "sf_database",
            "sf_schema",
            "sf_warehouse",
            "target_table",
            "write_mode",
            "parquet_folder_path",
        ),
    ),
    "s3": (
        "s3_config",
        load_to_s3,
        ("s3_parquet_path", "write_mode", "parquet_folder_path"),
    ),
    "s3_iceberg": (
        "iceberg_s3_config",
        load_to_s3_iceberg,
        (
            "s3_bucket_name",
            "catalog_name",
            "database_name",
            "table_name",
            "write_mode",
            "parquet_folder_path",
        ),
    ),
}


def resolve_load_target() -> str:
    """Read LOAD_TARGET (default 'mysql') and make sure it is supported."""
    raw_target = os.getenv("LOAD_TARGET", DEFAULT_LOAD_TARGET)
    target = raw_target.strip().lower()

    if target not in LOAD_TARGETS:
        supported = ", ".join(sorted(LOAD_TARGETS))
        print(f"LOAD_TARGET: {raw_target!r}")
        logger.error("LOAD_TARGET %r is not available. Supported targets: %s.", raw_target, supported)
        raise UnsupportedLoadTargetError(
            f"execute_load: LOAD_TARGET {raw_target!r} is not available. "
            f"Set LOAD_TARGET to one of: {supported}."
        )
    return target


def read_load_config(config_json: str, target: str) -> dict:
    """Read the config file and return the validated, secret-resolved section for ``target``.

    Only the selected section is resolved, so credentials of other targets do
    not need to be available.
    """
    config = load_json_config(config_json, "execute_load")

    section, _, required_keys = LOAD_TARGETS[target]
    if section not in config:
        raise ConfigurationError(
            f"execute_load: section '{section}' required for LOAD_TARGET '{target}' "
            f"is missing from '{config_json}'."
        )

    missing = [key for key in required_keys if key not in config[section]]
    if missing:
        raise ConfigurationError(
            f"execute_load: section '{section}' in '{config_json}' is missing "
            f"required key(s): {', '.join(missing)}."
        )
    return resolve_config(
        config[section],
        source=f"execute_load: '{config_json}' section '{section}'",
        allow_plaintext=config.get(ALLOW_PLAINTEXT_KEY, False) is True,
    )


def execute_load(config_json: str = DEFAULT_CONFIG_PATH) -> None:
    # Validate target and config before starting Spark (fail fast).
    target = resolve_load_target()
    target_config = read_load_config(config_json, target)
    _, loader, _ = LOAD_TARGETS[target]

    if target == "s3_iceberg":
        iceberg_conf = SparkInitializer.get_iceberg_config(
            catalog_name=target_config["catalog_name"],
            s3_bucket=target_config["s3_bucket_name"],
        )
        spark = SparkInitializer.get_spark(app="Iceberg Data Load", iceberg_config=iceberg_conf)
    else:
        spark = SparkInitializer.get_spark()

    source_path = target_config["parquet_folder_path"]
    logger.info("Loading '%s' into target '%s'.", source_path, target)

    try:
        df = spark.read.parquet(source_path)
        loader(df, target_config)
    except Exception as err:
        raise LoadError(
            f"execute_load: failed to load '{source_path}' into target '{target}': {short_error(err)}"
        ) from err


if __name__ == "__main__":
    configure_logging()
    execute_load(DEFAULT_CONFIG_PATH)
