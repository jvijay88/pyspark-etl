"""
Module Name: Spark_Initializer.py
Description: Creates and caches the single SparkSession used by the pipeline.
"""

import logging

from pyspark import SparkConf
from pyspark.sql import SparkSession

from src.exceptions import SparkSessionError

logger = logging.getLogger(__name__)

DEFAULT_APP_NAME = "ETL_Pyspark"
MYSQL_CONNECTOR_JAR = "mysql-connector-java-8.0.13.jar"


class SparkInitializer:
    _spark = None

    @staticmethod
    def get_iceberg_config(catalog_name: str, s3_bucket: str) -> list:
        """Return the Spark settings needed for an Iceberg catalog backed by AWS Glue + S3."""
        p_warehouse = f"s3://{s3_bucket}/{catalog_name}/"
        return [
            (
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            ),
            (
                f"spark.sql.catalog.{catalog_name}",
                "org.apache.iceberg.spark.SparkCatalog",
            ),
            (f"spark.sql.catalog.{catalog_name}.warehouse", p_warehouse),
            (
                f"spark.sql.catalog.{catalog_name}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
            ),
            ("spark.sql.legacy.timeParserPolicy", "LEGACY"),
            ("spark.sql.iceberg.handle-timestamp-without-timezone", "True"),
            ("spark.sql.iceberg.use-timestamp-without-timezone-in-new-tables", "True"),
            (
                f"spark.sql.catalog.{catalog_name}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            ),
            (f"spark.sql.catalog.{catalog_name}.http-client.type", "apache"),
        ]

    @staticmethod
    def _missing_settings(spark: SparkSession, settings: list) -> list:
        """Return the keys from ``settings`` that are not applied to ``spark``."""
        missing = []
        for key, expected in settings:
            try:
                actual = spark.conf.get(key, None)
            except Exception:  # noqa: BLE001 - treat unreadable as not applied
                actual = None
            if actual != expected:
                missing.append(key)
        return missing

    @classmethod
    def get_spark(cls, app: str = "", iceberg_config: list = None) -> SparkSession:
        """Return the shared SparkSession, creating it on first use.

        If ``iceberg_config`` is given and the existing session was created
        without it, the session is stopped and rebuilt. Static settings such
        as ``spark.sql.extensions`` cannot be changed on a running session,
        and ``SparkSession.builder.getOrCreate()`` silently ignores them.
        """
        app_name = app.strip() if app and app.strip() else DEFAULT_APP_NAME

        existing = cls._spark or SparkSession.getActiveSession()
        if existing is not None:
            missing = cls._missing_settings(existing, iceberg_config) if iceberg_config else []
            if not missing:
                cls._spark = existing
                return cls._spark
            logger.warning(
                "The running SparkSession was created without required settings %s. "
                "Restarting it so the Iceberg configuration is applied.",
                missing,
            )
            existing.stop()
            cls._spark = None

        conf_list = [("spark.driver.extraClassPath", MYSQL_CONNECTOR_JAR)]
        if iceberg_config:
            conf_list.extend(iceberg_config)

        try:
            spark_conf = SparkConf().setAll(conf_list)
            spark = SparkSession.builder.appName(app_name).config(conf=spark_conf).getOrCreate()
        except Exception as err:
            raise SparkSessionError(
                f"get_spark: could not create SparkSession '{app_name}': {err}. "
                "Check that Java 8/11/17 is installed and JAVA_HOME is set."
            ) from err

        if iceberg_config:
            still_missing = cls._missing_settings(spark, iceberg_config)
            if still_missing:
                spark.stop()
                raise SparkSessionError(
                    f"get_spark: Iceberg settings {still_missing} were not applied to the "
                    "SparkSession. Another session may still be active in this process."
                )

        cls._spark = spark
        logger.info("SparkSession '%s' started (Spark %s).", app_name, spark.version)
        return cls._spark
