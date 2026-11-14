from pathlib import Path
from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkInitializer:
    _spark = None

    @staticmethod
    def get_iceberg_config(catalog_name: str, s3_bucket: str) -> list:

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
    def get_spark(app: str = "", iceberg_config: list = None):
        if not app or not app.strip():
            app_name = "ETL_Pyspark"
        else:
            app_name = app

        if SparkInitializer._spark is None:
            try:

                conf_list = [
                    ("spark.driver.extraClassPath", "mysql-connector-java-8.0.13.jar"),
                ]

                if iceberg_config:
                    conf_list.extend(iceberg_config)

                spark_conf = SparkConf().setAll(conf_list)
                SparkInitializer._spark = (
                    SparkSession.builder.appName(app_name)
                    .config(
                        "spark.driver.extraClassPath", "mysql-connector-java-8.0.13.jar"
                    )
                    .getOrCreate()
                )

            except Exception as e:
                # Perform custom error handling, such as logging or raising custom exceptions
                error_message = "Error initializing SparkSession: {0}".format(str(e))
                raise RuntimeError(error_message)
        return SparkInitializer._spark
