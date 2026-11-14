"""
Module Name: iceberg_table_registration.py
Description: This custom made script helps to register iceberg tables with the metadata file generated in iceberg table.
             The script made compatibility to execute from AWS services.
"""

import sys
import argparse
from pyspark import SparkConf
from pyspark.sql import SparkSession


def register_tables(
    p_config_s3_path: str, p_catalog: str, p_s3_bucket_name: str
) -> None:

    p_warehouse = f"s3://{args['s3_bucket_name']}/{p_catalog}/"

    # Initialize SparkSession with Iceberg configurations
    conf_list = [
        (
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        ),
        (f"spark.sql.catalog.{p_catalog}", "org.apache.iceberg.spark.SparkCatalog"),
        (f"spark.sql.catalog.{p_catalog}.warehouse", p_warehouse),
        (
            f"spark.sql.catalog.{p_catalog}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        ),
        ("spark.sql.legacy.timeParserPolicy", "LEGACY"),
        ("spark.sql.iceberg.handle-timestamp-without-timezone", "True"),
        ("spark.sql.iceberg.use-timestamp-without-timezone-in-new-tables", "True"),
        (
            f"spark.sql.catalog.{p_catalog}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        ),
        (f"spark.sql.catalog.{p_catalog}.http-client.type", "apache"),
    ]

    spark_conf = SparkConf().setAll(conf_list)
    spark = (
        SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()
    )

    # Read table and metadata information from a CSV file in S3 using Spark
    tables_df = spark.read.option("header", "true").csv(p_config_s3_path)

    # Convert DataFrame to a list of dictionaries
    tables_to_register = tables_df.collect()

    # Loop through each table and register it
    for row in tables_to_register:
        db_name = row["database_name"]
        table_name = row["table_name"]
        metadata_file = row["metadata_file"]

        query = f"""
        CALL {p_catalog}.system.register_table(
        table => '{db_name}.{table_name}',
        metadata_file => '{metadata_file}'
        )
        """

        try:
            spark.sql(query)
            print(f"Table {table_name} registered successfully.")
        except Exception as e:
            print(f"Failed to register table {table_name}: {str(e)}")

    print("All tables processed.")


if __name__ == "main":
    parser = argparse.ArgumentParser(
        description="Register Iceberg tables to AWS Glue Catalog"
    )
    parser.add_argument("--p_s3_bucket_name", required=True, help="S3 bucket name")
    parser.add_argument("--p_catalog", required=True, help="Iceberg catalog name")
    parser.add_argument(
        "--p_config_s3_path",
        required=True,
        help="S3 or local path to the configuration CSV",
    )

    args = parser.parse_args()

    # Execute the utility function
    register_tables(
        p_config_s3_path=args.p_config_s3_path,
        p_catalog=args.p_catalog,
        p_s3_bucket_name=args.p_s3_bucket_name,
    )
