"""
Module Name: iceberg_table_registration.py
Description: Registers existing Iceberg tables in the AWS Glue catalog from their
             metadata files. The tables to register are listed in a CSV file
             with the columns: database_name, table_name, metadata_file

             The script is self-contained (no imports from src/) so it can be
             submitted on its own to AWS Glue / EMR.

Usage:
    spark-submit utils/iceberg_table_registration.py \
        --p_s3_bucket_name my-bucket \
        --p_catalog hive_prod \
        --p_config_s3_path s3://my-bucket/config/tables.csv
"""

import argparse
import logging
import re
import sys

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

logger = logging.getLogger(__name__)

REQUIRED_CSV_COLUMNS = ("database_name", "table_name", "metadata_file")
# Catalog, database and table names are inserted into SQL, so only plain
# identifiers are accepted.
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class IcebergRegistrationError(Exception):
    """Raised when the registration input is invalid or registrations fail."""


def build_iceberg_conf(p_catalog: str, p_s3_bucket_name: str) -> list:
    """Spark settings for an Iceberg catalog stored in AWS Glue with data in S3."""
    validate_identifier(p_catalog, "catalog")
    if not p_s3_bucket_name or not p_s3_bucket_name.strip():
        raise IcebergRegistrationError("S3 bucket name must not be empty.")

    p_warehouse = f"s3://{p_s3_bucket_name.strip()}/{p_catalog}/"
    return [
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


def validate_identifier(value: str, kind: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER_PATTERN.match(value):
        raise IcebergRegistrationError(
            f"Invalid {kind} name {value!r}: use letters, digits and underscores only, "
            "starting with a letter or underscore."
        )
    return value


def build_register_query(p_catalog: str, db_name: str, table_name: str, metadata_file: str) -> str:
    """SQL that registers one Iceberg table from its metadata file."""
    validate_identifier(p_catalog, "catalog")
    validate_identifier(db_name, "database")
    validate_identifier(table_name, "table")
    if not metadata_file or not metadata_file.strip():
        raise IcebergRegistrationError(f"Missing metadata_file for table '{db_name}.{table_name}'.")
    if "'" in metadata_file or "\\" in metadata_file:
        raise IcebergRegistrationError(
            f"Invalid metadata_file for table '{db_name}.{table_name}': quotes and "
            "backslashes are not allowed."
        )

    return (
        f"CALL {p_catalog}.system.register_table("
        f"table => '{db_name}.{table_name}', "
        f"metadata_file => '{metadata_file.strip()}')"
    )


def load_registration_entries(tables_df) -> list:
    """Validate the tables CSV DataFrame and return one dict per table.

    All problems are collected and reported together so the CSV can be fixed
    in one pass.
    """
    missing_columns = [c for c in REQUIRED_CSV_COLUMNS if c not in tables_df.columns]
    if missing_columns:
        raise IcebergRegistrationError(
            f"Registration CSV is missing column(s) {missing_columns}. Found columns: {tables_df.columns}."
        )

    entries, problems, seen = [], [], set()
    rows = tables_df.select(*REQUIRED_CSV_COLUMNS).collect()
    for line_number, row in enumerate(rows, start=2):  # line 1 is the header
        values = {key: (row[key] or "").strip() for key in REQUIRED_CSV_COLUMNS}
        empty = [key for key, value in values.items() if not value]
        if empty:
            problems.append(f"line {line_number}: empty {empty}")
            continue
        full_name = f"{values['database_name']}.{values['table_name']}"
        if full_name in seen:
            problems.append(f"line {line_number}: duplicate table '{full_name}'")
            continue
        seen.add(full_name)
        entries.append(values)

    if problems:
        raise IcebergRegistrationError("Invalid registration CSV: " + "; ".join(problems) + ".")
    if not entries:
        raise IcebergRegistrationError("Registration CSV contains no tables to register.")
    return entries


def register_entries(spark, p_catalog: str, entries: list) -> dict:
    """Register each entry, continue on failure, and return {'registered': [...], 'failed': {...}}."""
    registered, failed = [], {}
    for entry in entries:
        full_name = f"{entry['database_name']}.{entry['table_name']}"
        try:
            query = build_register_query(
                p_catalog, entry["database_name"], entry["table_name"], entry["metadata_file"]
            )
            spark.sql(query)
            registered.append(full_name)
            logger.info("Table %s registered successfully.", full_name)
        except Exception as err:  # noqa: BLE001 - keep going, report at the end
            failed[full_name] = str(err).strip().splitlines()[0] if str(err).strip() else repr(err)
            logger.error("Failed to register table %s: %s", full_name, failed[full_name])
    return {"registered": registered, "failed": failed}


def register_tables(p_config_s3_path: str, p_catalog: str, p_s3_bucket_name: str) -> dict:
    """Create an Iceberg-enabled SparkSession and register every table in the CSV."""
    from pyspark import SparkConf
    from pyspark.sql import SparkSession

    spark_conf = SparkConf().setAll(build_iceberg_conf(p_catalog, p_s3_bucket_name))
    spark = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()

    tables_df = spark.read.option("header", "true").csv(p_config_s3_path)
    entries = load_registration_entries(tables_df)
    result = register_entries(spark, p_catalog, entries)

    logger.info(
        "Processed %d table(s): %d registered, %d failed.",
        len(entries),
        len(result["registered"]),
        len(result["failed"]),
    )
    if result["failed"]:
        raise IcebergRegistrationError(
            f"{len(result['failed'])} of {len(entries)} table(s) failed to register: "
            f"{sorted(result['failed'])}."
        )
    return result


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register Iceberg tables to AWS Glue Catalog")
    parser.add_argument("--p_s3_bucket_name", required=True, help="S3 bucket name")
    parser.add_argument("--p_catalog", required=True, help="Iceberg catalog name")
    parser.add_argument(
        "--p_config_s3_path",
        required=True,
        help="S3 or local path to the configuration CSV",
    )
    # AWS Glue adds its own arguments (e.g. --JOB_NAME); ignore unknown ones.
    args, _ = parser.parse_known_args(argv)
    return args


def main(argv=None) -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    args = parse_args(argv)
    register_tables(
        p_config_s3_path=args.p_config_s3_path,
        p_catalog=args.p_catalog,
        p_s3_bucket_name=args.p_s3_bucket_name,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
