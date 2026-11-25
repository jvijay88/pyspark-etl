"""Unit tests for utils/iceberg_table_registration.py

Only the logic that prepares and validates the registration is tested.
Registering tables in the Glue catalog (spark.sql CALL ...) is external
connectivity and is intentionally not tested.
"""

import subprocess
import sys

import pytest

from tests.helpers import PROJECT_ROOT
from utils.iceberg_table_registration import (
    IcebergRegistrationError,
    build_iceberg_conf,
    build_register_query,
    load_registration_entries,
    parse_args,
    validate_identifier,
)

SCRIPT = PROJECT_ROOT / "utils" / "iceberg_table_registration.py"


# --------------------------------------------------------------------------- #
# Spark settings
# --------------------------------------------------------------------------- #
class TestBuildIcebergConf:
    def test_catalog_and_bucket_are_used(self):
        conf = dict(build_iceberg_conf("hive_prod", "my-bucket"))

        assert conf["spark.sql.catalog.hive_prod.warehouse"] == "s3://my-bucket/hive_prod/"
        assert conf["spark.sql.catalog.hive_prod"] == "org.apache.iceberg.spark.SparkCatalog"
        assert conf["spark.sql.catalog.hive_prod.catalog-impl"] == "org.apache.iceberg.aws.glue.GlueCatalog"
        assert conf["spark.sql.catalog.hive_prod.io-impl"] == "org.apache.iceberg.aws.s3.S3FileIO"
        assert conf["spark.sql.extensions"] == (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )

    def test_bucket_parameter_is_used_not_a_global(self):
        """Bug fix: the warehouse used args['s3_bucket_name'] (a global) before."""
        first = dict(build_iceberg_conf("cat", "bucket-a"))
        second = dict(build_iceberg_conf("cat", "bucket-b"))

        assert first["spark.sql.catalog.cat.warehouse"] == "s3://bucket-a/cat/"
        assert second["spark.sql.catalog.cat.warehouse"] == "s3://bucket-b/cat/"

    def test_bucket_is_trimmed(self):
        assert dict(build_iceberg_conf("cat", " b "))["spark.sql.catalog.cat.warehouse"] == "s3://b/cat/"

    def test_no_duplicate_keys(self):
        keys = [key for key, _ in build_iceberg_conf("cat", "bucket")]

        assert len(keys) == len(set(keys))

    @pytest.mark.parametrize("bucket", ["", "  ", None])
    def test_empty_bucket_raises(self, bucket):
        with pytest.raises(IcebergRegistrationError, match="bucket name must not be empty"):
            build_iceberg_conf("cat", bucket)

    def test_invalid_catalog_raises(self):
        with pytest.raises(IcebergRegistrationError, match="Invalid catalog name"):
            build_iceberg_conf("bad-catalog", "bucket")


# --------------------------------------------------------------------------- #
# SQL building
# --------------------------------------------------------------------------- #
class TestBuildRegisterQuery:
    def test_builds_register_table_call(self):
        query = build_register_query(
            "hive_prod", "hr_db", "employees", "s3://b/hr_db/employees/metadata/v1.metadata.json"
        )

        assert query == (
            "CALL hive_prod.system.register_table("
            "table => 'hr_db.employees', "
            "metadata_file => 's3://b/hr_db/employees/metadata/v1.metadata.json')"
        )

    def test_metadata_path_is_trimmed(self):
        assert "metadata_file => 's3://b/m.json')" in build_register_query("c", "d", "t", "  s3://b/m.json ")

    @pytest.mark.parametrize(
        "catalog, db, table, kind",
        [
            ("hive prod", "db", "t", "catalog"),
            ("c", "db; DROP TABLE x", "t", "database"),
            ("c", "db", "t') --", "table"),
            ("c", "db", "1table", "table"),
            ("c", "", "t", "database"),
        ],
    )
    def test_unsafe_identifiers_are_rejected(self, catalog, db, table, kind):
        with pytest.raises(IcebergRegistrationError, match=f"Invalid {kind} name"):
            build_register_query(catalog, db, table, "s3://b/m.json")

    @pytest.mark.parametrize("metadata", ["", "   ", None])
    def test_missing_metadata_file_raises(self, metadata):
        with pytest.raises(IcebergRegistrationError, match="Missing metadata_file for table 'd.t'"):
            build_register_query("c", "d", "t", metadata)

    @pytest.mark.parametrize("metadata", ["s3://b/x'); DROP TABLE y; --", "s3://b\\x.json"])
    def test_quotes_and_backslashes_in_metadata_rejected(self, metadata):
        with pytest.raises(IcebergRegistrationError, match="quotes and backslashes are not allowed"):
            build_register_query("c", "d", "t", metadata)


@pytest.mark.parametrize("name", ["employees", "_tmp", "hr_db_2", "A"])
def test_valid_identifiers(name):
    assert validate_identifier(name, "table") == name


# --------------------------------------------------------------------------- #
# Registration CSV validation (read with local Spark)
# --------------------------------------------------------------------------- #
@pytest.mark.spark
class TestLoadRegistrationEntries:
    @pytest.fixture
    def read_csv(self, spark, tmp_path):
        def _read(text):
            path = tmp_path / "tables.csv"
            path.write_text(text)
            return spark.read.option("header", "true").csv(str(path))

        return _read

    def test_valid_csv(self, read_csv):
        df = read_csv(
            "database_name,table_name,metadata_file\n"
            "hr_db,employees,s3://b/e/v1.metadata.json\n"
            "hr_db,departments, s3://b/d/v3.metadata.json \n"
        )

        assert load_registration_entries(df) == [
            {
                "database_name": "hr_db",
                "table_name": "employees",
                "metadata_file": "s3://b/e/v1.metadata.json",
            },
            {
                "database_name": "hr_db",
                "table_name": "departments",
                "metadata_file": "s3://b/d/v3.metadata.json",
            },
        ]

    def test_extra_columns_are_ignored(self, read_csv):
        df = read_csv("owner,database_name,table_name,metadata_file\nhr,db,t,s3://m.json\n")

        assert load_registration_entries(df) == [
            {"database_name": "db", "table_name": "t", "metadata_file": "s3://m.json"}
        ]

    def test_missing_columns_raise(self, read_csv):
        df = read_csv("database_name,table\nhr_db,employees\n")

        with pytest.raises(
            IcebergRegistrationError, match=r"missing column\(s\) \['table_name', 'metadata_file'\]"
        ):
            load_registration_entries(df)

    def test_all_row_problems_reported_with_line_numbers(self, read_csv):
        df = read_csv(
            "database_name,table_name,metadata_file\n"
            "hr_db,employees,s3://m1.json\n"
            "hr_db,,s3://m2.json\n"
            "hr_db,employees,s3://m3.json\n"
            "hr_db,salaries,\n"
        )

        with pytest.raises(IcebergRegistrationError) as exc:
            load_registration_entries(df)

        message = str(exc.value)
        assert "line 3: empty ['table_name']" in message
        assert "line 4: duplicate table 'hr_db.employees'" in message
        assert "line 5: empty ['metadata_file']" in message

    def test_header_only_csv_raises(self, read_csv):
        df = read_csv("database_name,table_name,metadata_file\n")

        with pytest.raises(IcebergRegistrationError, match="contains no tables"):
            load_registration_entries(df)


# --------------------------------------------------------------------------- #
# Command line
# --------------------------------------------------------------------------- #
class TestCommandLine:
    ARGS = ["--p_s3_bucket_name", "b", "--p_catalog", "hive_prod", "--p_config_s3_path", "s3://b/t.csv"]

    def test_parse_args(self):
        args = parse_args(self.ARGS)

        assert (args.p_s3_bucket_name, args.p_catalog, args.p_config_s3_path) == (
            "b",
            "hive_prod",
            "s3://b/t.csv",
        )

    def test_aws_glue_extra_arguments_are_ignored(self):
        args = parse_args(["--JOB_NAME", "register", *self.ARGS, "--job-bookmark-option", "x"])

        assert args.p_catalog == "hive_prod"

    def test_missing_required_argument_exits(self, capsys):
        with pytest.raises(SystemExit) as exc:
            parse_args(["--p_catalog", "hive_prod"])

        assert exc.value.code == 2
        assert "--p_s3_bucket_name" in capsys.readouterr().err

    def test_script_entry_point_runs(self):
        """Bug fix: the guard was `if __name__ == "main"`, so the script never ran."""
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--help"],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=PROJECT_ROOT,
        )

        assert result.returncode == 0, result.stderr
        assert "Register Iceberg tables to AWS Glue Catalog" in result.stdout
