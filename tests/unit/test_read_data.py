"""Unit tests for src/read_data.py (real local files only)."""

import json

import pytest

from src import read_data
from src.exceptions import ConfigurationError, DataSourceError
from src.read_data import load_read_config, read_file, resolve_format
from tests.helpers import CONFIG_DIR, EXTRACT_DIR


# --------------------------------------------------------------------------- #
# Format resolution (no Spark needed)
# --------------------------------------------------------------------------- #
class TestResolveFormat:
    @pytest.mark.parametrize(
        "path, expected",
        [
            ("data/transform/final.parquet", "parquet"),
            ("data/transform/FINAL.PARQUET", "parquet"),
            (r"data\transform\final.parquet", "parquet"),
            ("s3a://bucket/output/final.parquet/", "parquet"),
            ("abfss://container@account.dfs.core.windows.net/in/events.json", "json"),
            ("gs://bucket/in/employees.csv", "csv"),
            ("data/in/table.orc", "orc"),
        ],
    )
    def test_format_from_extension(self, path, expected):
        assert resolve_format(path) == expected

    @pytest.mark.parametrize(
        "path",
        ["s3a://my-s3-bucket/output/employees_parquet/", "data/transform/final", "data/file.txt"],
    )
    def test_explicit_format_for_paths_without_usable_extension(self, path):
        assert resolve_format(path, "parquet") == "parquet"

    def test_explicit_format_wins_over_extension(self):
        assert resolve_format("export.parquet", "json") == "json"

    @pytest.mark.parametrize("file_format", ["PARQUET", " Json ", "Csv"])
    def test_explicit_format_is_case_insensitive(self, file_format):
        assert resolve_format("x", file_format) == file_format.strip().lower()

    @pytest.mark.parametrize(
        "path, extension",
        [
            ("s3a://my-s3-bucket/output/employees_parquet/", ""),
            ("data/file.txt", ".txt"),
            ("data/file.parquet.gz", ".gz"),
        ],
    )
    def test_no_format_and_unknown_extension_raises_with_hint(self, path, extension):
        with pytest.raises(ConfigurationError, match=f"extension '{extension}'. Set 'format'"):
            resolve_format(path)

    @pytest.mark.parametrize("file_format", ["delta", "xml", "", "   "])
    def test_unsupported_format_raises(self, file_format):
        with pytest.raises(ConfigurationError, match="unsupported format"):
            resolve_format("x.parquet", file_format)

    def test_configuration_error_is_a_value_error(self):
        with pytest.raises(ValueError):
            resolve_format("file.txt")


# --------------------------------------------------------------------------- #
# Reading real files
# --------------------------------------------------------------------------- #
@pytest.mark.spark
class TestReadFile:
    def test_reads_parquet_by_extension(self, spark):
        df = read_file(spark, str(EXTRACT_DIR / "departments.parquet"))

        assert df.columns == ["dept_no", "dept_name"]
        assert df.count() == 9

    def test_reads_employees_schema(self, spark):
        df = read_file(spark, str(EXTRACT_DIR / "employees.parquet"), {})

        assert df.dtypes == [
            ("emp_no", "int"),
            ("birth_date", "date"),
            ("first_name", "string"),
            ("last_name", "string"),
            ("gender", "string"),
            ("hire_date", "date"),
        ]

    def test_folder_without_extension_with_explicit_format(self, spark, departments_df, tmp_path):
        """Previously rejected: Spark output folders / object-store prefixes."""
        folder = tmp_path / "departments_parquet"
        departments_df.write.parquet(str(folder))

        df = read_file(spark, str(folder) + "/", file_format="parquet")

        assert sorted(df.collect()) == sorted(departments_df.collect())

    def test_path_object_and_upper_case_extension(self, spark, departments_df, tmp_path):
        path = tmp_path / "DEPARTMENTS.PARQUET"
        departments_df.write.parquet(str(path))

        assert read_file(spark, path).count() == 9

    def test_json_lines(self, spark, tmp_path):
        path = tmp_path / "employees.json"
        path.write_text("\n".join(json.dumps({"emp_no": i, "name": n}) for i, n in [(1, "A"), (2, "B")]))

        df = read_file(spark, str(path))

        assert sorted((r.emp_no, r.name) for r in df.collect()) == [(1, "A"), (2, "B")]

    def test_multiline_json_with_options_and_other_extension(self, spark, tmp_path):
        path = tmp_path / "export.txt"
        path.write_text(json.dumps([{"emp_no": 1}, {"emp_no": 2}], indent=2))

        df = read_file(spark, str(path), {"multiLine": "true"}, file_format="json")

        assert sorted(r.emp_no for r in df.collect()) == [1, 2]

    def test_csv_with_header_and_schema_inference(self, spark, tmp_path):
        path = tmp_path / "employees.csv"
        path.write_text("emp_no,first_name\n1,Alice\n2,Bob\n")

        df = read_file(spark, str(path), {"header": "true", "inferSchema": "true"})

        assert df.dtypes == [("emp_no", "int"), ("first_name", "string")]
        assert df.count() == 2

    def test_orc_round_trip(self, spark, departments_df, tmp_path):
        path = tmp_path / "departments.orc"
        departments_df.write.orc(str(path))

        assert sorted(read_file(spark, str(path)).collect()) == sorted(departments_df.collect())

    def test_missing_path_raises_data_source_error(self, spark, tmp_path):
        missing = tmp_path / "missing.parquet"

        with pytest.raises(DataSourceError, match=r"could not read '.*missing.parquet' as parquet"):
            read_file(spark, str(missing))


# --------------------------------------------------------------------------- #
# config/read_data.json
# --------------------------------------------------------------------------- #
class TestReadConfig:
    def test_project_config(self):
        config = load_read_config(str(CONFIG_DIR / "read_data.json"))

        assert config == {"path": "data/transform/final.parquet", "format": "parquet", "options": {}}

    def test_format_inferred_when_omitted(self, write_json):
        assert load_read_config(write_json({"path": "out/final.parquet"}))["format"] == "parquet"

    def test_env_reference_in_path(self, write_json, monkeypatch):
        monkeypatch.setenv("ETL_BUCKET", "prod-bucket")

        config = load_read_config(write_json({"path": "s3a://${env:ETL_BUCKET}/final/", "format": "parquet"}))

        assert config["path"] == "s3a://prod-bucket/final/"

    @pytest.mark.parametrize(
        "content, message",
        [
            ({}, "non-empty 'path'"),
            ({"path": "  "}, "non-empty 'path'"),
            ({"path": "x.parquet", "options": []}, "'options' .* must be an object"),
            ({"path": "s3a://bucket/prefix/"}, "Set 'format'"),
            ({"path": "x", "format": "delta"}, "unsupported format 'delta'"),
        ],
    )
    def test_invalid_config_raises(self, write_json, content, message):
        with pytest.raises(ConfigurationError, match=message):
            load_read_config(write_json(content))

    def test_missing_config_file(self, tmp_path):
        with pytest.raises(ConfigurationError, match="configuration file not found"):
            load_read_config(str(tmp_path / "missing.json"))


@pytest.mark.spark
class TestMain:
    def test_main_reads_configured_dataset(self, spark, departments_df, tmp_path, write_json, capsys):
        folder = tmp_path / "departments_no_extension"
        departments_df.write.parquet(str(folder))
        config = write_json({"path": str(folder), "format": "parquet"})

        df = read_data.main(config, spark=spark)

        assert df.count() == 9
        out = capsys.readouterr().out
        assert "dept_name: string" in out and "Customer Service" in out

    def test_main_with_csv_options_from_config(self, spark, tmp_path, write_json):
        csv_path = tmp_path / "people.csv"
        csv_path.write_text("id;name\n1;Ann\n")
        config = write_json({"path": str(csv_path), "options": {"header": "true", "sep": ";"}})

        df = read_data.main(config, spark=spark)

        assert [(r.id, r.name) for r in df.collect()] == [("1", "Ann")]
