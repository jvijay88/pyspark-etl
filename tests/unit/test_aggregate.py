"""Unit tests for src/aggregate.py

The aggregations run on ``final_df``: the final.parquet written by the
pipeline for the real sample extract (13 rows, 12 employees; emp_no 10010
has two department rows). Edge cases use small mock DataFrames.
"""

import datetime as dt
import json
import re
from collections import Counter

import pytest
from pyspark.sql import functions as F

from src import aggregate
from src.aggregate import (
    RUN_METADATA_COLUMNS,
    JobRunContext,
    add_run_metadata,
    employees_by_department,
    employees_by_gender,
    employees_by_tenure,
    load_aggregation_config,
    new_job_run_id,
)
from src.exceptions import AggregationError, ColumnNotFoundError, ConfigurationError, DataSourceError
from tests.helpers import CONFIG_DIR, PROJECT_ROOT, expected_tenure

pytestmark = pytest.mark.spark

RUN_TIMESTAMP = dt.datetime(2026, 1, 31, 23, 45, 10, tzinfo=dt.timezone.utc)
FINAL_SCHEMA = "emp_no int, gender string, tenure_years bigint, dept_no string, to_date date"
OPEN = dt.date(9999, 1, 1)


def _rows(df, *columns):
    return [tuple(row[c] for c in columns) for row in df.collect()]


# --------------------------------------------------------------------------- #
# 1. employees_by_gender
# --------------------------------------------------------------------------- #
class TestEmployeesByGender:
    def test_distinct_employees_per_gender(self, final_df):
        result = employees_by_gender(final_df)

        assert _rows(result, "gender", "employee_count") == [("F", 6), ("M", 6)]

    def test_employee_in_two_departments_is_counted_once(self, final_df):
        female_rows = final_df.filter("gender = 'F'").count()

        female_count = employees_by_gender(final_df).filter("gender = 'F'").first().employee_count

        assert (female_rows, female_count) == (7, 6)

    def test_total_equals_distinct_employees(self, final_df):
        total = employees_by_gender(final_df).agg(F.sum("employee_count")).first()[0]

        assert total == final_df.select("emp_no").distinct().count() == 12

    def test_output_columns_and_types(self, final_df):
        assert employees_by_gender(final_df).dtypes == [("gender", "string"), ("employee_count", "bigint")]

    def test_null_gender_is_its_own_group_listed_last(self, make_df):
        df = make_df(
            [
                (1, "M", 1, "d1", OPEN),
                (2, None, 1, "d1", OPEN),
                (3, "F", 1, "d1", OPEN),
                (2, None, 1, "d2", OPEN),
            ],
            FINAL_SCHEMA,
        )

        assert _rows(employees_by_gender(df), "gender", "employee_count") == [("F", 1), ("M", 1), (None, 1)]

    def test_missing_column_raises(self, make_df):
        with pytest.raises(ColumnNotFoundError, match=r"employees_by_gender: column\(s\) \['gender'\]"):
            employees_by_gender(make_df([(1,)], "emp_no int"))


# --------------------------------------------------------------------------- #
# 2. employees_by_tenure
# --------------------------------------------------------------------------- #
class TestEmployeesByTenure:
    @pytest.fixture(scope="class")
    def expected_counts(self, employees_df):
        """Computed independently from the source employees table."""
        return Counter(expected_tenure(row.hire_date) for row in employees_df.collect())

    def test_counts_match_source_employees(self, final_df, expected_counts):
        result = employees_by_tenure(final_df)

        assert dict(_rows(result, "tenure_years", "employee_count")) == dict(expected_counts)

    def test_sorted_by_tenure_descending_by_default(self, final_df, expected_counts):
        tenures = [row.tenure_years for row in employees_by_tenure(final_df).collect()]

        assert tenures == sorted(expected_counts, reverse=True)

    @pytest.mark.parametrize("order", ["asc", "ASC", " Asc "])
    def test_ascending_order(self, final_df, order):
        tenures = [row.tenure_years for row in employees_by_tenure(final_df, order).collect()]

        assert tenures == sorted(tenures)

    def test_total_equals_distinct_employees(self, final_df):
        assert employees_by_tenure(final_df).agg(F.sum("employee_count")).first()[0] == 12

    def test_duplicate_rows_counted_once_and_order(self, make_df):
        df = make_df(
            [
                (1, "M", 5, "d1", OPEN),
                (1, "M", 5, "d2", OPEN),
                (2, "F", 5, "d1", OPEN),
                (3, "F", 12, "d1", OPEN),
                (4, "M", 0, "d3", OPEN),
            ],
            FINAL_SCHEMA,
        )

        assert _rows(employees_by_tenure(df), "tenure_years", "employee_count") == [(12, 1), (5, 2), (0, 1)]

    @pytest.mark.parametrize("order", ["descending", "", None])
    def test_invalid_order_raises(self, final_df, order):
        with pytest.raises(ConfigurationError, match="unsupported order"):
            employees_by_tenure(final_df, order)

    def test_output_columns_and_types(self, final_df):
        assert employees_by_tenure(final_df).dtypes == [
            ("tenure_years", "bigint"),
            ("employee_count", "bigint"),
        ]


# --------------------------------------------------------------------------- #
# 3. employees_by_department
# --------------------------------------------------------------------------- #
class TestEmployeesByDepartment:
    # From the sample dept_emp rows (to_date 9999-01-01 = currently working there).
    EXPECTED = {
        "d005": (3, 4),  # 10001, 10006, 10012 current; 10008 left in 2000
        "d004": (2, 3),  # 10003, 10004 current; 10010 moved to d006
        "d006": (2, 2),  # 10009, 10010
        "d003": (1, 1),
        "d007": (1, 1),
        "d008": (1, 1),
        "d009": (0, 1),  # 10011 left in 1996 -> still listed with 0 current
    }

    def test_current_and_total_counts(self, final_df):
        result = employees_by_department(final_df)

        rows = _rows(result, "dept_no", "current_employee_count", "total_employee_count")
        assert {dept: (cur, tot) for dept, cur, tot in rows} == self.EXPECTED

    def test_ordered_by_current_count_desc_then_dept(self, final_df):
        result = employees_by_department(final_df)

        assert [row.dept_no for row in result.collect()] == [
            "d005",
            "d004",
            "d006",
            "d003",
            "d007",
            "d008",
            "d009",
        ]

    def test_current_total_is_number_of_current_assignments(self, final_df, dept_emp_df):
        current_total = employees_by_department(final_df).agg(F.sum("current_employee_count")).first()[0]

        assert current_total == dept_emp_df.filter(F.col("to_date") == OPEN).count() == 10

    def test_repeated_rows_for_same_department_counted_once(self, make_df):
        df = make_df(
            [(1, "M", 3, "d1", OPEN), (1, "M", 3, "d1", OPEN), (2, "F", 3, "d1", dt.date(2001, 1, 1))],
            FINAL_SCHEMA,
        )

        assert _rows(
            employees_by_department(df), "dept_no", "current_employee_count", "total_employee_count"
        ) == [("d1", 1, 2)]

    def test_custom_current_to_date(self, make_df):
        df = make_df([(1, "M", 3, "d1", dt.date(2099, 12, 31)), (2, "F", 3, "d1", OPEN)], FINAL_SCHEMA)

        row = employees_by_department(df, current_to_date="2099-12-31").first()

        assert (row.current_employee_count, row.total_employee_count) == (1, 2)

    @pytest.mark.parametrize("value", ["31-12-9999", "not a date"])
    def test_invalid_current_to_date_raises(self, final_df, value):
        with pytest.raises(ConfigurationError, match="'current_to_date' must be a date in yyyy-MM-dd"):
            employees_by_department(final_df, current_to_date=value)

    def test_missing_column_raises(self, make_df):
        with pytest.raises(ColumnNotFoundError, match=r"\['to_date'\]"):
            employees_by_department(make_df([(1, "d1")], "emp_no int, dept_no string"))

    def test_output_columns_and_types(self, final_df):
        assert employees_by_department(final_df).dtypes == [
            ("dept_no", "string"),
            ("current_employee_count", "bigint"),
            ("total_employee_count", "bigint"),
        ]


# --------------------------------------------------------------------------- #
# Run metadata
# --------------------------------------------------------------------------- #
class TestRunMetadata:
    CONTEXT = JobRunContext(
        job_name="employee_analytics",
        job_run_id="run-42",
        job_run_timestamp=RUN_TIMESTAMP,
        environment="test",
        source_path="data/transform/final.parquet",
        source_record_count=13,
    )

    def test_columns_types_and_values(self, final_df):
        result = add_run_metadata(employees_by_gender(final_df), self.CONTEXT)

        assert result.dtypes[2:] == [
            ("job_name", "string"),
            ("job_run_id", "string"),
            ("job_run_date", "date"),
            ("job_run_timestamp", "timestamp"),
            ("environment", "string"),
            ("source_path", "string"),
            ("source_record_count", "bigint"),
        ]
        row = result.first()
        assert (row.job_name, row.job_run_id, row.environment, row.source_path, row.source_record_count) == (
            "employee_analytics",
            "run-42",
            "test",
            "data/transform/final.parquet",
            13,
        )
        assert row.job_run_date == dt.date(2026, 1, 31)

    def test_timestamp_is_the_utc_instant(self, final_df):
        row = add_run_metadata(employees_by_gender(final_df), self.CONTEXT).first()

        assert row.job_run_timestamp.astimezone(dt.timezone.utc) == RUN_TIMESTAMP

    def test_aggregation_columns_are_kept_first(self, final_df):
        result = add_run_metadata(employees_by_tenure(final_df), self.CONTEXT)

        assert result.columns == ["tenure_years", "employee_count", *RUN_METADATA_COLUMNS]
        assert result.count() == employees_by_tenure(final_df).count()

    def test_column_clash_raises(self, make_df):
        with pytest.raises(AggregationError, match=r"\['job_run_id'\] already exist"):
            add_run_metadata(make_df([("x",)], "job_run_id string"), self.CONTEXT)

    def test_generated_job_run_id(self):
        first = new_job_run_id("employee_analytics", RUN_TIMESTAMP)
        second = new_job_run_id("employee_analytics", RUN_TIMESTAMP)

        assert re.fullmatch(r"employee_analytics-20260131T234510Z-[0-9a-f]{8}", first)
        assert first != second


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
def _valid_config(tmp_path, source_path, **overrides):
    config = {
        "job_name": "employee_analytics",
        "environment": "test",
        "source": {"path": str(source_path), "format": "parquet"},
        "write_mode": "overwrite",
        "aggregations": {
            "employees_by_gender": {"output_path": str(tmp_path / "analytics" / "by_gender.parquet")},
            "employees_by_tenure": {
                "output_path": str(tmp_path / "analytics" / "by_tenure.parquet"),
                "order": "desc",
            },
            "employees_by_department": {"output_path": str(tmp_path / "analytics" / "by_department.parquet")},
        },
    }
    config.update(overrides)
    return config


class TestLoadAggregationConfig:
    def test_project_config_is_valid(self):
        config = load_aggregation_config(str(CONFIG_DIR / "aggregation.json"))

        assert set(config["aggregations"]) == set(aggregate.AGGREGATIONS)
        assert config["source"]["path"] == "data/transform/final.parquet"

    def test_project_outputs_are_separate_and_git_ignored(self):
        config = load_aggregation_config(str(CONFIG_DIR / "aggregation.json"))
        paths = [spec["output_path"] for spec in config["aggregations"].values()]
        ignored = (PROJECT_ROOT / ".gitignore").read_text().splitlines()

        assert len(set(paths)) == len(paths) == 3
        assert all(path.startswith("data/analytics/") for path in paths)
        assert "data/analytics/" in ignored

    def test_defaults(self, write_json, tmp_path):
        config = _valid_config(tmp_path, "final.parquet")
        for key in ("job_name", "environment", "write_mode"):
            config.pop(key)

        loaded = load_aggregation_config(write_json(config))

        assert (loaded["job_name"], loaded["environment"], loaded["write_mode"]) == (
            "employee_analytics",
            "local",
            "overwrite",
        )

    def test_environment_from_env_reference(self, write_json, tmp_path, monkeypatch):
        monkeypatch.setenv("ETL_ENV", "prod")

        loaded = load_aggregation_config(
            write_json(_valid_config(tmp_path, "f.parquet", environment="${env:ETL_ENV}"))
        )

        assert loaded["environment"] == "prod"

    def test_all_problems_reported_together(self, write_json, tmp_path):
        config = _valid_config(tmp_path, "data/final.parquet", write_mode="upsert")
        aggs = config["aggregations"]
        aggs["employees_by_salary"] = {"output_path": "x"}
        aggs["employees_by_tenure"]["output_path"] = aggs["employees_by_gender"]["output_path"]
        aggs["employees_by_department"] = {
            "output_path": "data/final.parquet/",
            "sort": "x",
            "enabled": "yes",
        }

        with pytest.raises(ConfigurationError) as exc:
            load_aggregation_config(write_json(config))

        message = str(exc.value)
        assert "'write_mode' must be one of overwrite, append, got 'upsert'" in message
        assert "unknown aggregation 'employees_by_salary'" in message
        assert "'employees_by_tenure.output_path' is the same as 'employees_by_gender.output_path'" in message
        assert "'employees_by_department.output_path' must not be the source path" in message
        assert "'employees_by_department' has unknown key(s) ['sort']" in message
        assert "'employees_by_department.enabled' must be true or false" in message

    @pytest.mark.parametrize(
        "overrides, message",
        [
            (
                {"aggregations": {"employees_by_gender": "out.parquet"}},
                "'employees_by_gender' must be an object",
            ),
            (
                {"aggregations": {"employees_by_gender": {}}},
                "'employees_by_gender.output_path' must be a non-empty",
            ),
            ({"source": {}}, "'source.path' must be a non-empty string"),
            ({"aggregations": {}}, "'aggregations' must be a non-empty object"),
            ({"job_name": " "}, "'job_name' must be a non-empty string"),
        ],
    )
    def test_invalid_values(self, write_json, tmp_path, overrides, message):
        with pytest.raises(ConfigurationError, match=message):
            load_aggregation_config(write_json(_valid_config(tmp_path, "f.parquet", **overrides)))


# --------------------------------------------------------------------------- #
# End to end: final.parquet -> three analytics outputs
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def default_run(spark, final_df, final_output_path, tmp_path_factory):
    """One aggregation run with the default config, shared by read-only tests."""
    tmp_path = tmp_path_factory.mktemp("aggregate_default")
    config_path = tmp_path / "agg.json"
    config_path.write_text(json.dumps(_valid_config(tmp_path, final_output_path)))
    outputs = aggregate.main(
        str(config_path), spark=spark, job_run_id="airflow-123", run_timestamp=RUN_TIMESTAMP
    )
    return tmp_path, outputs


class TestMainDefaultRun:
    def test_each_aggregation_has_its_own_output(self, spark, default_run, final_df):
        tmp_path, outputs = default_run

        assert {name: out.row_count for name, out in outputs.items()} == {
            "employees_by_gender": 2,
            "employees_by_tenure": final_df.select("tenure_years").distinct().count(),
            "employees_by_department": 7,
        }
        folders = sorted(p.name for p in (tmp_path / "analytics").iterdir())
        assert folders == ["by_department.parquet", "by_gender.parquet", "by_tenure.parquet"]
        for out in outputs.values():
            assert spark.read.parquet(out.output_path).count() == out.row_count
            assert len(list((tmp_path / "analytics" / out.output_path.split("/")[-1]).glob("part-*"))) == 1

    def test_written_values_match_aggregations(self, spark, default_run, final_df):
        _, outputs = default_run

        written = spark.read.parquet(outputs["employees_by_gender"].output_path)
        assert _rows(written, "gender", "employee_count") == [("F", 6), ("M", 6)]
        tenure = spark.read.parquet(outputs["employees_by_tenure"].output_path)
        assert _rows(tenure, "tenure_years", "employee_count") == _rows(
            employees_by_tenure(final_df), "tenure_years", "employee_count"
        )
        department = spark.read.parquet(outputs["employees_by_department"].output_path)
        assert _rows(department, "dept_no", "current_employee_count", "total_employee_count")[0] == (
            "d005",
            3,
            4,
        )

    def test_written_order_is_kept(self, spark, default_run):
        _, outputs = default_run

        tenures = [
            r.tenure_years for r in spark.read.parquet(outputs["employees_by_tenure"].output_path).collect()
        ]
        assert tenures == sorted(tenures, reverse=True)

    def test_same_run_metadata_on_every_output(self, spark, default_run):
        _, outputs = default_run

        for out in outputs.values():
            meta = spark.read.parquet(out.output_path).select(*RUN_METADATA_COLUMNS).distinct().collect()
            assert len(meta) == 1
            row = meta[0]
            assert (
                row.job_name,
                row.job_run_id,
                row.job_run_date,
                row.environment,
                row.source_record_count,
            ) == (
                "employee_analytics",
                "airflow-123",
                dt.date(2026, 1, 31),
                "test",
                13,
            )
            assert row.source_path.endswith("final.parquet")


class TestMain:
    @pytest.fixture
    def run(self, spark, final_df, final_output_path, tmp_path, write_json):
        def _run(job_run_id="run-1", run_timestamp=RUN_TIMESTAMP, **overrides):
            config_path = write_json(_valid_config(tmp_path, final_output_path, **overrides), name="agg.json")
            return aggregate.main(
                config_path, spark=spark, job_run_id=job_run_id, run_timestamp=run_timestamp
            )

        return _run

    def test_job_run_id_from_environment_variable(self, spark, run, monkeypatch):
        monkeypatch.setenv("ETL_JOB_RUN_ID", "glue-jr_abc")

        outputs = run(job_run_id=None)

        ids = spark.read.parquet(outputs["employees_by_gender"].output_path).select("job_run_id").distinct()
        assert [r.job_run_id for r in ids.collect()] == ["glue-jr_abc"]

    def test_job_run_id_generated_when_not_given(self, spark, run, monkeypatch):
        monkeypatch.delenv("ETL_JOB_RUN_ID", raising=False)

        outputs = run(job_run_id=None)

        run_id = spark.read.parquet(outputs["employees_by_gender"].output_path).first().job_run_id
        assert re.fullmatch(r"employee_analytics-20260131T234510Z-[0-9a-f]{8}", run_id)

    def test_overwrite_keeps_only_latest_run(self, spark, run):
        run(job_run_id="run-1")
        outputs = run(job_run_id="run-2")

        df = spark.read.parquet(outputs["employees_by_gender"].output_path)
        assert [r.job_run_id for r in df.select("job_run_id").distinct().collect()] == ["run-2"]
        assert df.count() == 2

    def test_append_keeps_run_history(self, spark, run):
        run(job_run_id="run-1", write_mode="append")
        outputs = run(
            job_run_id="run-2", write_mode="append", run_timestamp=RUN_TIMESTAMP + dt.timedelta(days=1)
        )

        df = spark.read.parquet(outputs["employees_by_gender"].output_path)
        latest_date = df.agg(F.max("job_run_date")).first()[0]
        assert sorted(r.job_run_id for r in df.select("job_run_id").distinct().collect()) == [
            "run-1",
            "run-2",
        ]
        assert df.count() == 4
        assert latest_date == dt.date(2026, 2, 1)

    def test_disabled_aggregation_is_not_written(self, run, tmp_path, final_output_path):
        config = _valid_config(tmp_path, final_output_path)
        config["aggregations"]["employees_by_tenure"]["enabled"] = False

        outputs = run(aggregations=config["aggregations"])

        assert "employees_by_tenure" not in outputs
        assert not (tmp_path / "analytics" / "by_tenure.parquet").exists()

    def test_missing_source_gives_hint(self, spark, tmp_path, write_json):
        config_path = write_json(_valid_config(tmp_path, tmp_path / "missing" / "final.parquet"))

        with pytest.raises(DataSourceError, match="Run 'python -m src.transform' first"):
            aggregate.main(config_path, spark=spark, job_run_id="x")
        assert not (tmp_path / "analytics").exists()

    def test_source_without_required_columns(self, spark, tmp_path, write_json, departments_df):
        source = tmp_path / "departments.parquet"
        departments_df.write.parquet(str(source))

        with pytest.raises(ColumnNotFoundError, match="Aggregation 'employees_by_gender' failed"):
            aggregate.main(write_json(_valid_config(tmp_path, source)), spark=spark, job_run_id="x")

    def test_unwritable_output_raises(self, run, tmp_path, final_output_path):
        blocker = tmp_path / "blocker"
        blocker.write_text("a file where a folder is expected")
        config = _valid_config(tmp_path, final_output_path)
        config["aggregations"]["employees_by_gender"]["output_path"] = str(blocker / "by_gender.parquet")

        with pytest.raises(
            AggregationError, match=r"write_aggregation: failed to write '.*blocker/by_gender.parquet'"
        ):
            run(aggregations=config["aggregations"])

    def test_empty_source_writes_empty_outputs(self, spark, tmp_path, write_json, final_df):
        source = tmp_path / "empty.parquet"
        final_df.limit(0).write.parquet(str(source))

        outputs = aggregate.main(write_json(_valid_config(tmp_path, source)), spark=spark, job_run_id="x")

        assert {out.row_count for out in outputs.values()} == {0}
        written = spark.read.parquet(outputs["employees_by_gender"].output_path)
        assert written.columns == ["gender", "employee_count", *RUN_METADATA_COLUMNS]
        assert written.count() == 0
