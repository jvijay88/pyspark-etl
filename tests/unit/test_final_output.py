"""Tests for the final parquet produced by the transform step.

The pipeline (src.transform.main + config/transformation.json) is run on:
* the real sample slice of data/extract (``final_df``), and
* a mock extract with edge cases (``edge_final_df``).
The written final.parquet is read back and validated column by column
against the source data.
"""

import datetime as dt
import re

import pytest
from pyspark.sql import functions as F

from src import transform
from src.exceptions import (
    ConfigurationError,
    DataSourceError,
    DataValidationError,
    TransformationError,
)
from tests.helpers import FINAL_OUTPUT_DTYPES, expected_tenure, row_by_emp

pytestmark = pytest.mark.spark


def _run_pipeline(spark, config_path, extract_dir, output_path):
    transform.main(
        json_path=config_path,
        extract_folder_path=extract_dir,
        output_path=str(output_path),
        spark=spark,
    )
    return spark.read.parquet(str(output_path))


@pytest.fixture(scope="module")
def source_rows(employees_df, dept_emp_df):
    employees = {r.emp_no: r for r in employees_df.collect()}
    dept_emp = dept_emp_df.collect()
    return employees, dept_emp


# --------------------------------------------------------------------------- #
# Output file and schema
# --------------------------------------------------------------------------- #
class TestFinalOutputFile:
    def test_parquet_written_with_success_marker(self, final_df, final_output_path):
        assert (final_output_path / "_SUCCESS").exists()
        assert list(final_output_path.glob("part-*.parquet"))

    def test_schema_names_types_and_order(self, final_df):
        assert final_df.dtypes == FINAL_OUTPUT_DTYPES

    def test_dropped_columns_are_absent(self, final_df):
        assert not {"birth_date", "first_name", "last_name"} & set(final_df.columns)


# --------------------------------------------------------------------------- #
# Row level completeness / integrity
# --------------------------------------------------------------------------- #
class TestFinalOutputRows:
    def test_one_row_per_department_assignment(self, final_df, source_rows):
        _, dept_emp = source_rows

        assert final_df.count() == len(dept_emp) == 13

    def test_every_department_assignment_is_present(self, final_df, source_rows):
        _, dept_emp = source_rows
        expected = {(r.emp_no, r.dept_no, r.from_date, r.to_date) for r in dept_emp}

        actual = {(r.emp_no, r.dept_no, r.from_date, r.to_date) for r in final_df.collect()}

        assert actual == expected

    def test_every_employee_is_present(self, final_df, source_rows):
        employees, _ = source_rows

        assert {r.emp_no for r in final_df.select("emp_no").distinct().collect()} == set(employees)

    def test_business_key_is_unique(self, final_df):
        duplicates = final_df.groupBy("emp_no", "dept_no", "from_date").count().filter("count > 1")

        assert duplicates.count() == 0

    @pytest.mark.parametrize(
        "column",
        [
            "emp_no",
            "gender",
            "hire_date",
            "tenure_years",
            "birth_year",
            "birth_month",
            "birth_day",
            "Full_name",
            "dept_no",
            "from_date",
            "to_date",
        ],
    )
    def test_no_nulls(self, final_df, column):
        assert final_df.filter(F.col(column).isNull()).count() == 0

    def test_department_dates_are_ordered(self, final_df):
        assert final_df.filter(F.col("from_date") > F.col("to_date")).count() == 0

    def test_gender_domain(self, final_df):
        assert {r.gender for r in final_df.select("gender").distinct().collect()} <= {"M", "F"}


# --------------------------------------------------------------------------- #
# Derived column values vs. source data
# --------------------------------------------------------------------------- #
class TestFinalOutputValues:
    def test_hire_date_format_and_value(self, final_df, source_rows):
        employees, _ = source_rows

        for row in final_df.collect():
            assert re.fullmatch(r"\d{2}-\d{2}-\d{4}", row.hire_date), row.hire_date
            assert row.hire_date == employees[row.emp_no].hire_date.strftime("%d-%m-%Y")

    def test_tenure_matches_source_hire_date(self, final_df, source_rows):
        employees, _ = source_rows

        for row in final_df.collect():
            assert row.tenure_years == expected_tenure(employees[row.emp_no].hire_date)
            assert row.tenure_years >= 0

    def test_birth_parts_rebuild_source_birth_date(self, final_df, source_rows):
        employees, _ = source_rows

        for row in final_df.collect():
            rebuilt = dt.date(int(row.birth_year), int(row.birth_month), int(row.birth_day))
            assert rebuilt == employees[row.emp_no].birth_date
            assert (len(row.birth_year), len(row.birth_month), len(row.birth_day)) == (4, 2, 2)

    def test_full_name_is_upper_first_and_last_name(self, final_df, source_rows):
        employees, _ = source_rows

        for row in final_df.collect():
            source = employees[row.emp_no]
            assert row.Full_name == f"{source.first_name} {source.last_name}".upper()

    def test_known_employee_row(self, final_df):
        row = row_by_emp(final_df, 10001)

        assert row.asDict() == {
            "emp_no": 10001,
            "gender": "M",
            "hire_date": "26-06-1986",
            "tenure_years": expected_tenure(dt.date(1986, 6, 26)),
            "birth_year": "1953",
            "birth_month": "09",
            "birth_day": "02",
            "Full_name": "GEORGI FACELLO",
            "dept_no": "d005",
            "from_date": dt.date(1986, 6, 26),
            "to_date": dt.date(9999, 1, 1),
        }

    def test_employee_in_two_departments(self, final_df):
        rows = sorted(final_df.filter("emp_no = 10010").collect(), key=lambda r: r.from_date)

        assert [(r.dept_no, r.from_date, r.to_date) for r in rows] == [
            ("d004", dt.date(1996, 11, 24), dt.date(2000, 6, 26)),
            ("d006", dt.date(2000, 6, 26), dt.date(9999, 1, 1)),
        ]
        assert rows[0].Full_name == rows[1].Full_name == "DUANGKAEW PIVETEAU"


# --------------------------------------------------------------------------- #
# Step order independence
# --------------------------------------------------------------------------- #
class TestStepOrder:
    def test_date_format_before_tenure_gives_same_output(
        self, spark, final_df, transformation_config, sample_extract_dir, write_json, tmp_path
    ):
        steps = transformation_config["transformations"]
        tenure = next(s for s in steps if s["transformation"] == "calculate_tenure")
        date_format = next(s for s in steps if s["transformation"] == "change_date_format")
        others = [s for s in steps if s not in (tenure, date_format)]
        transformation_config["transformations"] = [date_format, tenure, *others]

        reordered = _run_pipeline(
            spark, write_json(transformation_config), sample_extract_dir, tmp_path / "out.parquet"
        )

        assert reordered.count() == final_df.count()
        assert sorted(reordered.collect()) == sorted(final_df.collect())


# --------------------------------------------------------------------------- #
# Edge cases with a mock extract
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def edge_final_df(spark, project_config_path, tmp_path_factory):
    employees = [
        (1, dt.date(1980, 1, 15), "alice", "smith", "F", dt.date(2010, 3, 1)),  # normal
        (2, dt.date(1990, 12, 31), "Bob", None, "M", dt.date(2015, 7, 20)),  # no last name
        (3, dt.date(1970, 7, 7), None, None, "F", dt.date(2001, 1, 1)),  # no names
        (4, dt.date(1985, 2, 2), "  ", "", "M", dt.date(2005, 5, 5)),  # blank names
        (5, dt.date(1975, 6, 5), "Carl", "Diaz", "M", None),  # no hire date
        (6, dt.date(1995, 3, 3), "Dana", "Lee", "F", dt.date.today()),  # hired today
        (7, dt.date(1960, 1, 1), "No", "Dept", "M", dt.date(1990, 1, 1)),  # no department
    ]
    dept_emp = [(emp_no, "d001", dt.date(2020, 1, 1), dt.date(9999, 1, 1)) for emp_no in range(1, 7)]

    extract_dir = tmp_path_factory.mktemp("edge_extract")
    spark.createDataFrame(
        employees,
        "emp_no int, birth_date date, first_name string, last_name string, gender string, hire_date date",
    ).write.parquet(str(extract_dir / "employees.parquet"))
    spark.createDataFrame(dept_emp, "emp_no int, dept_no string, from_date date, to_date date").write.parquet(
        str(extract_dir / "dept_emp.parquet")
    )

    output = tmp_path_factory.mktemp("edge_transform") / "final.parquet"
    return _run_pipeline(spark, project_config_path, str(extract_dir), output).cache()


class TestFinalOutputEdgeCases:
    def test_schema_same_as_real_data(self, edge_final_df):
        assert edge_final_df.dtypes == FINAL_OUTPUT_DTYPES

    def test_missing_names_become_unknown(self, edge_final_df):
        assert row_by_emp(edge_final_df, 3).Full_name == "UNKNOWN"  # NULL names
        assert row_by_emp(edge_final_df, 4).Full_name == "UNKNOWN"  # blank names

    def test_partial_name_is_kept(self, edge_final_df):
        assert row_by_emp(edge_final_df, 1).Full_name == "ALICE SMITH"
        assert row_by_emp(edge_final_df, 2).Full_name == "BOB"

    def test_no_empty_full_names(self, edge_final_df):
        assert edge_final_df.filter(F.trim("Full_name") == "").count() == 0

    def test_rows_without_hire_date_are_filtered_out(self, edge_final_df):
        """tenure_years is NULL without a hire date, so 'tenure_years >= 0' drops it."""
        assert edge_final_df.filter("emp_no = 5").count() == 0

    def test_employee_hired_today_has_zero_tenure(self, edge_final_df):
        assert row_by_emp(edge_final_df, 6).tenure_years == 0

    def test_employee_without_department_is_excluded_by_inner_join(self, edge_final_df):
        assert edge_final_df.filter("emp_no = 7").count() == 0

    def test_expected_employees(self, edge_final_df):
        assert sorted(r.emp_no for r in edge_final_df.collect()) == [1, 2, 3, 4, 6]


# --------------------------------------------------------------------------- #
# Failures must not produce an output file
# --------------------------------------------------------------------------- #
class TestFinalOutputNotWrittenOnError:
    def test_invalid_config(self, spark, sample_extract_dir, write_json, tmp_path):
        output = tmp_path / "final.parquet"

        with pytest.raises(ConfigurationError, match="unknown transformation 'pivot'"):
            _run_pipeline(
                spark,
                write_json({"transformations": [{"transformation": "pivot"}]}),
                sample_extract_dir,
                output,
            )
        assert not output.exists()

    def test_missing_config_file(self, spark, sample_extract_dir, tmp_path):
        output = tmp_path / "final.parquet"

        with pytest.raises(ConfigurationError, match="configuration file not found"):
            _run_pipeline(spark, str(tmp_path / "missing.json"), sample_extract_dir, output)
        assert not output.exists()

    def test_missing_input_table(self, spark, project_config_path, tmp_path):
        output = tmp_path / "final.parquet"

        with pytest.raises(DataSourceError, match="Input table 'employees' could not be read"):
            _run_pipeline(spark, project_config_path, str(tmp_path / "empty_extract"), output)
        assert not output.exists()

    def test_output_location_not_writable(self, spark, project_config_path, sample_extract_dir, tmp_path):
        blocker = tmp_path / "not_a_folder"
        blocker.write_text("a regular file where a folder is expected")

        with pytest.raises(TransformationError, match="Failed to write transformed data to"):
            _run_pipeline(spark, project_config_path, sample_extract_dir, blocker / "final.parquet")

    def test_unparseable_hire_dates(self, spark, project_config_path, tmp_path):
        extract_dir = tmp_path / "extract"
        spark.createDataFrame(
            [(1, dt.date(1980, 1, 1), "A", "B", "M", "2020/01/31")],
            "emp_no int, birth_date date, first_name string, last_name string, gender string, "
            "hire_date string",
        ).write.parquet(str(extract_dir / "employees.parquet"))
        spark.createDataFrame(
            [(1, "d001", dt.date(2020, 1, 1), dt.date(9999, 1, 1))],
            "emp_no int, dept_no string, from_date date, to_date date",
        ).write.parquet(str(extract_dir / "dept_emp.parquet"))
        output = tmp_path / "final.parquet"

        with pytest.raises(DataValidationError, match=r"step 1 \('calculate_tenure'\).*2020/01/31"):
            _run_pipeline(spark, project_config_path, str(extract_dir), output)
        assert not output.exists()
