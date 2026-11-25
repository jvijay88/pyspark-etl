"""
Shared pytest fixtures.

* ONE local SparkSession per test run (JVM start-up is the expensive part).
  It is passed explicitly to the code under test; no external systems and no
  SparkInitializer connectivity are involved.
* "Real" data is a small deterministic slice of data/extract/*.parquet.
* Mock data covers edge cases the real extract does not contain (it has no
  NULLs, blanks or unmatched keys).
"""

import datetime as dt
import json
import os
import sys

import pytest

from tests.helpers import CONFIG_DIR, EXTRACT_DIR, SAMPLE_MAX_EMP_NO

# Spark python workers must use the interpreter running the tests
# (avoids "Python worker failed to connect back" on Windows).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# --------------------------------------------------------------------------- #
# Spark
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("pyspark-etl-unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# --------------------------------------------------------------------------- #
# Real sample data (data/extract/*.parquet)
# --------------------------------------------------------------------------- #
def _read_sample(spark, table):
    df = spark.read.parquet(str(EXTRACT_DIR / f"{table}.parquet"))
    if "emp_no" in df.columns:
        df = df.filter(df.emp_no <= SAMPLE_MAX_EMP_NO)
    return df.cache()


@pytest.fixture(scope="session")
def employees_df(spark):
    """12 rows: emp_no:int, birth_date:date, first_name, last_name, gender, hire_date:date."""
    return _read_sample(spark, "employees")


@pytest.fixture(scope="session")
def dept_emp_df(spark):
    """13 rows: emp_no:int, dept_no, from_date:date, to_date:date."""
    return _read_sample(spark, "dept_emp")


@pytest.fixture(scope="session")
def departments_df(spark):
    """9 rows: dept_no, dept_name."""
    return _read_sample(spark, "departments")


@pytest.fixture(scope="session")
def tables(employees_df, dept_emp_df, departments_df):
    """Table registry as built by transform.main()."""
    return {"employees": employees_df, "dept_emp": dept_emp_df, "departments": departments_df}


@pytest.fixture
def transformation_config():
    """Fresh copy of config/transformation.json (safe to modify in a test)."""
    return json.loads((CONFIG_DIR / "transformation.json").read_text())


@pytest.fixture(scope="session")
def sample_extract_dir(tmp_path_factory, employees_df, dept_emp_df, departments_df):
    """A local extract folder with the sample slice written as parquet."""
    extract_dir = tmp_path_factory.mktemp("extract")
    employees_df.write.parquet(str(extract_dir / "employees.parquet"))
    dept_emp_df.write.parquet(str(extract_dir / "dept_emp.parquet"))
    departments_df.write.parquet(str(extract_dir / "departments.parquet"))
    return str(extract_dir)


# --------------------------------------------------------------------------- #
# Mock data
# --------------------------------------------------------------------------- #
EMPLOYEE_SCHEMA = (
    "emp_no int, birth_date date, first_name string, last_name string, gender string, hire_date date"
)


@pytest.fixture(scope="session")
def mock_employees_df(spark):
    """Employees schema with NULL / blank edge cases."""
    rows = [
        (1, dt.date(1980, 1, 15), "alice", "smith", "F", dt.date(2010, 3, 1)),
        (2, dt.date(1990, 12, 31), "Bob", None, "M", dt.date(2015, 7, 20)),
        (3, None, None, "Jones", None, None),
        (4, dt.date(1975, 6, 5), None, None, "F", dt.date(2000, 1, 1)),
        (5, dt.date(1985, 2, 2), "  ", "", "M", dt.date(2005, 5, 5)),
    ]
    return spark.createDataFrame(rows, EMPLOYEE_SCHEMA)


@pytest.fixture
def make_df(spark):
    """Factory for ad-hoc DataFrames: make_df(rows, "a string, b int")."""

    def _make(rows, schema):
        return spark.createDataFrame(rows, schema)

    return _make


@pytest.fixture
def write_json(tmp_path):
    """Write a python object (or raw string) to a JSON file and return its path."""

    def _write(content, name="config.json"):
        path = tmp_path / name
        path.write_text(content if isinstance(content, str) else json.dumps(content))
        return str(path)

    return _write


# --------------------------------------------------------------------------- #
# Pipeline output (transform.main on the sample extract), shared by the
# final-output tests and the aggregation tests.
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="session")
def project_config_path():
    return str(CONFIG_DIR / "transformation.json")


@pytest.fixture(scope="session")
def final_output_path(tmp_path_factory):
    return tmp_path_factory.mktemp("transform") / "final.parquet"


@pytest.fixture(scope="session")
def final_df(spark, project_config_path, sample_extract_dir, final_output_path):
    """final.parquet written by the pipeline for the real sample extract (13 rows)."""
    from src import transform

    transform.main(
        json_path=project_config_path,
        extract_folder_path=sample_extract_dir,
        output_path=str(final_output_path),
        spark=spark,
    )
    return spark.read.parquet(str(final_output_path)).cache()
