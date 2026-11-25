"""Constants and small helpers shared by the test modules."""

import datetime as dt
import math
from pathlib import Path

from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXTRACT_DIR = PROJECT_ROOT / "data" / "extract"
CONFIG_DIR = PROJECT_ROOT / "config"

# Highest emp_no in the "real data" sample (12 employees, 13 dept_emp rows;
# emp_no 10010 belongs to two departments).
SAMPLE_MAX_EMP_NO = 10012

EMPLOYEE_COLUMNS = ["emp_no", "birth_date", "first_name", "last_name", "gender", "hire_date"]

# Schema of data/transform/final.parquet produced by config/transformation.json
FINAL_OUTPUT_DTYPES = [
    ("emp_no", "int"),
    ("gender", "string"),
    ("hire_date", "string"),
    ("tenure_years", "bigint"),
    ("birth_year", "string"),
    ("birth_month", "string"),
    ("birth_day", "string"),
    ("Full_name", "string"),
    ("dept_no", "string"),
    ("from_date", "date"),
    ("to_date", "date"),
]


def row_by_emp(df, emp_no):
    """Return the single Row for ``emp_no`` (fails if there is not exactly one)."""
    rows = df.filter(F.col("emp_no") == emp_no).collect()
    assert len(rows) == 1, f"expected 1 row for emp_no={emp_no}, got {len(rows)}"
    return rows[0]


def expected_tenure(start: dt.date) -> int:
    """Python mirror of floor(datediff(current_date, start) / 365.25)."""
    return math.floor((dt.date.today() - start).days / 365.25)
