"""
Module Name: aggregate.py
Description: Business aggregations built from the transform output
             (data/transform/final.parquet). Each aggregation is written to its
             own output folder together with job run metadata for analytics.

Aggregations:
    employees_by_gender      distinct employees per gender
    employees_by_tenure      distinct employees per tenure_years, ordered by tenure (desc by default)
    employees_by_department  distinct employees per department: currently working + ever assigned

final.parquet has one row per department assignment (an employee who moved
department appears more than once), so every count is a distinct count of emp_no.

Usage:
    python -m src.aggregate        # runs automatically after python -m src.transform

Configuration: config/aggregation.json
Job run id: taken from the ETL_JOB_RUN_ID environment variable when set (e.g.
an Airflow / AWS Glue run id), otherwise generated.
"""

import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, LongType, StringType, TimestampType

from src.config_loader import load_json_config, resolve_config
from src.exceptions import (
    AggregationError,
    ColumnNotFoundError,
    ConfigurationError,
    DataSourceError,
    short_error,
)
from src.logging_config import configure_logging
from src.read_data import read_file
from src.Spark_Initializer import SparkInitializer

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "config/aggregation.json"
DEFAULT_JOB_NAME = "employee_analytics"
DEFAULT_ENVIRONMENT = "local"
JOB_RUN_ID_ENV_VAR = "ETL_JOB_RUN_ID"
OPEN_ENDED_TO_DATE = "9999-01-01"  # to_date of a department assignment that is still active
WRITE_MODES = ("overwrite", "append")
SORT_ORDERS = ("asc", "desc")

# Run metadata appended to every aggregation output (name, Spark type).
RUN_METADATA_SCHEMA = [
    ("job_name", StringType()),
    ("job_run_id", StringType()),
    ("job_run_date", DateType()),
    ("job_run_timestamp", TimestampType()),
    ("environment", StringType()),
    ("source_path", StringType()),
    ("source_record_count", LongType()),
]
RUN_METADATA_COLUMNS = [name for name, _ in RUN_METADATA_SCHEMA]


def _require_columns(df: DataFrame, columns, operation: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ColumnNotFoundError(
            f"{operation}: column(s) {missing} not found in the source data. Available columns: {df.columns}."
        )


def _sort_order(order: str, operation: str) -> str:
    normalized = order.strip().lower() if isinstance(order, str) else order
    if normalized not in SORT_ORDERS:
        raise ConfigurationError(
            f"{operation}: unsupported order {order!r}. Supported: {', '.join(SORT_ORDERS)}."
        )
    return normalized


# --------------------------------------------------------------------------- #
# Business aggregations
# --------------------------------------------------------------------------- #
def employees_by_gender(df: DataFrame) -> DataFrame:
    """Distinct employees per gender, ordered by gender.

    Output: gender, employee_count
    """
    _require_columns(df, ["gender", "emp_no"], "employees_by_gender")
    return (
        df.groupBy("gender")
        .agg(F.countDistinct("emp_no").alias("employee_count"))
        .orderBy(F.col("gender").asc_nulls_last())
    )


def employees_by_tenure(df: DataFrame, order: str = "desc") -> DataFrame:
    """Distinct employees per tenure_years, ordered by tenure_years (``order``: desc/asc).

    Output: tenure_years, employee_count
    """
    operation = "employees_by_tenure"
    _require_columns(df, ["tenure_years", "emp_no"], operation)
    tenure = F.col("tenure_years")
    sort = tenure.desc_nulls_last() if _sort_order(order, operation) == "desc" else tenure.asc_nulls_last()
    return df.groupBy("tenure_years").agg(F.countDistinct("emp_no").alias("employee_count")).orderBy(sort)


def employees_by_department(df: DataFrame, current_to_date: str = OPEN_ENDED_TO_DATE) -> DataFrame:
    """Distinct employees per department.

    * current_employee_count: employees whose assignment is still open
      (to_date == ``current_to_date``, 9999-01-01 in the employees database)
    * total_employee_count: every employee ever assigned to the department

    Ordered by current_employee_count (desc), then dept_no.
    Output: dept_no, current_employee_count, total_employee_count
    """
    operation = "employees_by_department"
    _require_columns(df, ["dept_no", "emp_no", "to_date"], operation)
    current_date_value = F.to_date(F.lit(current_to_date))
    if df.sparkSession.range(1).select(current_date_value).first()[0] is None:
        raise ConfigurationError(
            f"{operation}: 'current_to_date' must be a date in yyyy-MM-dd format, got {current_to_date!r}."
        )

    is_current = F.col("to_date") == current_date_value
    return (
        df.groupBy("dept_no")
        .agg(
            # countDistinct ignores NULL, so non-current rows are not counted.
            F.countDistinct(F.when(is_current, F.col("emp_no"))).alias("current_employee_count"),
            F.countDistinct("emp_no").alias("total_employee_count"),
        )
        .orderBy(F.col("current_employee_count").desc(), F.col("dept_no").asc_nulls_last())
    )


# name -> (function, allowed option keys)
AGGREGATIONS = {
    "employees_by_gender": (employees_by_gender, ()),
    "employees_by_tenure": (employees_by_tenure, ("order",)),
    "employees_by_department": (employees_by_department, ("current_to_date",)),
}


# --------------------------------------------------------------------------- #
# Job run metadata
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class JobRunContext:
    job_name: str
    job_run_id: str
    job_run_timestamp: datetime  # timezone-aware, UTC
    environment: str
    source_path: str
    source_record_count: int

    @property
    def job_run_date(self):
        return self.job_run_timestamp.date()


def new_job_run_id(job_name: str, run_timestamp: datetime) -> str:
    """Unique, sortable run id: <job_name>-<UTC timestamp>-<8 hex chars>."""
    return f"{job_name}-{run_timestamp:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def add_run_metadata(df: DataFrame, context: JobRunContext) -> DataFrame:
    """Append the job run metadata columns (see RUN_METADATA_SCHEMA) to ``df``."""
    clashes = [column for column in RUN_METADATA_COLUMNS if column in df.columns]
    if clashes:
        raise AggregationError(
            f"add_run_metadata: columns {clashes} already exist in the aggregation result."
        )

    values = {
        "job_name": context.job_name,
        "job_run_id": context.job_run_id,
        "job_run_date": context.job_run_date,
        "job_run_timestamp": context.job_run_timestamp,
        "environment": context.environment,
        "source_path": context.source_path,
        "source_record_count": context.source_record_count,
    }
    return df.select(
        "*", *[F.lit(values[name]).cast(data_type).alias(name) for name, data_type in RUN_METADATA_SCHEMA]
    )


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
def _normalize_path(path: str) -> str:
    return str(PurePosixPath(path.replace("\\", "/").rstrip("/")))


def load_aggregation_config(config_path: str) -> dict:
    """Read and validate config/aggregation.json; report all problems at once."""
    config = resolve_config(load_json_config(config_path, "aggregate"), source=f"aggregate: '{config_path}'")
    problems = []

    source = config.get("source")
    if not isinstance(source, dict) or not isinstance(source.get("path"), str) or not source["path"].strip():
        problems.append("'source.path' must be a non-empty string")

    write_mode = config.setdefault("write_mode", "overwrite")
    if write_mode not in WRITE_MODES:
        problems.append(f"'write_mode' must be one of {', '.join(WRITE_MODES)}, got {write_mode!r}")

    for key, default in (("job_name", DEFAULT_JOB_NAME), ("environment", DEFAULT_ENVIRONMENT)):
        value = config.setdefault(key, default)
        if not isinstance(value, str) or not value.strip():
            problems.append(f"'{key}' must be a non-empty string")

    aggregations = config.get("aggregations")
    if not isinstance(aggregations, dict) or not aggregations:
        problems.append("'aggregations' must be a non-empty object of <name>: {output_path, ...}")
        aggregations = {}

    seen_paths = {}
    source_path = _normalize_path(source["path"]) if isinstance(source, dict) and source.get("path") else None
    for name, spec in aggregations.items():
        if name not in AGGREGATIONS:
            problems.append(f"unknown aggregation '{name}' (supported: {', '.join(AGGREGATIONS)})")
            continue
        if not isinstance(spec, dict):
            problems.append(f"'{name}' must be an object")
            continue
        _, allowed_options = AGGREGATIONS[name]
        unknown = sorted(set(spec) - {"output_path", "enabled", *allowed_options})
        if unknown:
            problems.append(f"'{name}' has unknown key(s) {unknown}")
        if not isinstance(spec.get("enabled", True), bool):
            problems.append(f"'{name}.enabled' must be true or false")
        output_path = spec.get("output_path")
        if not isinstance(output_path, str) or not output_path.strip():
            problems.append(f"'{name}.output_path' must be a non-empty string")
            continue
        normalized = _normalize_path(output_path)
        if normalized == source_path:
            problems.append(f"'{name}.output_path' must not be the source path")
        elif normalized in seen_paths:
            problems.append(f"'{name}.output_path' is the same as '{seen_paths[normalized]}.output_path'")
        seen_paths[normalized] = name

    if problems:
        raise ConfigurationError(
            f"aggregate: invalid configuration '{config_path}': " + "; ".join(problems) + "."
        )
    return config


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class AggregationOutput:
    name: str
    output_path: str
    row_count: int


def write_aggregation(df: DataFrame, output_path: str, write_mode: str) -> None:
    """Write a (small) aggregation as a single parquet file, keeping the row order."""
    try:
        df.coalesce(1).write.mode(write_mode).parquet(output_path)
    except Exception as err:
        raise AggregationError(
            f"write_aggregation: failed to write '{output_path}': {short_error(err)}"
        ) from err


def main(
    config_path: str = DEFAULT_CONFIG_PATH,
    spark=None,
    job_run_id: str = None,
    run_timestamp: datetime = None,
) -> dict:
    """Run every enabled aggregation on the transform output.

    Returns ``{name: AggregationOutput}``. ``job_run_id`` defaults to
    $ETL_JOB_RUN_ID, then to a generated id.
    """
    config = load_aggregation_config(config_path)
    run_timestamp = (run_timestamp or datetime.now(timezone.utc)).astimezone(timezone.utc)
    job_name = config["job_name"]
    job_run_id = job_run_id or os.getenv(JOB_RUN_ID_ENV_VAR) or new_job_run_id(job_name, run_timestamp)

    spark = spark or SparkInitializer.get_spark()
    source_cfg = config["source"]
    try:
        source_df = read_file(spark, source_cfg["path"], source_cfg.get("options"), source_cfg.get("format"))
    except DataSourceError as err:
        raise DataSourceError(f"aggregate: {err} Run 'python -m src.transform' first.") from err

    source_df = source_df.cache()
    try:
        context = JobRunContext(
            job_name=job_name,
            job_run_id=job_run_id,
            job_run_timestamp=run_timestamp,
            environment=config["environment"],
            source_path=source_cfg["path"],
            source_record_count=source_df.count(),
        )
        logger.info(
            "Aggregation run %s started on '%s' (%d rows).",
            job_run_id,
            context.source_path,
            context.source_record_count,
        )
        if context.source_record_count == 0:
            logger.warning("Source '%s' is empty; aggregations will be empty.", context.source_path)

        outputs = {}
        for name, spec in config["aggregations"].items():
            if not spec.get("enabled", True):
                logger.info("Aggregation '%s' is disabled; skipped.", name)
                continue
            function, allowed_options = AGGREGATIONS[name]
            options = {key: spec[key] for key in allowed_options if key in spec}
            try:
                result = function(source_df, **options)
            except (ColumnNotFoundError, ConfigurationError) as err:
                raise type(err)(f"Aggregation '{name}' failed: {err}") from err

            result = add_run_metadata(result, context).cache()
            write_aggregation(result, spec["output_path"], config["write_mode"])
            outputs[name] = AggregationOutput(name, spec["output_path"], result.count())
            result.unpersist()
            logger.info(
                "Aggregation '%s': %d rows written to '%s'.",
                name,
                outputs[name].row_count,
                spec["output_path"],
            )
        return outputs
    finally:
        source_df.unpersist()


if __name__ == "__main__":
    configure_logging()
    main()
