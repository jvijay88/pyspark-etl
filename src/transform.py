"""
Module Name: transform.py
Description: Applies the transformations listed in config/transformation.json to
             the extracted parquet tables and writes the result to
             data/transform/final.parquet.

Usage:
    python -m src.transform      # then runs the aggregations in src/aggregate.py

Config format (config/transformation.json):
    {
      "primary_table": "employees",          # optional, default "employees"
      "transformations": [ {"transformation": "<name>", ...}, ... ]
    }

Steps run in the order listed. Every step is validated before any data is
processed. See STEP_REGISTRY for the supported names and their required keys.
"""

import logging

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegralType,
    StringType,
    TimestampType,
)

from src.config_loader import load_json_config
from src.exceptions import (
    ColumnNotFoundError,
    ConfigurationError,
    DataSourceError,
    DataValidationError,
    ETLError,
    TransformationError,
    short_error,
)
from src.logging_config import configure_logging
from src.Spark_Initializer import SparkInitializer

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "config/transformation.json"
DEFAULT_EXTRACT_FOLDER = "data/extract"
DEFAULT_OUTPUT_PATH = "data/transform/final.parquet"
DEFAULT_PRIMARY_TABLE = "employees"

CASE_TYPES = ("upper", "lower")
SEPARATOR_TYPES = ("literal", "regex")
JOIN_TYPES = (
    "inner",
    "cross",
    "outer",
    "full",
    "fullouter",
    "full_outer",
    "left",
    "leftouter",
    "left_outer",
    "right",
    "rightouter",
    "right_outer",
    "semi",
    "leftsemi",
    "left_semi",
    "anti",
    "leftanti",
    "left_anti",
)
# Number of offending values quoted in data validation error messages.
ERROR_SAMPLE_SIZE = 5


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _require_columns(df: DataFrame, columns, operation: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ColumnNotFoundError(
            f"{operation}: column(s) {missing} not found in the DataFrame. Available columns: {df.columns}."
        )


def _require_new_columns(df: DataFrame, columns, operation: str) -> None:
    existing = [column for column in columns if column in df.columns]
    if existing:
        raise ConfigurationError(
            f"{operation}: target column(s) {existing} already exist in the DataFrame. "
            "Choose new column names or drop the existing columns first."
        )


def _require_non_empty_list(value, name: str, operation: str) -> None:
    if not isinstance(value, (list, tuple)) or not value:
        raise ConfigurationError(f"{operation}: '{name}' must be a non-empty list, got {value!r}.")


def _to_timestamp_checked(df: DataFrame, column: str, operation: str, date_format=None) -> Column:
    """Return ``column`` as a timestamp expression.

    Date/timestamp columns are used as-is. String columns are parsed with
    ``date_format`` (ISO 'yyyy-MM-dd' when not given). If any non-null value
    cannot be parsed a DataValidationError is raised instead of silently
    producing NULLs.
    """
    data_type = df.schema[column].dataType
    if isinstance(data_type, (DateType, TimestampType)):
        return F.col(column)

    if not isinstance(data_type, StringType):
        raise TransformationError(
            f"{operation}: column '{column}' has type {data_type.simpleString()}; "
            "expected a date, timestamp or string column."
        )

    parsed = F.to_timestamp(F.col(column), date_format) if date_format else F.to_timestamp(F.col(column))
    hint = (
        f"with format '{date_format or 'yyyy-MM-dd'}'. Set 'date_format' (or "
        "'source_format') in the step to the pattern of the data"
    )
    try:
        bad_values = [
            row[0]
            for row in df.filter(F.col(column).isNotNull() & parsed.isNull())
            .select(column)
            .limit(ERROR_SAMPLE_SIZE)
            .collect()
        ]
    except Exception as err:
        raise DataValidationError(
            f"{operation}: column '{column}' could not be parsed as dates {hint}: {short_error(err)}"
        ) from err
    if bad_values:
        raise DataValidationError(
            f"{operation}: column '{column}' contains values that cannot be parsed as dates "
            f"{hint}. Examples: {bad_values}."
        )
    return parsed


# --------------------------------------------------------------------------- #
# Transformations
# --------------------------------------------------------------------------- #
def change_date_format(
    df: DataFrame, source_column: str, target_format: str, source_format: str = None
) -> DataFrame:
    """Replace ``source_column`` with its value formatted as ``target_format`` (string).

    ``source_format`` is only needed when the column is a string that is not
    ISO formatted (e.g. it was already reformatted by an earlier step).
    """
    operation = "change_date_format"
    _require_columns(df, [source_column], operation)
    if not isinstance(target_format, str) or not target_format.strip():
        raise ConfigurationError(
            f"{operation}: 'target_format' must be a non-empty date pattern such as "
            f"'dd-MM-yyyy', got {target_format!r}."
        )

    parsed = _to_timestamp_checked(df, source_column, operation, source_format)
    return df.select(
        *[
            F.date_format(parsed, target_format).alias(column) if column == source_column else F.col(column)
            for column in df.columns
        ]
    )


def _literal_pattern(separator: str) -> str:
    """Regex matching ``separator`` literally.

    Every non-alphanumeric character is escaped with a backslash, which Java
    regex always treats as the literal character.
    """
    return "".join(char if char.isalnum() else "\\" + char for char in separator)


def split_column(
    df: DataFrame,
    source_column: str,
    target_columns,
    separator: str,
    separator_type: str = "literal",
) -> DataFrame:
    """Split ``source_column`` by ``separator`` into one new column per target name.

    * Works for any number of target columns. Missing parts become NULL and
      parts beyond the number of target columns are ignored.
    * ``separator_type`` = "literal" (default): the separator is matched as
      plain text, so "." or "|" split on that character.
      ``separator_type`` = "regex": the separator is a Java regular
      expression, e.g. "\\s+" for any run of whitespace.
    """
    operation = "split_column"
    _require_columns(df, [source_column], operation)
    _require_non_empty_list(target_columns, "target_columns", operation)
    if not all(isinstance(name, str) and name.strip() for name in target_columns):
        raise ConfigurationError(
            f"{operation}: 'target_columns' must contain non-empty column names, got {list(target_columns)}."
        )
    if len(set(target_columns)) != len(target_columns):
        raise ConfigurationError(
            f"{operation}: 'target_columns' contains duplicate names: {list(target_columns)}."
        )
    _require_new_columns(df, target_columns, operation)
    if not isinstance(separator, str) or separator == "":
        raise ConfigurationError(f"{operation}: 'separator' must be a non-empty string, got {separator!r}.")

    mode = separator_type.strip().lower() if isinstance(separator_type, str) else separator_type
    if mode not in SEPARATOR_TYPES:
        raise ConfigurationError(
            f"{operation}: unsupported separator_type {separator_type!r}. "
            f"Supported: {', '.join(SEPARATOR_TYPES)}."
        )
    if mode == "literal":
        pattern = _literal_pattern(separator)
    else:
        pattern = separator
        try:  # compile the regex once so a bad pattern fails here, not mid-job
            df.sparkSession.range(1).select(F.split(F.lit("probe"), pattern)).collect()
        except Exception as err:
            raise ConfigurationError(
                f"{operation}: separator {separator!r} is not a valid regular expression: {short_error(err)}"
            ) from err

    parts = F.split(F.col(source_column).cast("string"), pattern)
    return df.select("*", *[parts.getItem(index).alias(name) for index, name in enumerate(target_columns)])


def concatenate_columns(df: DataFrame, source_columns, target_column: str, separator: str = " ") -> DataFrame:
    """Add ``target_column`` = source column values joined by ``separator``.

    NULL inputs are skipped, so if every input is NULL the result is an empty
    string. Use fill_missing_values afterwards to replace it.
    """
    operation = "concatenate_columns"
    _require_non_empty_list(source_columns, "source_columns", operation)
    _require_columns(df, source_columns, operation)
    _require_new_columns(df, [target_column], operation)

    return df.select(
        "*", F.concat_ws(separator, *[F.col(column) for column in source_columns]).alias(target_column)
    )


def drop_column(df: DataFrame, source_columns) -> DataFrame:
    """Drop the listed columns. All of them must exist."""
    operation = "drop_column"
    if not isinstance(source_columns, (list, tuple)):
        raise ConfigurationError(f"{operation}: 'source_columns' must be a list, got {source_columns!r}.")
    _require_columns(df, source_columns, operation)
    return df.select([column for column in df.columns if column not in source_columns])


def join_tables(df1: DataFrame, df2: DataFrame, source_column: str, join_type: str = "inner") -> DataFrame:
    """Join ``df1`` with ``df2`` on ``source_column`` (which appears once in the result)."""
    operation = "join_tables"
    normalized_join_type = join_type.strip().lower() if isinstance(join_type, str) else join_type
    if normalized_join_type not in JOIN_TYPES:
        raise ConfigurationError(
            f"{operation}: unsupported join_type {join_type!r}. Supported: {', '.join(JOIN_TYPES)}."
        )
    if source_column not in df1.columns:
        raise ColumnNotFoundError(
            f"{operation}: join column '{source_column}' not found in the left table. "
            f"Available columns: {df1.columns}."
        )
    if source_column not in df2.columns:
        raise ColumnNotFoundError(
            f"{operation}: join column '{source_column}' not found in the right table. "
            f"Available columns: {df2.columns}."
        )

    overlapping = (set(df1.columns) & set(df2.columns)) - {source_column}
    if overlapping:
        logger.warning(
            "%s: columns %s exist in both tables; the result will contain duplicate names.",
            operation,
            sorted(overlapping),
        )
    return df1.join(df2, source_column, normalized_join_type)


def filter_rows(df: DataFrame, expr) -> DataFrame:
    """Keep the rows matching ``expr`` (SQL string or Column)."""
    try:
        return df.filter(expr)
    except Exception as err:
        raise TransformationError(
            f"filter_rows: invalid condition {str(expr)!r}: {short_error(err)}"
        ) from err


def calculate_tenure(
    df: DataFrame, start_date_column: str, target_column: str, date_format: str = None
) -> DataFrame:
    """Add ``target_column`` = full years between ``start_date_column`` and today.

    String columns are parsed with ``date_format`` (ISO by default). Values
    that cannot be parsed raise DataValidationError instead of becoming NULL.
    """
    operation = "calculate_tenure"
    _require_columns(df, [start_date_column], operation)
    start = _to_timestamp_checked(df, start_date_column, operation, date_format)
    return df.withColumn(target_column, F.floor(F.datediff(F.current_date(), start) / 365.25))


def _validated_fill_value(df: DataFrame, column: str, value):
    """Return ``value`` converted to the type of ``column`` or raise ConfigurationError."""
    operation = "fill_missing_values"
    data_type = df.schema[column].dataType

    if value is None:
        raise ConfigurationError(f"{operation}: fill value for column '{column}' must not be null.")
    if isinstance(data_type, StringType):
        return str(value)
    if isinstance(value, bool) and not isinstance(data_type, BooleanType):
        raise ConfigurationError(
            f"{operation}: boolean fill value {value!r} is not valid for column "
            f"'{column}' of type {data_type.simpleString()}."
        )
    if isinstance(data_type, IntegralType) and isinstance(value, float) and not value.is_integer():
        raise ConfigurationError(
            f"{operation}: fill value {value!r} for integer column '{column}' would lose "
            "its decimal part. Use a whole number."
        )

    converted = df.sparkSession.range(1).select(F.lit(value).cast(data_type)).first()[0]
    if converted is None:
        raise ConfigurationError(
            f"{operation}: fill value {value!r} cannot be converted to "
            f"{data_type.simpleString()} for column '{column}'."
        )
    return converted


def fill_missing_values(df: DataFrame, mapping_dict: dict) -> DataFrame:
    """Replace missing values using ``{column: fill_value}``.

    * String columns: NULL, empty and whitespace-only (spaces, tabs, newlines)
      values are replaced.
    * Other columns: NULL values are replaced. The fill value is converted to
      the column type first; values that cannot be converted (e.g. 'abc' for
      an integer column) raise ConfigurationError instead of being ignored.
    """
    operation = "fill_missing_values"
    if not isinstance(mapping_dict, dict) or not mapping_dict:
        raise ConfigurationError(
            f"{operation}: 'mapping_dict' must be a non-empty object of column -> value, "
            f"got {mapping_dict!r}."
        )
    _require_columns(df, list(mapping_dict), operation)

    replacements = {
        column: _validated_fill_value(df, column, value) for column, value in mapping_dict.items()
    }

    output = []
    for column in df.columns:
        if column not in replacements:
            output.append(F.col(column))
            continue
        data_type = df.schema[column].dataType
        fill = F.lit(replacements[column]).cast(data_type)
        if isinstance(data_type, StringType):
            # \s also covers tabs/newlines; F.trim only removes spaces.
            is_missing = F.col(column).isNull() | F.col(column).rlike(r"^\s*$")
            output.append(F.when(is_missing, fill).otherwise(F.col(column)).alias(column))
        else:
            output.append(F.coalesce(F.col(column), fill).alias(column))
    return df.select(*output)


def normalize_strings(df: DataFrame, columns, case_type: str = "upper") -> DataFrame:
    """Convert string ``columns`` to upper or lower case.

    ``case_type`` is case-insensitive ('Upper', ' LOWER ' are accepted).
    Unknown values raise ConfigurationError.
    """
    operation = "normalize_strings"
    normalized_case = case_type.strip().lower() if isinstance(case_type, str) else case_type
    if normalized_case not in CASE_TYPES:
        raise ConfigurationError(
            f"{operation}: unsupported case_type {case_type!r}. Supported: {', '.join(CASE_TYPES)}."
        )
    _require_non_empty_list(columns, "source_columns", operation)
    _require_columns(df, columns, operation)
    non_string = [
        f"{column} ({df.schema[column].dataType.simpleString()})"
        for column in columns
        if not isinstance(df.schema[column].dataType, StringType)
    ]
    if non_string:
        raise TransformationError(f"{operation}: only string columns can be normalized, got {non_string}.")

    case_function = F.upper if normalized_case == "upper" else F.lower
    return df.select(
        *[
            case_function(F.col(column)).alias(column) if column in columns else F.col(column)
            for column in df.columns
        ]
    )


# --------------------------------------------------------------------------- #
# Config-driven pipeline
# --------------------------------------------------------------------------- #
class _PipelineContext:
    """State shared between steps of one apply_transformations run."""

    def __init__(self, tables, primary_table):
        self.tables = dict(tables or {})
        self.primary_table = primary_table
        # column -> pattern applied by change_date_format, so later steps
        # (calculate_tenure, another change_date_format) can still parse it.
        self.date_formats = {}


def _step_change_date_format(df, step, ctx):
    column = step["source_column"]
    source_format = step.get("source_format") or ctx.date_formats.get(column)
    result = change_date_format(df, column, step["target_format"], source_format)
    ctx.date_formats[column] = step["target_format"]
    return result


def _step_split(df, step, ctx):
    return split_column(
        df,
        step["source_column"],
        step["target_columns"],
        step["separator"],
        step.get("separator_type", "literal"),
    )


def _step_concatenate(df, step, ctx):
    return concatenate_columns(df, step["source_columns"], step["target_column"], step.get("separator", " "))


def _step_drop(df, step, ctx):
    return drop_column(df, step["source_columns"])


def _step_join(df, step, ctx):
    source_tables = step["source_tables"]
    if not isinstance(source_tables, (list, tuple)) or len(source_tables) != 2:
        raise ConfigurationError(
            "join_tables: 'source_tables' must list exactly two tables "
            f"[<left>, <right>], got {source_tables!r}."
        )
    left, right = source_tables
    if left != ctx.primary_table:
        raise ConfigurationError(
            f"join_tables: the left table must be the table being transformed "
            f"('{ctx.primary_table}'), got '{left}'."
        )
    if right not in ctx.tables:
        raise DataSourceError(
            f"join_tables: table '{right}' is not available. Loaded tables: {sorted(ctx.tables)}."
        )
    return join_tables(df, ctx.tables[right], step["source_column"], step["join_type"])


def _step_tenure(df, step, ctx):
    column = step["start_date_column"]
    date_format = step.get("date_format") or ctx.date_formats.get(column)
    return calculate_tenure(df, column, step["target_column"], date_format)


def _step_filter(df, step, ctx):
    return filter_rows(df, step["condition_expr"])


def _step_fill(df, step, ctx):
    return fill_missing_values(df, step["mapping_dict"])


def _step_normalize(df, step, ctx):
    return normalize_strings(df, step["source_columns"], step.get("case_type", "upper"))


# name -> (required keys, handler)
STEP_REGISTRY = {
    "change_date_format": (("source_column", "target_format"), _step_change_date_format),
    "split": (("source_column", "target_columns", "separator"), _step_split),
    "concatenate_columns": (("source_columns", "target_column"), _step_concatenate),
    "drop_column": (("source_columns",), _step_drop),
    "join_tables": (("source_tables", "source_column", "join_type"), _step_join),
    "calculate_tenure": (("start_date_column", "target_column"), _step_tenure),
    "filter_rows": (("condition_expr",), _step_filter),
    "fill_missing_values": (("mapping_dict",), _step_fill),
    "normalize_strings": (("source_columns",), _step_normalize),
}


def validate_transformations(transformations) -> None:
    """Check every step before any data is touched; report all problems at once."""
    if not isinstance(transformations, list):
        raise ConfigurationError(
            f"'transformations' must be a list of steps, got {type(transformations).__name__}."
        )

    problems = []
    for index, step in enumerate(transformations, start=1):
        if not isinstance(step, dict) or "transformation" not in step:
            problems.append(f"step {index}: must be an object with a 'transformation' key")
            continue
        name = step["transformation"]
        if name not in STEP_REGISTRY:
            problems.append(
                f"step {index}: unknown transformation '{name}' (supported: {', '.join(STEP_REGISTRY)})"
            )
            continue
        required_keys, _ = STEP_REGISTRY[name]
        missing = [key for key in required_keys if key not in step]
        if missing:
            problems.append(f"step {index} '{name}': missing required key(s) {missing}")

    if problems:
        raise ConfigurationError("Invalid transformation configuration: " + "; ".join(problems) + ".")


def join_table_names(transformations) -> list:
    """Names of the right-hand tables referenced by join_tables steps."""
    names = []
    for step in transformations:
        if step.get("transformation") == "join_tables":
            tables = step.get("source_tables") or []
            if len(tables) == 2 and tables[1] not in names:
                names.append(tables[1])
    return names


def apply_transformations(
    df: DataFrame, transformations, tables=None, primary_table: str = DEFAULT_PRIMARY_TABLE
) -> DataFrame:
    """Apply the config-driven ``transformations`` in order to ``df``.

    ``tables`` maps table names to DataFrames and provides the right-hand side
    of join_tables steps (``source_tables: [<primary_table>, <right table>]``).
    """
    validate_transformations(transformations)
    ctx = _PipelineContext(tables, primary_table)

    for index, step in enumerate(transformations, start=1):
        name = step["transformation"]
        _, handler = STEP_REGISTRY[name]
        logger.info("Transformation step %d/%d: %s", index, len(transformations), name)
        try:
            df = handler(df, step, ctx)
        except ETLError as err:
            raise type(err)(f"Transformation step {index} ('{name}') failed: {err}") from err
        except Exception as err:
            raise TransformationError(
                f"Transformation step {index} ('{name}') failed: {short_error(err)}"
            ) from err
    return df


def load_transformation_config(json_path: str) -> dict:
    """Read and validate the transformation config file."""
    config = load_json_config(json_path, "transform")
    if "transformations" not in config:
        raise ConfigurationError(
            f"transform: configuration '{json_path}' must contain a 'transformations' list."
        )
    validate_transformations(config["transformations"])
    return config


def read_extract_table(spark, extract_folder_path: str, table_name: str) -> DataFrame:
    path = f"{extract_folder_path}/{table_name}.parquet"
    try:
        return spark.read.parquet(path)
    except Exception as err:
        raise DataSourceError(
            f"Input table '{table_name}' could not be read from '{path}': {short_error(err)}. "
            "Run the extract step first or check 'extract_folder_path'."
        ) from err


def main(
    json_path: str = DEFAULT_CONFIG_PATH,
    extract_folder_path: str = DEFAULT_EXTRACT_FOLDER,
    output_path: str = DEFAULT_OUTPUT_PATH,
    spark=None,
) -> None:
    config = load_transformation_config(json_path)
    transformations = config["transformations"]
    primary_table = config.get("primary_table", DEFAULT_PRIMARY_TABLE)

    spark = spark or SparkInitializer.get_spark()

    tables = {
        name: read_extract_table(spark, extract_folder_path, name)
        for name in [primary_table, *join_table_names(transformations)]
    }
    # The primary table is reused by several steps.
    primary_df = tables[primary_table].cache()

    result_df = apply_transformations(primary_df, transformations, tables, primary_table)
    result_df.show()

    try:
        result_df.write.mode("overwrite").parquet(output_path)
    except Exception as err:
        raise TransformationError(
            f"Failed to write transformed data to '{output_path}': {short_error(err)}"
        ) from err
    logger.info("Transformed data written to '%s'.", output_path)


if __name__ == "__main__":
    from src import aggregate

    configure_logging()
    main()
    # Analytics aggregations are built from the final parquet written above.
    aggregate.main()
