"""Unit tests for src/exceptions.py"""

import pytest

from src.exceptions import (
    ColumnNotFoundError,
    ConfigurationError,
    DataSourceError,
    DataValidationError,
    ETLError,
    ExtractError,
    LoadError,
    SparkSessionError,
    TransformationError,
    UnsupportedLoadTargetError,
    short_error,
)


@pytest.mark.parametrize(
    "error_class, parents",
    [
        (ConfigurationError, (ETLError, ValueError)),
        (ColumnNotFoundError, (TransformationError, ETLError, ValueError)),
        (DataValidationError, (TransformationError, ETLError)),
        (UnsupportedLoadTargetError, (ConfigurationError, ETLError, ValueError)),
        (SparkSessionError, (ETLError, RuntimeError)),
        (DataSourceError, (ETLError,)),
        (ExtractError, (ETLError,)),
        (LoadError, (ETLError,)),
    ],
)
def test_hierarchy(error_class, parents):
    error = error_class("message")

    for parent in parents:
        assert isinstance(error, parent)
    assert str(error) == "message"


class TestShortError:
    def test_first_line_of_multi_line_message(self):
        assert short_error(ValueError("first line\n  stack trace line")) == "first line"

    def test_empty_message_uses_class_name(self):
        assert short_error(KeyError()) == "KeyError"

    @pytest.mark.spark
    def test_spark_job_failure_reports_java_root_cause(self, spark):
        from pyspark.sql import functions as F

        df = spark.createDataFrame([("2020-1-5",)], "d string")
        with pytest.raises(Exception) as exc:
            df.select(F.to_timestamp("d", "yyyy-MM-dd")).collect()

        message = short_error(exc.value)

        assert "An error occurred while calling" in str(exc.value)
        assert message == "DateTimeParseException: Text '2020-1-5' could not be parsed at index 5"
