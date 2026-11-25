"""
Exception hierarchy for the ETL pipeline.

Every error raised by the pipeline derives from :class:`ETLError`, so callers
(schedulers, notebooks, tests) can catch pipeline failures with a single
``except ETLError`` while still being able to react to a specific stage.

Message convention
------------------
``"<operation>: <what went wrong>. <context / how to fix>"``

The original low-level exception (Py4J, AnalysisException, OSError, ...) is
always chained with ``raise ... from err`` so the full root cause stays in
the traceback.

Some classes also inherit from a built-in (``ValueError``, ``RuntimeError``)
for backwards compatibility with code that caught the built-in before.
"""


class ETLError(Exception):
    """Base class for every error raised by the ETL pipeline."""


class ConfigurationError(ETLError, ValueError):
    """A configuration file or configuration value is missing or invalid."""


class DataSourceError(ETLError):
    """An input dataset could not be found or read."""


class ExtractError(ETLError):
    """Extracting data from a source system failed."""


class TransformationError(ETLError):
    """A transformation step could not be applied to the data."""


class ColumnNotFoundError(TransformationError, ValueError):
    """A transformation referenced a column that is not in the DataFrame."""


class DataValidationError(TransformationError):
    """Data values failed validation (e.g. dates that cannot be parsed)."""


class AggregationError(ETLError):
    """Building or writing an analytics aggregation failed."""


class LoadError(ETLError):
    """Writing data to a target system failed."""


class UnsupportedLoadTargetError(ConfigurationError):
    """The LOAD_TARGET environment variable names an unknown target."""


class SparkSessionError(ETLError, RuntimeError):
    """The SparkSession could not be created or configured."""


def _java_root_cause(err: BaseException):
    """For a Py4JJavaError return 'ExceptionClass: message' of the deepest Java cause."""
    java_exception = getattr(err, "java_exception", None)
    if java_exception is None:
        return None
    try:
        root = java_exception
        while root.getCause() is not None and root.getCause() != root:
            root = root.getCause()
        message = (root.getMessage() or "").strip().splitlines()
        name = root.getClass().getSimpleName()
        return f"{name}: {message[0]}" if message else name
    except Exception:  # noqa: BLE001 - the JVM may already be gone
        return None


def short_error(err: BaseException) -> str:
    """One-line, human readable summary of an exception.

    Py4J / Spark errors embed a full Java stack trace in ``str(err)`` and the
    first line is often only "An error occurred while calling o51.collect".
    For those the deepest Java cause is reported instead. The full trace stays
    available through exception chaining.
    """
    root_cause = _java_root_cause(err)
    if root_cause:
        return root_cause
    text = str(err).strip()
    first_line = text.splitlines()[0] if text else ""
    return first_line or type(err).__name__
