# Unit tests

## Run

```bash
pip install -r requirements-dev.txt
python -m pytest                      # full suite (~2 min, one local Spark session)
python -m pytest --cov                # with coverage report
python -m pytest -m "not spark"       # only tests that need no JVM (~1 s)
python -m pytest tests/unit/test_final_output.py   # only the final parquet checks
```

Run from the project root. **Java 8, 11 or 17 is required for pyspark 3.4.1**
(Java 21 fails with `NoSuchMethodException: java.nio.DirectByteBuffer.<init>`).

## Scope

The tests cover **how data is created and validated**. They use a local
SparkSession only and never connect to external systems.

Out of scope, by decision: MySQL / Postgres / Snowflake / S3 / Iceberg
connectivity, data extract and load (`src/extract.py`, `src/load.py`),
SparkSession creation (`src/Spark_Initializer.py`) and the Glue catalog
registration call in `utils/iceberg_table_registration.py` and the AWS Secrets
Manager lookup in `src/config_loader.py` (only its reference parsing is tested). These are omitted
from coverage in `pyproject.toml`.

## Layout

| File | Covers | Data |
|---|---|---|
| `conftest.py` | Spark session + shared fixtures | `data/extract/*.parquet` slice (emp_no 10001-10012) + mock DataFrames |
| `helpers.py` | paths, expected final schema, small helpers | |
| `unit/test_final_output.py` | `transform.main` end to end: the written `final.parquet` (schema, completeness, derived values, edge cases, no output on failure) | real slice + mock extract |
| `unit/test_aggregate.py` | gender / tenure / department aggregations, run metadata, `config/aggregation.json`, separate outputs, overwrite vs append | `final.parquet` from the sample pipeline run + mocks |
| `unit/test_transform.py` | every transformation, config validation, step dispatch | real slice + mocks |
| `unit/test_read_data.py` | format resolution, `read_file`, `config/read_data.json`, `main` | real parquet, temp JSON / CSV / ORC |
| `unit/test_config_loader.py` | JSON loading, `${env:}` / `${file:}` references, plain-text secret policy, custom providers | temp files, env vars |
| `unit/test_project_configs.py` | shipped `config/*.json`: no plain-text secrets, documented variables, literal split | config files |
| `unit/test_exceptions.py` | error hierarchy, `short_error` | |
| `unit/utils/test_json_mapper.py` | `map_json_with_custom_mapping` | dicts |
| `unit/utils/test_json_mapper_ng.py` | `transform_with_jsonpath` | dicts |
| `unit/utils/test_iceberg_table_registration.py` | Spark conf, SQL building, CSV validation, CLI | temp CSV |

## Conventions

* Test names describe behaviour; one behaviour per test.
* Tests that document a deliberate limitation say `KNOWN LIMITATION` in the docstring.
* `@pytest.mark.spark` marks tests that need a JVM.
