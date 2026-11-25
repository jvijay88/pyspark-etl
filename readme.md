![cover_image](https://github.com/jvijay88/pyspark-etl/blob/master/assets/github-cover.png)

## Overview

- This project is a ETL (Extract, Transform, Load) project that demonstrates the process of extracting data from an employee database using Apache Spark, performing various transformations, and finally loading the processed data into a MySQL database Postgres, Snowflake, AWS S3 and S3 Iceberg based on config json provided. The entire process is implemented in Python, and JSON is used for configuration to enhance modularity and maintainability.

## Table of Contents

- [Introduction](#overview)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Transformation](#transformation)
- [Analytics Aggregations](#analytics-aggregations)
- [Error Handling](#error-handling)
- [Testing](#testing)
- [Technologies Used](#technologies-used)
- [Contributing](#contributing)
- [License](#license)

## Installation

1.  Clone this repository to your local machine:

        git clone https://github.com/your-username/pyspark-etl.git
        cd pyspark-etl

2.  Install the required dependencies (Java 8, 11 or 17 is required by pyspark 3.4.1):

        pip install -r requirements.txt

3.  Setup MySQL database and adjust the connection configuration in `config/`

4.  Provide credentials as environment variables (see [Secrets](#secrets)):

        cp config/secrets.env.example config/secrets.env   # git-ignored, fill in
        set -a; . config/secrets.env; set +a

## Usage

Run each step from the project root:

        python -m src.extract      # MySQL -> data/extract/<table>.parquet
        python -m src.transform    # data/extract -> data/transform/final.parquet,
                                   # then the analytics aggregations (data/analytics/)
        python -m src.aggregate    # re-run only the aggregations on final.parquet
        python -m src.load         # final.parquet -> LOAD_TARGET

        python -m src.read_data    # print schema and preview (config/read_data.json)

`src.load` writes to the target named by the `LOAD_TARGET` environment variable
(case-insensitive): `mysql` (default), `postgres`, `snowflake`, `s3`, `s3_iceberg`.
An unknown value is printed, logged as an error and stops the run.

        LOAD_TARGET=postgres python -m src.load

Set `LOG_LEVEL` (DEBUG, INFO, WARNING, ERROR) to control logging; default INFO.

## Configuration

The project utilizes JSON configuration files to specify the Spark ETL process. This allows for easy customization without modifying the code directly.

- `config/extract.json` - MySQL connection, tables to extract, output folder
- `config/transformation.json` - ordered list of transformation steps
- `config/config.json` - one section per load target
- `config/aggregation.json` - analytics aggregations: source, output path per aggregation, write mode
- `config/read_data.json` - dataset inspected by `src.read_data`: `path`, optional
  `format` (parquet, json, csv, orc) and reader `options`. `format` is required
  when the path has no extension, e.g. `s3a://bucket/output/employees_parquet/`.

All configuration is validated before Spark starts; missing keys or unknown
steps are reported together in one error.

### Secrets

Credentials are not stored in the config files. Any string value can contain a
reference that is resolved at run time:

| Reference | Resolved from |
|---|---|
| `${env:MYSQL_PASSWORD}` | environment variable |
| `${file:/run/secrets/mysql_password}` | file content, e.g. Docker / Kubernetes secrets |
| `${aws-sm:prod/etl/mysql#password}` | AWS Secrets Manager (JSON key after `#` is optional; needs `boto3`) |

- References can be part of a longer value: `"${env:SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"`.
- Keys that look sensitive (`password`, `secret`, `token`, `api_key`, ...) must use a
  reference; a literal value stops the run with an error. For local development
  only, set `"allow_plaintext_secrets": true` at the top level of the config file.
- `src.load` resolves only the section of the selected `LOAD_TARGET`.
- Other stores (Azure Key Vault, GCP Secret Manager, Vault) can be added with
  `src.config_loader.register_secret_provider`.
- The variables used by the shipped configs are listed in `config/secrets.env.example`.

## Transformation

Steps in `config/transformation.json` run in the listed order. The primary
table is `employees` (override with a top-level `"primary_table"` key).

| transformation | required keys | optional keys |
|---|---|---|
| `change_date_format` | `source_column`, `target_format` | `source_format` |
| `split` | `source_column`, `target_columns` (any number), `separator` | `separator_type`: `literal` (default) / `regex` |
| `normalize_strings` | `source_columns` | `case_type`: `upper` / `lower`, any letter case (default `upper`) |
| `concatenate_columns` | `source_columns`, `target_column` | `separator` (default `" "`) |
| `fill_missing_values` | `mapping_dict` `{column: value}` | |
| `drop_column` | `source_columns` | |
| `join_tables` | `source_tables` `[<primary table>, <table to join>]`, `source_column`, `join_type` | |
| `calculate_tenure` | `start_date_column`, `target_column` | `date_format` |
| `filter_rows` | `condition_expr` (Spark SQL) | |

Notes:

- Date steps keep track of formats applied earlier, so `calculate_tenure` works
  before or after `change_date_format`. Values that cannot be parsed raise an
  error instead of silently becoming NULL.
- `fill_missing_values` replaces NULL values, and for string columns also
  empty / whitespace-only values. The fill value must match the column type.
- `split` matches the separator as plain text by default (`"."` splits on dots).
  Set `"separator_type": "regex"` to use a Java regular expression such as `"\\s+"`.
- `join_tables` reads the table named in `source_tables` from the extract
  folder (e.g. `dept_emp`, `departments`).

## Analytics Aggregations

`src/aggregate.py` reads the transform output `data/transform/final.parquet` and
writes one parquet output per aggregation (single file, git-ignored folder
`data/analytics/`). It runs automatically after `python -m src.transform`.

`final.parquet` has one row per department assignment, so an employee who moved
department appears more than once; all counts are distinct counts of `emp_no`.

| Output | Columns | Order |
|---|---|---|
| `data/analytics/employees_by_gender.parquet` | `gender`, `employee_count` | gender |
| `data/analytics/employees_by_tenure.parquet` | `tenure_years`, `employee_count` | tenure_years descending (`"order": "asc"` to reverse) |
| `data/analytics/employees_by_department.parquet` | `dept_no`, `current_employee_count` (assignment still open: `to_date = 9999-01-01`), `total_employee_count` (ever assigned) | current_employee_count descending, then dept_no |

Every output row also carries the job run metadata:

| Column | Type | Value |
|---|---|---|
| `job_name` | string | `job_name` in config (default `employee_analytics`) |
| `job_run_id` | string | `$ETL_JOB_RUN_ID` (e.g. Airflow / Glue run id) or generated `<job_name>-<UTC time>-<8 hex>` |
| `job_run_date` | date | UTC date of the run |
| `job_run_timestamp` | timestamp | UTC start time of the run |
| `environment` | string | `environment` in config (can be `"${env:ETL_ENV}"`) |
| `source_path` | string | path of the aggregated final.parquet |
| `source_record_count` | long | rows in final.parquet at run time |

`"write_mode": "overwrite"` (default) keeps only the latest run; `"append"` keeps
every run, so history can be queried by `job_run_id` / `job_run_date`.
An aggregation can be switched off with `"enabled": false`. Output paths must be
different from each other and from the source; the config is validated before
any data is read.

## Error Handling

All pipeline errors derive from `src.exceptions.ETLError`:

| Error | Raised when |
|---|---|
| `ConfigurationError` | a config file / key / value is missing or invalid |
| `UnsupportedLoadTargetError` | `LOAD_TARGET` is not a supported target |
| `DataSourceError` | an input table cannot be read |
| `ColumnNotFoundError` | a step references a column that does not exist |
| `DataValidationError` | data values fail validation (e.g. unparseable dates) |
| `TransformationError` | a transformation or the final write fails |
| `AggregationError` | an analytics aggregation cannot be built or written |
| `ExtractError` / `LoadError` | reading from / writing to an external system fails |
| `SparkSessionError` | the SparkSession cannot be created or configured |

Messages follow the format `<operation>: <what went wrong>. <context / fix>`,
and the original exception is chained so the full root cause stays available.
Failed steps are reported as `Transformation step <n> ('<name>') failed: ...`.

## Testing

        pip install -r requirements-dev.txt
        python -m pytest                       # all unit tests
        python -m pytest --cov                 # with coverage
        ruff check src utils tests             # lint

See [tests/README.md](tests/README.md) for details.

## Technologies Used

- Python: Programming language used for implementing the Spark ETL process.
- Apache Spark: Distributed data processing framework used for scalable ETL operations.
- JSON: Configuration files are written in JSON format for easy customization.
- MySQL: The final processed data is loaded into a MySQL database.
- Snowflake : Option to load the final processed data is loaded into a Snowflake.
- Parquet: The extracted data is saved in Parquet file format, which is optimized for performance and space efficiency.
- Postgres : Option to load data into postgres
- S3 Parquet : Option to load final data as parquet file and move to s3 bucket
- Iceberg : Option to load final data as Iceberg table to s3 bucket

## Contributing

I welcome contributions from the community! If you find any issues or have suggestions for improvements, please feel free to open an issue or submit a pull request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---
