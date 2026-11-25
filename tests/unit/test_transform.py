"""Unit tests for the transformation functions in src/transform.py

Data used:
* ``employees_df`` / ``dept_emp_df`` / ``departments_df`` -> real slice of
  data/extract/*.parquet (emp_no 10001..10012, see tests/conftest.py)
* ``mock_employees_df`` / ``make_df`` -> in-memory mock data for edge cases
  (NULLs, blanks, unmatched keys) that the real extract does not contain.

The end-to-end run that produces final.parquet is tested in
test_final_output.py.
"""

import datetime as dt

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, LongType, StringType

from src.exceptions import (
    ColumnNotFoundError,
    ConfigurationError,
    DataSourceError,
    DataValidationError,
    ETLError,
    TransformationError,
)
from src.transform import (
    STEP_REGISTRY,
    apply_transformations,
    calculate_tenure,
    change_date_format,
    concatenate_columns,
    drop_column,
    fill_missing_values,
    filter_rows,
    join_table_names,
    join_tables,
    load_transformation_config,
    normalize_strings,
    split_column,
    validate_transformations,
)
from tests.helpers import EMPLOYEE_COLUMNS, expected_tenure, row_by_emp

pytestmark = pytest.mark.spark


# --------------------------------------------------------------------------- #
# change_date_format
# --------------------------------------------------------------------------- #
class TestChangeDateFormat:
    def test_formats_date_column_with_target_pattern(self, employees_df):
        result = change_date_format(employees_df, "hire_date", "dd-MM-yyyy")

        assert row_by_emp(result, 10001).hire_date == "26-06-1986"
        assert row_by_emp(result, 10012).hire_date == "18-12-1992"

    def test_result_is_string_and_column_order_kept(self, employees_df):
        result = change_date_format(employees_df, "hire_date", "yyyy/MM/dd")

        assert result.columns == employees_df.columns
        assert isinstance(result.schema["hire_date"].dataType, StringType)
        assert row_by_emp(result, 10001).hire_date == "1986/06/26"

    def test_other_columns_and_row_count_unchanged(self, employees_df):
        result = change_date_format(employees_df, "hire_date", "dd-MM-yyyy")

        assert result.count() == employees_df.count()
        original, changed = row_by_emp(employees_df, 10001), row_by_emp(result, 10001)
        for column in ["emp_no", "birth_date", "first_name", "last_name", "gender"]:
            assert changed[column] == original[column]

    def test_null_dates_stay_null(self, mock_employees_df):
        result = change_date_format(mock_employees_df, "hire_date", "dd-MM-yyyy")

        assert row_by_emp(result, 3).hire_date is None
        assert row_by_emp(result, 1).hire_date == "01-03-2010"

    def test_iso_string_column_is_parsed(self, make_df):
        df = make_df([(1, "2020-01-15")], "emp_no int, d string")

        assert change_date_format(df, "d", "dd/MM/yyyy").first().d == "15/01/2020"

    def test_non_iso_string_column_with_source_format(self, make_df):
        df = make_df([(1, "15-01-2020")], "emp_no int, d string")

        result = change_date_format(df, "d", "yyyy-MM-dd", source_format="dd-MM-yyyy")

        assert result.first().d == "2020-01-15"

    def test_unparseable_string_raises_with_examples(self, make_df):
        df = make_df([(1, "15-01-2020"), (2, None)], "emp_no int, d string")

        with pytest.raises(DataValidationError, match=r"column 'd'.*Examples: \['15-01-2020'\]"):
            change_date_format(df, "d", "yyyy-MM-dd")

    def test_missing_column_raises(self, employees_df):
        with pytest.raises(ColumnNotFoundError, match=r"change_date_format: column\(s\) \['nope'\]"):
            change_date_format(employees_df, "nope", "dd-MM-yyyy")

    @pytest.mark.parametrize("bad_format", ["", "   ", None])
    def test_empty_target_format_raises(self, employees_df, bad_format):
        with pytest.raises(ConfigurationError, match="target_format"):
            change_date_format(employees_df, "hire_date", bad_format)

    def test_non_date_column_raises(self, employees_df):
        with pytest.raises(TransformationError, match="has type int"):
            change_date_format(employees_df, "emp_no", "dd-MM-yyyy")


# --------------------------------------------------------------------------- #
# split_column  (generic number of target columns)
# --------------------------------------------------------------------------- #
class TestSplitColumn:
    def test_splits_date_column_into_three_parts(self, employees_df):
        result = split_column(employees_df, "birth_date", ["y", "m", "d"], "-")

        row = row_by_emp(result, 10001)  # birth_date 1953-09-02
        assert (row.y, row.m, row.d) == ("1953", "09", "02")

    @pytest.mark.parametrize(
        "targets, expected",
        [
            (["p1"], ("a",)),
            (["p1", "p2"], ("a", "b")),
            (["p1", "p2", "p3"], ("a", "b", "c")),
            (["p1", "p2", "p3", "p4"], ("a", "b", "c", "d")),
            (["p1", "p2", "p3", "p4", "p5"], ("a", "b", "c", "d", None)),
        ],
        ids=["1", "2", "3", "4", "5-more-than-parts"],
    )
    def test_any_number_of_target_columns(self, make_df, targets, expected):
        df = make_df([("a-b-c-d",)], "raw string")

        result = split_column(df, "raw", targets, "-")

        assert result.columns == ["raw"] + targets
        assert tuple(result.first()[name] for name in targets) == expected

    def test_parts_beyond_target_columns_are_ignored(self, make_df):
        df = make_df([("John Ronald Smith",)], "name string")

        row = split_column(df, "name", ["first", "middle"], " ").first()

        assert (row.first, row.middle) == ("John", "Ronald")

    def test_targets_are_strings_and_source_kept(self, employees_df):
        result = split_column(employees_df, "birth_date", ["y", "m", "d"], "-")

        assert result.columns == employees_df.columns + ["y", "m", "d"]
        assert all(isinstance(result.schema[c].dataType, StringType) for c in ["y", "m", "d"])

    def test_null_source_gives_null_parts(self, make_df):
        row = split_column(make_df([(None,)], "raw string"), "raw", ["a", "b"], "-").first()

        assert (row.a, row.b) == (None, None)

    @pytest.mark.parametrize(
        "separator",
        [
            ".",
            "|",
            "\\",
            "$",
            "^",
            "*",
            "+",
            "?",
            "(",
            ")",
            "[",
            "]",
            "{",
            "}",
            " ",
            "\t",
            "||",
            "::",
            ".*",
            "\\E",
            "\\Q",
            "/",
            ",",
            "#",
            "é",
        ],
    )
    def test_literal_separator_is_matched_as_plain_text(self, make_df, separator):
        """Default separator_type 'literal': regex characters have no special meaning."""
        df = make_df([(separator.join(["a1", "b2", "c3"]),)], "raw string")

        row = split_column(df, "raw", ["p1", "p2", "p3", "p4"], separator).first()

        assert (row.p1, row.p2, row.p3, row.p4) == ("a1", "b2", "c3", None)

    def test_explicit_literal_separator_type(self, make_df):
        df = make_df([("a.b.c",)], "raw string")

        row = split_column(df, "raw", ["p1", "p2"], ".", separator_type="LITERAL").first()

        assert (row.p1, row.p2) == ("a", "b")

    @pytest.mark.parametrize(
        "value, separator, expected",
        [
            ("a   b\tc", r"\s+", ("a", "b", "c")),
            ("a1b22c", r"\d+", ("a", "b", "c")),
            ("a.b|c", r"[.|]", ("a", "b", "c")),
        ],
    )
    def test_regex_separator_type(self, make_df, value, separator, expected):
        df = make_df([(value,)], "raw string")

        row = split_column(df, "raw", ["p1", "p2", "p3"], separator, separator_type="regex").first()

        assert (row.p1, row.p2, row.p3) == expected

    def test_regex_dot_matches_every_character(self, make_df):
        """With separator_type 'regex' '.' keeps its regex meaning (opt-in)."""
        row = split_column(make_df([("a.b",)], "raw string"), "raw", ["p1"], ".", "regex").first()

        assert row.p1 == ""

    def test_invalid_regex_raises_immediately(self, make_df):
        with pytest.raises(ConfigurationError, match=r"separator '\(' is not a valid regular expression"):
            split_column(make_df([("a",)], "raw string"), "raw", ["p1"], "(", separator_type="regex")

    @pytest.mark.parametrize("separator_type", ["glob", "", None])
    def test_unknown_separator_type_raises(self, make_df, separator_type):
        with pytest.raises(ConfigurationError, match="unsupported separator_type"):
            split_column(make_df([("a",)], "raw string"), "raw", ["p1"], "-", separator_type)

    def test_missing_source_column_raises(self, employees_df):
        with pytest.raises(ColumnNotFoundError, match="split_column"):
            split_column(employees_df, "nope", ["a", "b"], "-")

    @pytest.mark.parametrize("targets", [[], None, "year", ["a", ""]])
    def test_invalid_target_columns_raise(self, employees_df, targets):
        with pytest.raises(ConfigurationError, match="target_columns"):
            split_column(employees_df, "birth_date", targets, "-")

    def test_duplicate_target_columns_raise(self, employees_df):
        with pytest.raises(ConfigurationError, match="duplicate"):
            split_column(employees_df, "birth_date", ["y", "y"], "-")

    def test_target_column_that_already_exists_raises(self, employees_df):
        with pytest.raises(ConfigurationError, match=r"\['gender'\] already exist"):
            split_column(employees_df, "birth_date", ["y", "gender"], "-")

    @pytest.mark.parametrize("separator", ["", None])
    def test_empty_separator_raises(self, employees_df, separator):
        with pytest.raises(ConfigurationError, match="separator"):
            split_column(employees_df, "birth_date", ["y"], separator)


# --------------------------------------------------------------------------- #
# concatenate_columns
# --------------------------------------------------------------------------- #
class TestConcatenateColumns:
    def test_concatenates_with_single_space(self, employees_df):
        result = concatenate_columns(employees_df, ["first_name", "last_name"], "Full_name")

        assert row_by_emp(result, 10001).Full_name == "Georgi Facello"
        assert result.columns == employees_df.columns + ["Full_name"]

    def test_custom_separator_and_three_columns(self, employees_df):
        result = concatenate_columns(
            employees_df, ["first_name", "last_name", "gender"], "label", separator="|"
        )

        assert row_by_emp(result, 10002).label == "Bezalel|Simmel|F"

    def test_null_values_are_skipped(self, mock_employees_df):
        result = concatenate_columns(mock_employees_df, ["first_name", "last_name"], "n")

        assert row_by_emp(result, 2).n == "Bob"
        assert row_by_emp(result, 3).n == "Jones"
        assert row_by_emp(result, 4).n == ""  # all NULL -> empty string

    def test_missing_column_raises(self, employees_df):
        with pytest.raises(ColumnNotFoundError, match=r"concatenate_columns: column\(s\) \['nope'\]"):
            concatenate_columns(employees_df, ["first_name", "nope"], "Full_name")

    def test_existing_target_column_raises(self, employees_df):
        with pytest.raises(ConfigurationError, match="already exist"):
            concatenate_columns(employees_df, ["first_name"], "last_name")

    def test_empty_source_list_raises(self, employees_df):
        with pytest.raises(ConfigurationError, match="source_columns"):
            concatenate_columns(employees_df, [], "Full_name")


# --------------------------------------------------------------------------- #
# drop_column
# --------------------------------------------------------------------------- #
class TestDropColumn:
    def test_drops_listed_columns_and_keeps_order(self, employees_df):
        result = drop_column(employees_df, ["birth_date", "first_name", "last_name"])

        assert result.columns == ["emp_no", "gender", "hire_date"]
        assert result.count() == employees_df.count()

    def test_empty_list_is_noop(self, employees_df):
        assert drop_column(employees_df, []).columns == EMPLOYEE_COLUMNS

    def test_input_dataframe_not_mutated(self, employees_df):
        drop_column(employees_df, ["gender"])

        assert employees_df.columns == EMPLOYEE_COLUMNS

    def test_unknown_column_raises_and_names_it(self, employees_df):
        with pytest.raises(ColumnNotFoundError, match=r"\['nope'\] not found"):
            drop_column(employees_df, ["gender", "nope"])

    def test_non_list_raises(self, employees_df):
        with pytest.raises(ConfigurationError, match="must be a list"):
            drop_column(employees_df, "gender")


# --------------------------------------------------------------------------- #
# join_tables
# --------------------------------------------------------------------------- #
class TestJoinTables:
    def test_inner_join_with_dept_emp(self, employees_df, dept_emp_df):
        result = join_tables(employees_df, dept_emp_df, "emp_no", "inner")

        assert result.count() == 13  # emp_no 10010 is in two departments
        assert sorted(r.dept_no for r in result.filter("emp_no = 10010").collect()) == [
            "d004",
            "d006",
        ]
        assert result.columns == EMPLOYEE_COLUMNS + ["dept_no", "from_date", "to_date"]

    def test_inner_vs_left_join_on_unmatched_keys(self, make_df):
        left = make_df([(1, "a"), (2, "b")], "emp_no int, name string")
        right = make_df([(1, "d001")], "emp_no int, dept_no string")

        assert [r.emp_no for r in join_tables(left, right, "emp_no", "inner").collect()] == [1]
        left_join = join_tables(left, right, "emp_no", "left")
        assert left_join.count() == 2
        assert left_join.filter("emp_no = 2").first().dept_no is None

    def test_overlapping_non_key_columns_log_a_warning(self, employees_df, make_df, caplog):
        right = make_df([(10001, "X")], "emp_no int, gender string")

        with caplog.at_level("WARNING", logger="src.transform"):
            result = join_tables(employees_df, right, "emp_no", "inner")

        assert result.columns.count("gender") == 2
        assert "columns ['gender'] exist in both tables" in caplog.text

    def test_no_warning_when_only_key_overlaps(self, employees_df, dept_emp_df, caplog):
        with caplog.at_level("WARNING", logger="src.transform"):
            join_tables(employees_df, dept_emp_df, "emp_no", "inner")

        assert caplog.text == ""

    def test_join_type_is_case_insensitive(self, employees_df, dept_emp_df):
        assert join_tables(employees_df, dept_emp_df, "emp_no", " INNER ").count() == 13

    def test_unsupported_join_type_raises(self, employees_df, dept_emp_df):
        with pytest.raises(ConfigurationError, match="unsupported join_type 'sideways'"):
            join_tables(employees_df, dept_emp_df, "emp_no", "sideways")

    def test_column_missing_in_left_raises(self, make_df, dept_emp_df):
        with pytest.raises(ColumnNotFoundError, match="not found in the left table"):
            join_tables(make_df([(1,)], "id int"), dept_emp_df, "emp_no", "inner")

    def test_column_missing_in_right_raises(self, employees_df, departments_df):
        with pytest.raises(ColumnNotFoundError, match="not found in the right table"):
            join_tables(employees_df, departments_df, "emp_no", "inner")


# --------------------------------------------------------------------------- #
# filter_rows
# --------------------------------------------------------------------------- #
class TestFilterRows:
    def test_sql_string_expression(self, employees_df):
        result = filter_rows(employees_df, "gender = 'F'")

        assert sorted(r.emp_no for r in result.collect()) == [
            10002,
            10006,
            10007,
            10009,
            10010,
            10011,
        ]

    def test_column_expression(self, employees_df):
        result = filter_rows(employees_df, F.col("hire_date") < F.lit("1986-01-01"))

        assert sorted(r.emp_no for r in result.collect()) == [10002, 10009]

    def test_schema_unchanged(self, employees_df):
        assert filter_rows(employees_df, "emp_no > 0").schema == employees_df.schema

    @pytest.mark.parametrize(
        "bad_expr", ["emp_no >", "unknown_col > 1"], ids=["syntax_error", "unknown_column"]
    )
    def test_invalid_expression_raises(self, employees_df, bad_expr):
        with pytest.raises(TransformationError, match=f"filter_rows: invalid condition '{bad_expr}'"):
            filter_rows(employees_df, bad_expr)


# --------------------------------------------------------------------------- #
# calculate_tenure  (no more silent NULLs)
# --------------------------------------------------------------------------- #
class TestCalculateTenure:
    def test_tenure_on_real_data(self, employees_df):
        result = calculate_tenure(employees_df, "hire_date", "tenure_years")

        for row in result.collect():
            assert row.tenure_years == expected_tenure(row.hire_date), row.emp_no
        assert isinstance(result.schema["tenure_years"].dataType, LongType)

    def test_boundaries_and_null(self, make_df):
        today = dt.date.today()
        rows = [(1, today), (2, today - dt.timedelta(days=int(5 * 365.25) + 10)), (3, None)]
        df = make_df(rows, "emp_no int, hire_date date")

        result = {r.emp_no: r.t for r in calculate_tenure(df, "hire_date", "t").collect()}

        assert result == {1: 0, 2: 5, 3: None}

    def test_iso_string_dates(self, make_df):
        df = make_df([(1, "2000-01-01")], "emp_no int, hire_date string")

        assert calculate_tenure(df, "hire_date", "t").first().t == expected_tenure(dt.date(2000, 1, 1))

    def test_string_dates_with_explicit_format(self, employees_df):
        reformatted = change_date_format(employees_df, "hire_date", "dd-MM-yyyy")

        result = calculate_tenure(reformatted, "hire_date", "t", date_format="dd-MM-yyyy")

        assert row_by_emp(result, 10001).t == expected_tenure(dt.date(1986, 6, 26))

    def test_unparseable_dates_raise_instead_of_null(self, employees_df):
        """Previously this silently produced NULL tenure for every row."""
        reformatted = change_date_format(employees_df, "hire_date", "dd-MM-yyyy")

        with pytest.raises(DataValidationError, match=r"hire_date.*Examples: \['26-06-1986'"):
            calculate_tenure(reformatted, "hire_date", "tenure_years")

    def test_parser_exception_is_reported_with_root_cause(self, make_df):
        """Spark 3 raises (instead of returning NULL) for values such as
        '2020-1-5' with pattern 'yyyy-MM-dd'; the root cause must be readable."""
        df = make_df([(1, "2020-1-5")], "emp_no int, d string")

        with pytest.raises(DataValidationError, match=r"Text '2020-1-5' could not be parsed"):
            calculate_tenure(df, "d", "t", date_format="yyyy-MM-dd")

    def test_existing_target_column_is_overwritten(self, make_df):
        df = make_df([(1, dt.date(2000, 1, 1), -1)], "emp_no int, hire_date date, t bigint")

        assert calculate_tenure(df, "hire_date", "t").first().t == expected_tenure(dt.date(2000, 1, 1))

    def test_missing_column_raises(self, employees_df):
        with pytest.raises(ColumnNotFoundError, match="calculate_tenure"):
            calculate_tenure(employees_df, "start_date", "tenure_years")

    def test_non_date_column_raises(self, employees_df):
        with pytest.raises(TransformationError, match="'emp_no' has type int"):
            calculate_tenure(employees_df, "emp_no", "tenure_years")


# --------------------------------------------------------------------------- #
# fill_missing_values  (NULL + blank strings, type-checked fill values)
# --------------------------------------------------------------------------- #
class TestFillMissingValues:
    def test_fills_null_strings(self, mock_employees_df):
        result = fill_missing_values(mock_employees_df, {"first_name": "N/A", "gender": "U"})

        row = row_by_emp(result, 3)
        assert (row.first_name, row.gender, row.last_name) == ("N/A", "U", "Jones")

    @pytest.mark.parametrize("blank", ["", " ", "   \t"])
    def test_fills_empty_and_whitespace_strings(self, make_df, blank):
        df = make_df([(1, blank), (2, "x")], "emp_no int, name string")

        result = fill_missing_values(df, {"name": "UNKNOWN"})

        assert row_by_emp(result, 1).name == "UNKNOWN"
        assert row_by_emp(result, 2).name == "x"

    def test_fills_empty_result_of_concatenation(self, mock_employees_df):
        """Bug fix: Full_name of an employee without names is '' -> UNKNOWN."""
        concatenated = concatenate_columns(mock_employees_df, ["first_name", "last_name"], "Full_name")

        result = fill_missing_values(concatenated, {"Full_name": "UNKNOWN"})

        assert row_by_emp(result, 4).Full_name == "UNKNOWN"  # both NULL
        assert row_by_emp(result, 1).Full_name == "alice smith"

    def test_non_null_values_and_other_columns_untouched(self, mock_employees_df):
        result = fill_missing_values(mock_employees_df, {"first_name": "N/A"})

        assert result.columns == mock_employees_df.columns
        assert row_by_emp(result, 1).first_name == "alice"
        assert row_by_emp(result, 2).last_name is None

    def test_real_data_without_nulls_is_unchanged(self, employees_df):
        result = fill_missing_values(employees_df, {"first_name": "N/A"})

        assert sorted(result.collect()) == sorted(employees_df.collect())

    @pytest.mark.parametrize("value, expected", [("0", 0), (0, 0), (5.0, 5)])
    def test_numeric_fill_value_converted_to_column_type(self, make_df, value, expected):
        df = make_df([(1, None), (2, 7)], "emp_no int, salary int")

        result = fill_missing_values(df, {"salary": value})

        assert row_by_emp(result, 1).salary == expected
        assert row_by_emp(result, 2).salary == 7

    def test_date_fill_value_converted(self, mock_employees_df):
        result = fill_missing_values(mock_employees_df, {"hire_date": "1900-01-01"})

        assert isinstance(result.schema["hire_date"].dataType, DateType)
        assert row_by_emp(result, 3).hire_date == dt.date(1900, 1, 1)

    @pytest.mark.parametrize(
        "column, value, message",
        [
            ("salary", "abc", "cannot be converted to int"),
            ("start", "not-a-date", "cannot be converted to date"),
            ("salary", 1.9, "would lose its decimal part"),
            ("salary", True, "boolean fill value"),
            ("salary", None, "must not be null"),
        ],
    )
    def test_invalid_fill_value_raises(self, make_df, column, value, message):
        """Previously these were silently ignored and the NULL stayed."""
        df = make_df([(1, None, None)], "emp_no int, salary int, start date")

        with pytest.raises(ConfigurationError, match=message):
            fill_missing_values(df, {column: value})

    def test_unknown_column_raises(self, mock_employees_df):
        with pytest.raises(ColumnNotFoundError, match="not_a_column"):
            fill_missing_values(mock_employees_df, {"not_a_column": "x"})

    @pytest.mark.parametrize("mapping", [{}, None, ["first_name"]])
    def test_invalid_mapping_raises(self, mock_employees_df, mapping):
        with pytest.raises(ConfigurationError, match="mapping_dict"):
            fill_missing_values(mock_employees_df, mapping)


# --------------------------------------------------------------------------- #
# normalize_strings  (case-insensitive case_type, validated)
# --------------------------------------------------------------------------- #
class TestNormalizeStrings:
    @pytest.mark.parametrize("case_type", ["upper", "Upper", "UPPER", " upper "])
    def test_upper_in_any_case(self, employees_df, case_type):
        row = row_by_emp(normalize_strings(employees_df, ["first_name", "last_name"], case_type), 10001)

        assert (row.first_name, row.last_name) == ("GEORGI", "FACELLO")

    @pytest.mark.parametrize("case_type", ["lower", "Lower", "LOWER"])
    def test_lower_in_any_case(self, employees_df, case_type):
        row = row_by_emp(normalize_strings(employees_df, ["first_name"], case_type), 10001)

        assert (row.first_name, row.last_name) == ("georgi", "Facello")

    def test_nulls_stay_null_and_columns_kept(self, mock_employees_df):
        result = normalize_strings(mock_employees_df, ["first_name"], "upper")

        assert result.columns == mock_employees_df.columns
        assert row_by_emp(result, 1).first_name == "ALICE"
        assert row_by_emp(result, 3).first_name is None

    @pytest.mark.parametrize("case_type", ["title", "uper", "", None])
    def test_unknown_case_type_raises(self, employees_df, case_type):
        with pytest.raises(ConfigurationError, match="unsupported case_type"):
            normalize_strings(employees_df, ["first_name"], case_type)

    def test_missing_column_raises(self, employees_df):
        with pytest.raises(ColumnNotFoundError, match="nope"):
            normalize_strings(employees_df, ["first_name", "nope"], "upper")

    def test_non_string_column_raises(self, employees_df):
        with pytest.raises(TransformationError, match=r"only string columns.*emp_no \(int\)"):
            normalize_strings(employees_df, ["emp_no"], "upper")


# --------------------------------------------------------------------------- #
# Config validation
# --------------------------------------------------------------------------- #
class TestValidateTransformations:
    def test_project_config_is_valid(self, transformation_config):
        validate_transformations(transformation_config["transformations"])

    def test_every_registered_step_is_used_by_project_config(self, transformation_config):
        used = {s["transformation"] for s in transformation_config["transformations"]}

        assert used == set(STEP_REGISTRY)

    def test_reports_all_problems_at_once(self):
        steps = [
            {"transformation": "pivot"},
            {"transformation": "split", "source_column": "x"},
            "not a dict",
        ]

        with pytest.raises(ConfigurationError) as exc:
            validate_transformations(steps)

        message = str(exc.value)
        assert "step 1: unknown transformation 'pivot'" in message
        assert "step 2 'split': missing required key(s) ['target_columns', 'separator']" in message
        assert "step 3: must be an object" in message

    def test_non_list_raises(self):
        with pytest.raises(ConfigurationError, match="must be a list"):
            validate_transformations({"transformation": "split"})

    def test_join_table_names(self, transformation_config):
        steps = transformation_config["transformations"] + [
            {
                "transformation": "join_tables",
                "source_tables": ["employees", "departments"],
                "source_column": "dept_no",
                "join_type": "left",
            }
        ]

        assert join_table_names(steps) == ["dept_emp", "departments"]


class TestLoadTransformationConfig:
    def test_loads_project_config(self, transformation_config, write_json):
        assert load_transformation_config(write_json(transformation_config)) == transformation_config

    def test_missing_file(self, tmp_path):
        with pytest.raises(ConfigurationError, match="configuration file not found") as exc:
            load_transformation_config(str(tmp_path / "missing.json"))
        assert isinstance(exc.value.__cause__, FileNotFoundError)

    def test_invalid_json_reports_position(self, write_json):
        with pytest.raises(ConfigurationError, match=r"not valid JSON \(line 1, column 3"):
            load_transformation_config(write_json("{ not json"))

    def test_missing_transformations_key(self, write_json):
        with pytest.raises(ConfigurationError, match="must contain a 'transformations' list"):
            load_transformation_config(write_json({"steps": []}))

    def test_invalid_step_is_rejected(self, write_json):
        with pytest.raises(ConfigurationError, match="unknown transformation 'pivot'"):
            load_transformation_config(write_json({"transformations": [{"transformation": "pivot"}]}))


# --------------------------------------------------------------------------- #
# apply_transformations
# --------------------------------------------------------------------------- #
class TestApplyTransformations:
    def test_empty_config_returns_input(self, employees_df):
        assert apply_transformations(employees_df, []) is employees_df

    def test_steps_applied_in_order(self, employees_df):
        steps = [
            {"transformation": "normalize_strings", "source_columns": ["first_name"], "case_type": "lower"},
            {
                "transformation": "concatenate_columns",
                "source_columns": ["first_name", "last_name"],
                "target_column": "n",
            },
            {"transformation": "drop_column", "source_columns": ["first_name", "last_name"]},
        ]

        result = apply_transformations(employees_df, steps)

        assert "first_name" not in result.columns
        assert row_by_emp(result, 10001).n == "georgi Facello"

    @pytest.mark.parametrize(
        "extra, expected",
        [
            ({}, ("a", "b")),
            ({"separator_type": "literal"}, ("a", "b")),
            ({"separator_type": "regex"}, ("", "")),
        ],
        ids=["default-literal", "literal", "regex"],
    )
    def test_split_step_uses_separator_type_from_config(self, make_df, extra, expected):
        steps = [
            {
                "transformation": "split",
                "source_column": "raw",
                "target_columns": ["p1", "p2"],
                "separator": ".",
                **extra,
            }
        ]

        row = apply_transformations(make_df([("a.b",)], "raw string"), steps).first()

        assert (row.p1, row.p2) == expected

    def test_normalize_strings_defaults_to_upper(self, employees_df):
        steps = [{"transformation": "normalize_strings", "source_columns": ["first_name"]}]

        assert row_by_emp(apply_transformations(employees_df, steps), 10001).first_name == "GEORGI"

    def test_tenure_after_date_format_uses_tracked_format(self, employees_df):
        """Bug fix: step order no longer matters for calculate_tenure."""
        steps = [
            {
                "transformation": "change_date_format",
                "source_column": "hire_date",
                "target_format": "dd-MM-yyyy",
            },
            {"transformation": "calculate_tenure", "start_date_column": "hire_date", "target_column": "t"},
            {"transformation": "filter_rows", "condition_expr": "t >= 0"},
        ]

        result = apply_transformations(employees_df, steps)

        assert result.count() == employees_df.count()
        assert row_by_emp(result, 10001).t == expected_tenure(dt.date(1986, 6, 26))

    def test_date_format_applied_twice(self, employees_df):
        steps = [
            {
                "transformation": "change_date_format",
                "source_column": "hire_date",
                "target_format": "dd-MM-yyyy",
            },
            {
                "transformation": "change_date_format",
                "source_column": "hire_date",
                "target_format": "yyyy/MM/dd",
            },
        ]

        assert row_by_emp(apply_transformations(employees_df, steps), 10001).hire_date == "1986/06/26"

    def test_join_uses_the_table_named_in_source_tables(self, employees_df, tables):
        """Bug fix: source_tables is honoured (previously always dept_emp)."""
        steps = [
            {
                "transformation": "join_tables",
                "source_tables": ["employees", "dept_emp"],
                "source_column": "emp_no",
                "join_type": "inner",
            },
            {
                "transformation": "join_tables",
                "source_tables": ["employees", "departments"],
                "source_column": "dept_no",
                "join_type": "left",
            },
        ]

        result = apply_transformations(employees_df, steps, tables)

        assert result.count() == 13
        assert row_by_emp(result, 10001).dept_name == "Development"

    def test_join_with_unknown_table_raises(self, employees_df, tables):
        steps = [
            {
                "transformation": "join_tables",
                "source_tables": ["employees", "salaries"],
                "source_column": "emp_no",
                "join_type": "inner",
            }
        ]

        with pytest.raises(DataSourceError, match="table 'salaries' is not available"):
            apply_transformations(employees_df, steps, tables)

    @pytest.mark.parametrize(
        "source_tables, message",
        [
            (["dept_emp", "employees"], "left table must be the table being transformed"),
            (["employees"], "exactly two tables"),
            ("dept_emp", "exactly two tables"),
        ],
    )
    def test_invalid_source_tables_raise(self, employees_df, tables, source_tables, message):
        steps = [
            {
                "transformation": "join_tables",
                "source_tables": source_tables,
                "source_column": "emp_no",
                "join_type": "inner",
            }
        ]

        with pytest.raises(ConfigurationError, match=message):
            apply_transformations(employees_df, steps, tables)

    def test_custom_primary_table_name(self, dept_emp_df, tables):
        steps = [
            {
                "transformation": "join_tables",
                "source_tables": ["dept_emp", "departments"],
                "source_column": "dept_no",
                "join_type": "inner",
            }
        ]

        result = apply_transformations(dept_emp_df, steps, tables, primary_table="dept_emp")

        assert "dept_name" in result.columns and result.count() == 13

    def test_step_errors_name_the_step_and_keep_their_type(self, employees_df):
        steps = [
            {"transformation": "normalize_strings", "source_columns": ["first_name"]},
            {"transformation": "drop_column", "source_columns": ["nope"]},
        ]

        with pytest.raises(
            ColumnNotFoundError, match=r"Transformation step 2 \('drop_column'\) failed"
        ) as exc:
            apply_transformations(employees_df, steps)
        assert isinstance(exc.value, ETLError) and isinstance(exc.value, ValueError)

    def test_invalid_config_fails_before_any_step_runs(self, employees_df):
        steps = [
            {"transformation": "normalize_strings", "source_columns": ["first_name"]},
            {"transformation": "unknown_step"},
        ]

        with pytest.raises(ConfigurationError, match="step 2: unknown transformation"):
            apply_transformations(employees_df, steps)
