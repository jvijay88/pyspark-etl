"""Unit tests for utils/json_mapper_ng.py"""

import copy

import pytest

from utils.json_mapper_ng import DEFAULT_MAPPINGS, transform_with_jsonpath

SOURCE = {"source_field1": "Value 1", "source_field2": "Value 2", "source_field3": "Value 3"}


class TestDefaultMappings:
    def test_maps_all_default_fields(self):
        assert transform_with_jsonpath(SOURCE) == {
            "target_field1": "Value 1",
            "target_field2": "Value 2",
            "target_field3": "Value 3",
        }

    def test_missing_source_fields_are_skipped(self):
        assert transform_with_jsonpath({"source_field2": "only"}) == {"target_field2": "only"}

    def test_no_matches_gives_empty_document(self):
        assert transform_with_jsonpath({"other": 1}) == {}

    def test_default_mappings_cover_three_fields(self):
        assert len(DEFAULT_MAPPINGS) == 3


class TestCustomMappings:
    def test_nested_source_to_nested_target(self):
        source = {"employee": {"name": {"first": "Georgi"}, "id": 10001}}
        mappings = {"$.employee.name.first": "$.person.first_name", "$.employee.id": "$.person.id"}

        assert transform_with_jsonpath(source, mappings) == {"person": {"first_name": "Georgi", "id": 10001}}

    def test_first_match_is_used_for_multiple_matches(self):
        source = {"items": [{"id": "a"}, {"id": "b"}]}

        assert transform_with_jsonpath(source, {"$.items[*].id": "$.first_id"}) == {"first_id": "a"}

    def test_array_index_source(self):
        source = {"items": [{"id": "a"}, {"id": "b"}]}

        assert transform_with_jsonpath(source, {"$.items[1].id": "$.second_id"}) == {"second_id": "b"}

    @pytest.mark.parametrize("value", [0, "", False, None, [1, 2], {"k": "v"}])
    def test_values_of_any_type_are_copied(self, value):
        assert transform_with_jsonpath({"v": value}, {"$.v": "$.out"}) == {"out": value}

    def test_empty_mapping(self):
        assert transform_with_jsonpath(SOURCE, {}) == {}

    def test_input_is_not_modified(self):
        source = copy.deepcopy(SOURCE)

        transform_with_jsonpath(source)

        assert source == SOURCE

    def test_invalid_expression_raises_value_error(self):
        with pytest.raises(ValueError, match=r"Invalid JSONPath expression '\$\.\['"):
            transform_with_jsonpath(SOURCE, {"$.[": "$.out"})
