"""Unit tests for utils/json_mapper.py"""

import copy

import pytest

from utils.json_mapper import map_json_with_custom_mapping

SOURCE = {"mdm.mapping.name": "John", "age": 30, "address": "123 Main St"}
MAPPING = {"mdm.mapping.name": "full_name", "age": "age", "address": "location"}


class TestMapJsonWithCustomMapping:
    def test_maps_and_renames_keys(self):
        assert map_json_with_custom_mapping(SOURCE, MAPPING) == {
            "full_name": "John",
            "age": 30,
            "location": "123 Main St",
        }

    def test_only_mapped_keys_are_returned(self):
        assert map_json_with_custom_mapping(SOURCE, {"age": "years"}) == {"years": 30}

    def test_missing_source_keys_are_skipped(self):
        assert map_json_with_custom_mapping({"age": 30}, MAPPING) == {"age": 30}

    def test_none_values_are_skipped(self):
        assert map_json_with_custom_mapping({"age": None, "address": "x"}, MAPPING) == {"location": "x"}

    @pytest.mark.parametrize("falsy", [0, "", False, [], {}])
    def test_falsy_non_none_values_are_kept(self, falsy):
        assert map_json_with_custom_mapping({"age": falsy}, {"age": "age"}) == {"age": falsy}

    def test_dotted_keys_are_literal_not_nested(self):
        nested = {"mdm": {"mapping": {"name": "John"}}}

        assert map_json_with_custom_mapping(nested, MAPPING) == {}

    def test_nested_values_are_copied_as_is(self):
        source = {"address": {"city": "Pune", "zip": "411001"}}

        assert map_json_with_custom_mapping(source, MAPPING) == {"location": source["address"]}

    def test_two_sources_to_same_target_last_wins(self):
        source = {"a": 1, "b": 2}

        assert map_json_with_custom_mapping(source, {"a": "x", "b": "x"}) == {"x": 2}

    def test_empty_inputs(self):
        assert map_json_with_custom_mapping({}, MAPPING) == {}
        assert map_json_with_custom_mapping(SOURCE, {}) == {}

    def test_input_is_not_modified(self):
        source = copy.deepcopy(SOURCE)

        map_json_with_custom_mapping(source, MAPPING)

        assert source == SOURCE

    @pytest.mark.parametrize("json_data, mapping", [(None, MAPPING), ([1], MAPPING), (SOURCE, None)])
    def test_non_dict_arguments_raise_type_error(self, json_data, mapping):
        with pytest.raises(TypeError, match="must be a dict"):
            map_json_with_custom_mapping(json_data, mapping)
