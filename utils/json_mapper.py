"""
Module Name: json_mapper.py
Description: Renames the keys of a flat JSON object using a source -> target mapping.
"""

import json


def map_json_with_custom_mapping(json_data: dict, mapping: dict) -> dict:
    """Return a new dict containing only the mapped keys, renamed.

    * Keys are matched literally (a dotted key such as "mdm.mapping.name" is
      one key, not a nested path).
    * Source keys that are missing or have a ``None`` value are skipped.
      Other falsy values (0, "", False, []) are kept.
    * The input dict is not modified.
    """
    if not isinstance(json_data, dict):
        raise TypeError(f"json_data must be a dict, got {type(json_data).__name__}.")
    if not isinstance(mapping, dict):
        raise TypeError(f"mapping must be a dict, got {type(mapping).__name__}.")

    mapped_json = {}
    for source_key, target_key in mapping.items():
        source_value = json_data.get(source_key)
        if source_value is not None:
            mapped_json[target_key] = source_value
    return mapped_json


if __name__ == "__main__":
    example_data = {"mdm.mapping.name": "John", "age": 30, "address": "123 Main St"}
    example_mapping = {"mdm.mapping.name": "full_name", "age": "age", "address": "location"}
    print(json.dumps(map_json_with_custom_mapping(example_data, example_mapping), indent=4))
