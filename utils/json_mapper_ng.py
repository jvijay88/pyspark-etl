"""
Module Name: json_mapper_ng.py
Description: Maps values between JSON documents using JSONPath expressions
             (jsonpath-ng), including nested target paths.
"""

from jsonpath_ng import parse

DEFAULT_MAPPINGS = {
    "$.source_field1": "$.target_field1",
    "$.source_field2": "$.target_field2",
    "$.source_field3": "$.target_field3",
}


def transform_with_jsonpath(input_json: dict, mappings: dict = None) -> dict:
    """Build a new document by copying values from source paths to target paths.

    * ``mappings`` is ``{source_jsonpath: target_jsonpath}``; DEFAULT_MAPPINGS
      is used when it is not given.
    * When a source path matches several values, the first match is used.
    * Source paths without a match are skipped.
    * Invalid JSONPath expressions raise ValueError naming the expression.
    """
    mappings = DEFAULT_MAPPINGS if mappings is None else mappings
    output_json = {}

    for source_path, target_path in mappings.items():
        source_expr = _parse(source_path)
        target_expr = _parse(target_path)

        matches = [match.value for match in source_expr.find(input_json)]
        if matches:
            target_expr.update_or_create(output_json, matches[0])

    return output_json


def _parse(expression: str):
    try:
        return parse(expression)
    except Exception as err:
        raise ValueError(f"Invalid JSONPath expression {expression!r}: {err}") from err


if __name__ == "__main__":
    example = {"source_field1": "Value 1", "source_field2": "Value 2", "source_field3": "Value 3"}
    print(transform_with_jsonpath(example))
