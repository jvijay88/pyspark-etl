from jsonpath_ng import parse

def transform_with_jsonpath(input_json):
    output_json = {}

    # Define the transformation rules using JSONPath expressions
    mappings = {
        "$.source_field1": "$.target_field1",
        "$.source_field2": "$.target_field2",
        "$.source_field3": "$.target_field3"
    }

    for source_path, target_path in mappings.items():
        source_expr = parse(source_path)
        target_expr = parse(target_path)
        print("Hello")
        matches = [match.value for match in source_expr.find(input_json)]

        if matches:
            target_expr.update_or_create(output_json, matches[0])

    return output_json

# Input JSON data
input_json = {
    "source_field1": "Value 1",
    "source_field2": "Value 2",
    "source_field3": "Value 3"
}

# Apply the transformation
output_json = transform_with_jsonpath(input_json)
print(output_json)
