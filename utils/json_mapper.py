import json

def map_json_with_custom_mapping(json_data, mapping):
    mapped_json = {}

    for source_key, target_key in mapping.items():
        source_value = json_data.get(source_key)

        if source_value is not None:
            mapped_json[target_key] = source_value

    return mapped_json

# JSON data from the data source
json_data = {
    "mdm.mapping.name": "John",
    "age": 30,
    "address": "123 Main St"
}

# Custom mapping dictionary
mapping = {
    "mdm.mapping.name": "full_name",
    "age": "age",
    "address": "location"
}

# Map JSON with custom mapping
mapped_json = map_json_with_custom_mapping(json_data, mapping)

# Output mapped JSON
print(json.dumps(mapped_json, indent=4))
