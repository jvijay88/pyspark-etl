"""Checks on the configuration files shipped in config/ (no external systems)."""

import json
import re

import pytest

from src.config_loader import REFERENCE_PATTERN, is_sensitive_key, load_json_config, resolve_config
from src.exceptions import ConfigurationError
from tests.helpers import CONFIG_DIR

SECRETS_EXAMPLE = CONFIG_DIR / "secrets.env.example"


def _env_references(config):
    return set(re.findall(r"\$\{env:([^}]+)\}", json.dumps(config)))


def _example_variables():
    lines = SECRETS_EXAMPLE.read_text().splitlines()
    return {line.split("=", 1)[0].strip() for line in lines if "=" in line and not line.startswith("#")}


def _sensitive_values(node, path=""):
    if isinstance(node, dict):
        for key, value in node.items():
            child = f"{path}.{key}" if path else key
            if is_sensitive_key(key) and isinstance(value, str):
                yield child, value
            yield from _sensitive_values(value, child)


@pytest.mark.parametrize("name", ["extract.json", "config.json"])
class TestCredentialConfigs:
    def test_no_plain_text_secrets(self, name):
        config = load_json_config(str(CONFIG_DIR / name), name)

        plaintext = [path for path, value in _sensitive_values(config) if not REFERENCE_PATTERN.search(value)]
        assert plaintext == []
        assert config.get("allow_plaintext_secrets", False) is False

    def test_every_secret_is_a_reference(self, name):
        config = load_json_config(str(CONFIG_DIR / name), name)

        assert all(REFERENCE_PATTERN.fullmatch(value) for _, value in _sensitive_values(config))

    def test_referenced_variables_are_documented(self, name):
        config = load_json_config(str(CONFIG_DIR / name), name)

        assert _env_references(config) <= _example_variables()

    def test_resolves_when_variables_are_set(self, name, monkeypatch):
        config = load_json_config(str(CONFIG_DIR / name), name)
        for variable in _env_references(config):
            monkeypatch.setenv(variable, f"value-of-{variable}")

        resolved = resolve_config(config, source=name)

        assert "${" not in json.dumps(resolved)

    def test_fails_clearly_when_a_variable_is_missing(self, name, monkeypatch):
        config = load_json_config(str(CONFIG_DIR / name), name)
        for variable in _env_references(config):
            monkeypatch.delenv(variable, raising=False)

        with pytest.raises(ConfigurationError, match="environment variable '.*' is not set"):
            resolve_config(config, source=name)


def test_example_file_lists_only_used_variables():
    used = set()
    for name in ["extract.json", "config.json", "read_data.json"]:
        used |= _env_references(load_json_config(str(CONFIG_DIR / name), name))

    assert _example_variables() == used


def test_transformation_config_uses_explicit_literal_separator():
    config = load_json_config(str(CONFIG_DIR / "transformation.json"), "transform")

    split_steps = [s for s in config["transformations"] if s["transformation"] == "split"]
    assert split_steps and all(s.get("separator_type") == "literal" for s in split_steps)
