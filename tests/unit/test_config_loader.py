"""Unit tests for src/config_loader.py (JSON loading + secret references).

Only local providers (env, file) are exercised. The AWS Secrets Manager
provider talks to AWS and is out of scope; only its reference parsing is tested.
"""

import pytest

from src import config_loader
from src.config_loader import (
    SECRET_PROVIDERS,
    SecretLookupError,
    is_sensitive_key,
    load_json_config,
    parse_aws_reference,
    register_secret_provider,
    resolve_config,
)
from src.exceptions import ConfigurationError


class TestLoadJsonConfig:
    def test_reads_object(self, write_json):
        assert load_json_config(write_json({"a": 1}), "op") == {"a": 1}

    def test_missing_file(self, tmp_path):
        with pytest.raises(
            ConfigurationError, match=r"op: configuration file not found: '.*nope.json'"
        ) as exc:
            load_json_config(str(tmp_path / "nope.json"), "op")
        assert isinstance(exc.value.__cause__, FileNotFoundError)

    def test_invalid_json_reports_position(self, write_json):
        with pytest.raises(ConfigurationError, match=r"not valid JSON \(line 2, column 1"):
            load_json_config(write_json('{"a": 1,\n}'), "op")

    @pytest.mark.parametrize("content", ["[1, 2]", '"text"', "3"])
    def test_top_level_must_be_object(self, write_json, content):
        with pytest.raises(ConfigurationError, match="must be a JSON object"):
            load_json_config(write_json(content), "op")


class TestEnvReferences:
    def test_whole_value(self, monkeypatch):
        monkeypatch.setenv("DB_PASSWORD", "s3cr3t")

        assert resolve_config({"password": "${env:DB_PASSWORD}"}) == {"password": "s3cr3t"}

    def test_embedded_and_multiple_references(self, monkeypatch):
        monkeypatch.setenv("ACCOUNT", "acme")
        monkeypatch.setenv("REGION", "eu-west-1")

        resolved = resolve_config({"url": "${env:ACCOUNT}.${env:REGION}.snowflakecomputing.com"})

        assert resolved == {"url": "acme.eu-west-1.snowflakecomputing.com"}

    def test_nested_objects_and_lists(self, monkeypatch):
        monkeypatch.setenv("T1", "employees")
        config = {"section": {"tables": ["${env:T1}", "dept_emp"], "port": 3306, "ssl": True}}

        assert resolve_config(config) == {
            "section": {"tables": ["employees", "dept_emp"], "port": 3306, "ssl": True}
        }

    def test_empty_variable_is_allowed(self, monkeypatch):
        monkeypatch.setenv("DB_PASSWORD", "")

        assert resolve_config({"password": "${env:DB_PASSWORD}"}) == {"password": ""}

    def test_unset_variable_names_key_path_and_variable(self, monkeypatch):
        monkeypatch.delenv("NOT_SET_VAR", raising=False)

        with pytest.raises(ConfigurationError) as exc:
            resolve_config({"db": {"users": [{"password": "${env:NOT_SET_VAR}"}]}}, source="extract.json")

        assert str(exc.value) == (
            "extract.json: 'db.users[0].password' could not be resolved: "
            "environment variable 'NOT_SET_VAR' is not set."
        )

    def test_input_is_not_modified(self, monkeypatch):
        monkeypatch.setenv("X", "1")
        config = {"a": {"b": "${env:X}"}}

        resolve_config(config)

        assert config == {"a": {"b": "${env:X}"}}

    def test_values_without_references_are_unchanged(self):
        config = {"host": "localhost", "expr": "tenure_years >= 0", "price": "$5", "n": None}

        assert resolve_config(config) == config


class TestFileReferences:
    def test_reads_file_and_strips_trailing_newline(self, tmp_path):
        secret = tmp_path / "mysql_password"
        secret.write_text("p@ss word\n")

        assert resolve_config({"password": f"${{file:{secret}}}"}) == {"password": "p@ss word"}

    def test_missing_file(self, tmp_path):
        with pytest.raises(
            ConfigurationError, match=r"'password' could not be resolved: secret file .* cannot be read"
        ):
            resolve_config({"password": f"${{file:{tmp_path / 'missing'}}}"})


class TestPlainTextSecrets:
    @pytest.mark.parametrize(
        "key",
        [
            "mysql_password",
            "sf_password",
            "PG_PASSWD",
            "client_secret",
            "api_key",
            "apiKey",
            "auth_token",
            "private_key",
            "db_pwd",
            "aws_credentials",
        ],
    )
    def test_sensitive_keys(self, key):
        assert is_sensitive_key(key)

    @pytest.mark.parametrize("key", ["mysql_user", "host", "target_table", "s3_bucket_name", None, 3])
    def test_non_sensitive_keys(self, key):
        assert not is_sensitive_key(key)

    def test_plaintext_secret_is_rejected_without_leaking_it(self):
        with pytest.raises(ConfigurationError) as exc:
            resolve_config({"mysql_config": {"mysql_password": "root"}}, source="config.json")

        message = str(exc.value)
        assert "'mysql_config.mysql_password' contains a plain-text secret" in message
        assert "${env:MYSQL_PASSWORD}" in message
        assert "root" not in message

    def test_plaintext_allowed_when_configured(self):
        assert resolve_config({"password": "root"}, allow_plaintext=True) == {"password": "root"}

    def test_empty_secret_value_is_not_plaintext(self):
        assert resolve_config({"password": ""}) == {"password": ""}

    def test_reference_inside_sensitive_value_is_accepted(self, monkeypatch):
        monkeypatch.setenv("PART", "xyz")

        assert resolve_config({"token": "Bearer ${env:PART}"}) == {"token": "Bearer xyz"}


class TestProviders:
    def test_unknown_provider(self):
        with pytest.raises(
            ConfigurationError, match="unknown secret provider 'vault'. Supported: aws-sm, env, file"
        ):
            resolve_config({"password": "${vault:kv/etl#password}"})

    def test_register_custom_provider(self, monkeypatch):
        monkeypatch.setitem(SECRET_PROVIDERS, "dict", None)  # restored after the test
        store = {"etl/mysql": "from-store"}

        def dict_provider(reference):
            if reference not in store:
                raise SecretLookupError(f"no secret '{reference}'")
            return store[reference]

        register_secret_provider("dict", dict_provider)

        assert resolve_config({"password": "${dict:etl/mysql}"}) == {"password": "from-store"}
        with pytest.raises(ConfigurationError, match="could not be resolved: no secret 'other'"):
            resolve_config({"password": "${dict:other}"})

    @pytest.mark.parametrize("name", ["", "1abc", "has space", None])
    def test_invalid_provider_name(self, name):
        with pytest.raises(ValueError, match="Invalid secret provider name"):
            register_secret_provider(name, lambda ref: ref)

    @pytest.mark.parametrize(
        "reference, expected",
        [
            ("prod/etl/mysql", ("prod/etl/mysql", None)),
            ("prod/etl/mysql#password", ("prod/etl/mysql", "password")),
            (
                " arn:aws:secretsmanager:eu-west-1:1:secret:etl#user ",
                ("arn:aws:secretsmanager:eu-west-1:1:secret:etl", "user"),
            ),
        ],
    )
    def test_parse_aws_reference(self, reference, expected):
        assert parse_aws_reference(reference) == expected

    def test_parse_aws_reference_needs_secret_id(self):
        with pytest.raises(SecretLookupError, match="needs a secret id"):
            parse_aws_reference("#password")

    def test_builtin_providers(self):
        assert {"env", "file", "aws-sm"} <= set(config_loader.SECRET_PROVIDERS)
