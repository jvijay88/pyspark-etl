"""
Module Name: config_loader.py
Description: Reads JSON configuration files and resolves secret / environment
             references inside them, so no credentials have to be stored in
             the config files.

Reference syntax (usable in any string value, also inside a longer string):

    ${env:MYSQL_PASSWORD}                 environment variable
    ${file:/run/secrets/mysql_password}   file content (Docker / Kubernetes secrets)
    ${aws-sm:prod/etl/mysql}              AWS Secrets Manager, whole SecretString
    ${aws-sm:prod/etl/mysql#password}     AWS Secrets Manager, one key of a JSON secret

Plain-text secrets policy:
    Keys that look sensitive (password, secret, token, ...) must use a
    reference. A literal value raises ConfigurationError unless the config
    file sets  "allow_plaintext_secrets": true  at the top level (local
    development only).

Other secret stores (Azure Key Vault, GCP Secret Manager, HashiCorp Vault)
can be plugged in with ``register_secret_provider``.
"""

import json
import os
import re

from src.exceptions import ConfigurationError

ALLOW_PLAINTEXT_KEY = "allow_plaintext_secrets"

# ${provider:reference}
REFERENCE_PATTERN = re.compile(r"\$\{(?P<provider>[A-Za-z][A-Za-z0-9_-]*):(?P<reference>[^}]*)\}")
SENSITIVE_KEY_PATTERN = re.compile(
    r"password|passwd|pwd|secret|token|api_?key|private_?key|credential", re.IGNORECASE
)


# --------------------------------------------------------------------------- #
# JSON files
# --------------------------------------------------------------------------- #
def load_json_config(path: str, operation: str) -> dict:
    """Read a JSON object file; errors are raised as ConfigurationError with the file name."""
    try:
        with open(path) as config_file:
            config = json.load(config_file)
    except FileNotFoundError as err:
        raise ConfigurationError(f"{operation}: configuration file not found: '{path}'.") from err
    except json.JSONDecodeError as err:
        raise ConfigurationError(
            f"{operation}: configuration '{path}' is not valid JSON "
            f"(line {err.lineno}, column {err.colno}: {err.msg})."
        ) from err
    if not isinstance(config, dict):
        raise ConfigurationError(
            f"{operation}: configuration '{path}' must be a JSON object, got {type(config).__name__}."
        )
    return config


# --------------------------------------------------------------------------- #
# Secret providers
# --------------------------------------------------------------------------- #
class SecretLookupError(Exception):
    """Raised by a provider when a reference cannot be resolved."""


def _env_provider(reference: str) -> str:
    name = reference.strip()
    if name not in os.environ:
        raise SecretLookupError(f"environment variable '{name}' is not set")
    return os.environ[name]


def _file_provider(reference: str) -> str:
    path = reference.strip()
    try:
        with open(path) as secret_file:
            # Secret files usually end with a newline that is not part of the value.
            return secret_file.read().rstrip("\r\n")
    except OSError as err:
        raise SecretLookupError(f"secret file '{path}' cannot be read ({err.strerror})") from err


def parse_aws_reference(reference: str):
    """'secret-id#json_key' -> ('secret-id', 'json_key'); the key is optional."""
    secret_id, _, json_key = reference.strip().partition("#")
    if not secret_id:
        raise SecretLookupError("AWS Secrets Manager reference needs a secret id")
    return secret_id, (json_key or None)


_aws_secret_cache = {}


def _aws_secrets_manager_provider(reference: str) -> str:
    secret_id, json_key = parse_aws_reference(reference)
    if secret_id not in _aws_secret_cache:
        try:
            import boto3  # optional dependency, available on AWS Glue / EMR
        except ImportError as err:
            raise SecretLookupError("the 'aws-sm' provider needs boto3 (pip install boto3)") from err
        try:
            response = boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)
        except Exception as err:  # noqa: BLE001 - report provider failures uniformly
            raise SecretLookupError(
                f"AWS secret '{secret_id}' could not be read ({type(err).__name__})"
            ) from err
        _aws_secret_cache[secret_id] = response["SecretString"]

    secret_string = _aws_secret_cache[secret_id]
    if json_key is None:
        return secret_string
    try:
        return str(json.loads(secret_string)[json_key])
    except (ValueError, KeyError, TypeError) as err:
        raise SecretLookupError(f"AWS secret '{secret_id}' has no JSON key '{json_key}'") from err


SECRET_PROVIDERS = {
    "env": _env_provider,
    "file": _file_provider,
    "aws-sm": _aws_secrets_manager_provider,
}


def register_secret_provider(name: str, provider) -> None:
    """Add a provider: ``provider(reference: str) -> str``; raise SecretLookupError on failure."""
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]*", name or ""):
        raise ValueError(f"Invalid secret provider name {name!r}.")
    SECRET_PROVIDERS[name] = provider


# --------------------------------------------------------------------------- #
# Resolution
# --------------------------------------------------------------------------- #
def is_sensitive_key(key) -> bool:
    return isinstance(key, str) and bool(SENSITIVE_KEY_PATTERN.search(key))


def _resolve_string(value: str, key_path: str, source: str) -> str:
    def replace(match):
        provider_name, reference = match.group("provider"), match.group("reference")
        provider = SECRET_PROVIDERS.get(provider_name)
        if provider is None:
            raise ConfigurationError(
                f"{source}: '{key_path}' uses unknown secret provider '{provider_name}'. "
                f"Supported: {', '.join(sorted(SECRET_PROVIDERS))}."
            )
        try:
            return provider(reference)
        except SecretLookupError as err:
            raise ConfigurationError(f"{source}: '{key_path}' could not be resolved: {err}.") from err

    return REFERENCE_PATTERN.sub(replace, value)


def resolve_config(config, source: str = "config", allow_plaintext: bool = False):
    """Return a deep copy of ``config`` with every ${provider:reference} resolved.

    ``source`` names the file/section in error messages. Secret values are
    never included in error messages.
    """

    def walk(node, key_path, key):
        if isinstance(node, dict):
            return {k: walk(v, f"{key_path}.{k}" if key_path else str(k), k) for k, v in node.items()}
        if isinstance(node, list):
            return [walk(item, f"{key_path}[{i}]", key) for i, item in enumerate(node)]
        if not isinstance(node, str):
            return node
        if is_sensitive_key(key) and node and not REFERENCE_PATTERN.search(node) and not allow_plaintext:
            raise ConfigurationError(
                f"{source}: '{key_path}' contains a plain-text secret. Use a reference such as "
                f"'${{env:{str(key).upper()}}}', or set \"{ALLOW_PLAINTEXT_KEY}\": true for "
                "local development only."
            )
        return _resolve_string(node, key_path, source)

    return walk(config, "", None)
