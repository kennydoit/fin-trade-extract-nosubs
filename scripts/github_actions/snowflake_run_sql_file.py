#!/usr/bin/env python3
"""Run a Snowflake SQL file with simple environment-variable templating."""

import os
import re
import sys

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


TOKEN_PATTERN = re.compile(r"\{\{(RAW|SQLSTR):([A-Z0-9_]+)\}\}")
OPT_TOKEN_LINE = re.compile(r"^[^\n]*\{\{OPT:([A-Z0-9_]+)\}\}[^\n]*\n?", re.MULTILINE)


def preprocess_optional_lines(sql_text: str) -> str:
    """Remove lines with {{OPT:VAR}} tokens where VAR is unset or empty.
    When VAR is set, replace {{OPT:VAR}} with {{SQLSTR:VAR}} for normal processing."""
    result_lines = []
    for line in sql_text.splitlines(keepends=True):
        m = re.search(r"\{\{OPT:([A-Z0-9_]+)\}\}", line)
        if m:
            variable = m.group(1)
            value = os.environ.get(variable, "")
            if not value:
                continue  # drop the line entirely
            line = line.replace(f"{{{{OPT:{variable}}}}}", f"{{{{SQLSTR:{variable}}}}}")
        result_lines.append(line)
    return "".join(result_lines)


def load_private_key_bytes() -> bytes:
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "snowflake_rsa_key.der")
    if not os.path.isfile(key_path):
        raise FileNotFoundError(f"Snowflake private key not found: {key_path}")

    with open(key_path, "rb") as key_file:
        private_key = serialization.load_der_private_key(
            key_file.read(),
            password=None,
            backend=default_backend(),
        )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def sql_escape(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def render_sql_template(sql_text: str) -> str:
    sql_text = preprocess_optional_lines(sql_text)

    def replace_token(match: re.Match) -> str:
        mode, variable = match.group(1), match.group(2)
        value = os.environ.get(variable)
        if value is None:
            raise KeyError(f"Missing required environment variable: {variable}")
        return value if mode == "RAW" else sql_escape(value)

    return TOKEN_PATTERN.sub(replace_token, sql_text)


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python scripts/github_actions/snowflake_run_sql_file.py <sql_file>")
        return 1

    sql_file = sys.argv[1]
    if not os.path.isfile(sql_file):
        print(f"SQL file not found: {sql_file}")
        return 1

    with open(sql_file, "r", encoding="utf-8") as f:
        sql_text = f.read()

    try:
        rendered_sql = render_sql_template(sql_text)
    except KeyError as exc:
        print(str(exc))
        return 1

    conn_params = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "private_key": load_private_key_bytes(),
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    }
    role = os.environ.get("SNOWFLAKE_ROLE")
    if role:
        conn_params["role"] = role

    conn = snowflake.connector.connect(**conn_params)

    try:
        conn.execute_string(rendered_sql)
        print(f"Successfully executed SQL file: {sql_file}")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
