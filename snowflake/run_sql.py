"""Run a .sql file against Snowflake, substituting {PLACEHOLDER} tokens from
.env first.

setup.sql commits placeholders like {AWS_ACCESS_KEY_ID} rather than real
values -- see docs/adr/0002-stage-credentials-over-storage-integration.md for
why the stage needs a real key at all. This resolves them in memory only; the
substituted SQL is never written to disk.

    ./.venv/bin/python snowflake/run_sql.py snowflake/setup.sql
    ./.venv/bin/python snowflake/run_sql.py snowflake/copy_into.sql
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

PLACEHOLDERS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_BUCKET")


def split_statements(sql: str) -> list[str]:
    """Strip `--` comments, then split on ';'.

    A comment containing its own semicolon (`-- UTC; see the ADR`) will
    otherwise be read as a statement boundary and silently slice whatever
    statement follows it in half. Stripping comments first closes that,
    though a real tokenizer would still be needed the moment a statement
    contains a semicolon inside a string literal -- none of these do.
    """
    without_comments = "\n".join(
        line.split("--", 1)[0] for line in sql.splitlines()
    )
    return [s.strip() for s in without_comments.split(";") if s.strip()]


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("usage: run_sql.py <path/to/file.sql>")

    root = Path(__file__).resolve().parent.parent
    load_dotenv(root / ".env")

    path = Path(sys.argv[1])
    text = path.read_text()

    for name in PLACEHOLDERS:
        value = os.getenv(name)
        if value and f"{{{name}}}" in text:
            text = text.replace(f"{{{name}}}", value.strip())

    remaining = [p for p in PLACEHOLDERS if f"{{{p}}}" in text]
    if remaining:
        sys.exit(f"Unresolved placeholders, missing from .env: {remaining}")

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"].strip(),
        user=os.environ["SNOWFLAKE_USER"].strip(),
        password=os.environ["SNOWFLAKE_PASSWORD"].strip(),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"].strip(),
    )
    cursor = conn.cursor()

    print(f"running {path} ({len(split_statements(text))} statements)\n")
    for statement in split_statements(text):
        first_line = statement.splitlines()[0][:80]
        print(f"  > {first_line}")
        try:
            cursor.execute(statement)
            if cursor.description:
                for row in cursor.fetchall():
                    print(f"    {row}")
        except snowflake.connector.errors.ProgrammingError as exc:
            # str(exc) here is fine to print: Snowflake's error text quotes
            # the failing clause, not the resolved stage credentials.
            sys.exit(f"    FAILED: {exc}")

    cursor.close()
    conn.close()
    print("\nOK")


if __name__ == "__main__":
    main()
