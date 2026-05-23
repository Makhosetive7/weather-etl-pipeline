#!/usr/bin/env python3
"""Wait for PostgreSQL and apply sql/schema.sql (used in CI)."""

import os
import sys
import time
from pathlib import Path

from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/weather_analytics",
)
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"
MAX_WAIT_SECONDS = 60


def wait_for_postgres(engine) -> None:
    for attempt in range(1, MAX_WAIT_SECONDS + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"PostgreSQL ready (attempt {attempt})")
            return
        except Exception as exc:
            print(f"Waiting for PostgreSQL ({attempt}/{MAX_WAIT_SECONDS}): {exc}")
            time.sleep(1)
    raise RuntimeError("PostgreSQL did not become ready in time")


def schema_exists(engine) -> bool:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'weather'")
        ).fetchone()
    return row is not None


def apply_schema(engine) -> None:
    sql = SCHEMA_PATH.read_text()
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for statement in statements:
        if statement.upper().startswith("SELECT "):
            continue
        with engine.begin() as conn:
            conn.execute(text(statement))
    print("Schema applied successfully")


def main() -> int:
    engine = create_engine(DATABASE_URL)
    try:
        wait_for_postgres(engine)
        if schema_exists(engine):
            print("Schema already exists, skipping apply")
        else:
            apply_schema(engine)
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main())
