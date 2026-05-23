"""Fixtures for integration tests (require PostgreSQL)."""

import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from config.config import Config


def _postgres_available(url: str) -> bool:
    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:
        return False


def _schema_exists(engine) -> bool:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'weather'")
        ).fetchone()
    return row is not None


def _apply_schema(engine) -> None:
    schema_path = Path(__file__).resolve().parents[2] / "sql" / "schema.sql"
    sql = schema_path.read_text()
    for statement in [s.strip() for s in sql.split(";") if s.strip()]:
        if statement.upper().startswith("SELECT "):
            continue
        with engine.begin() as conn:
            conn.execute(text(statement))


@pytest.fixture(scope="session")
def integration_db_url():
    url = os.getenv(
        "DATABASE_URL",
        f"postgresql://{Config.DB_USER}:{Config.DB_PASSWORD}@"
        f"{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}",
    )
    if not _postgres_available(url):
        pytest.skip("PostgreSQL not available — start with make db-up or set DATABASE_URL")
    return url


@pytest.fixture(scope="session")
def integration_engine(integration_db_url):
    engine = create_engine(integration_db_url)
    if not _schema_exists(engine):
        _apply_schema(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def db_conn(integration_engine):
    with integration_engine.connect() as conn:
        yield conn
        conn.rollback()
