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
    schema_path = Path(__file__).resolve().parents[2] / "sql" / "schema.sql"
    sql = schema_path.read_text()
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    with engine.begin() as conn:
        for statement in statements:
            if statement.upper().startswith("SELECT "):
                continue
            conn.execute(text(statement))
    yield engine
    engine.dispose()


@pytest.fixture
def db_conn(integration_engine):
    with integration_engine.connect() as conn:
        yield conn
        conn.rollback()
