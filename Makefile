.PHONY: help install install-dev db-up db-down db-init db-logs run test test-cov lint format clean

PYTHON ?= python3
PIP ?= pip3
COMPOSE ?= docker compose

help:
	@echo "Weather ETL Pipeline"
	@echo ""
	@echo "  make install      Install runtime dependencies"
	@echo "  make install-dev  Install runtime + dev/test/lint tools"
	@echo "  make db-up        Start PostgreSQL (Docker)"
	@echo "  make db-down      Stop PostgreSQL"
	@echo "  make db-init      Apply sql/schema.sql to running database"
	@echo "  make db-logs      Tail Postgres container logs"
	@echo "  make run          Run the ETL pipeline"
	@echo "  make test         Run unit tests"
	@echo "  make test-cov     Run tests with coverage report"
	@echo "  make lint         Run flake8 and black --check"
	@echo "  make format       Format code with black"
	@echo "  make clean        Remove caches and coverage artifacts"

install:
	$(PIP) install -r requirements.txt

install-dev:
	$(PIP) install -r requirements-dev.txt

db-up:
	$(COMPOSE) up -d postgres
	@echo "PostgreSQL listening on localhost:5435"

db-down:
	$(COMPOSE) down

db-init: db-up
	@echo "Waiting for PostgreSQL..."
	@sleep 3
	$(COMPOSE) exec -T postgres psql -U postgres -d weather_analytics -f - < sql/schema.sql
	@echo "Schema applied."

db-logs:
	$(COMPOSE) logs -f postgres

run:
	$(PYTHON) main.py

test:
	$(PYTHON) -m pytest tests/

test-cov:
	$(PYTHON) -m pytest tests/ --cov=src --cov-report=term-missing

lint:
	$(PYTHON) -m flake8 src tests config main.py
	$(PYTHON) -m black --check src tests config main.py

format:
	$(PYTHON) -m black src tests config main.py

clean:
	rm -rf .pytest_cache .coverage htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
