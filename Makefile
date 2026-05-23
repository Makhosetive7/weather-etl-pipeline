.PHONY: help install install-dev install-api db-up db-down db-init db-logs docker-build docker-up docker-down api-run api-up run test test-cov test-integration lint format pre-commit clean

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
	@echo "  make docker-build Build ETL Docker image"
	@echo "  make docker-up    Start Postgres + run ETL (requires .env)"
	@echo "  make docker-down  Stop full Docker stack"
	@echo "  make api-run      Run FastAPI locally (port 8000)"
	@echo "  make api-up       Start Postgres + API in Docker"
	@echo "  make run          Run the ETL pipeline (local Python)"
	@echo "  make test         Run unit tests"
	@echo "  make test-cov     Run tests with coverage report"
	@echo "  make test-integration  Integration tests (requires Postgres)"
	@echo "  make lint         Run flake8 and black --check"
	@echo "  make format       Format code with black"
	@echo "  make clean        Remove caches and coverage artifacts"

install:
	$(PIP) install -r requirements.txt

install-dev:
	$(PIP) install -r requirements-dev.txt

install-api:
	$(PIP) install -r requirements-api.txt

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

docker-build:
	$(COMPOSE) build etl

docker-up:
	$(COMPOSE) up --build

docker-down:
	$(COMPOSE) down

api-run:
	$(PYTHON) -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

api-up:
	$(COMPOSE) up -d postgres api
	@echo "API docs: http://localhost:8000/docs"

run:
	$(PYTHON) main.py

test:
	$(PYTHON) -m pytest tests/ --ignore=tests/integration

test-cov:
	$(PYTHON) -m pytest tests/ --ignore=tests/integration --cov=src --cov-report=term-missing

test-integration: db-up
	@echo "Waiting for PostgreSQL..."
	@sleep 3
	$(COMPOSE) exec -T postgres psql -U postgres -d weather_analytics -f - < sql/schema.sql 2>/dev/null || true
	$(PYTHON) -m pytest tests/integration -m integration -v

lint:
	$(PYTHON) -m flake8 src api tests config main.py
	$(PYTHON) -m black --check src api tests config main.py

format:
	$(PYTHON) -m black src api tests config main.py

pre-commit:
	pre-commit install
	pre-commit run --all-files

clean:
	rm -rf .pytest_cache .coverage htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
