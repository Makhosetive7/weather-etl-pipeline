# Weather ETL Pipeline

A production-style **Extract–Transform–Load (ETL)** pipeline that pulls current weather from the [OpenWeather API](https://openweathermap.org/api), validates and transforms it, and loads it into a **PostgreSQL star schema** for analytics.

Built to demonstrate skills commonly listed in data engineering job posts: ETL design, dimensional modeling, REST integration, retries, validation, SQLAlchemy, pytest, Docker, and environment-based configuration.

## Problem & solution

Weather APIs return nested JSON that is awkward for reporting. This project:

1. **Extracts** current conditions for multiple cities via REST.
2. **Transforms** responses into dimension and fact shapes with validation.
3. **Loads** into PostgreSQL using idempotent dimension lookups and fact inserts.

The result is a small analytics warehouse you can query with standard SQL (e.g. latest temperature per city, trends over time).

## Architecture

```mermaid
flowchart LR
    subgraph extract [Extract]
        API[OpenWeather REST API]
        Retry[Retry on timeout]
    end
    subgraph transform [Transform]
        Validate[Field validation]
        Map[Star schema mapping]
    end
    subgraph load [Load]
        DimCity[cities dimension]
        DimCond[weather_conditions dimension]
        Fact[weather_measurements fact]
    end
    API --> Retry --> Validate --> Map --> DimCity --> Fact
    Map --> DimCond --> Fact
```

### Project layout

```
weather-etl-pipeline/
├── config/config.py      # Environment-driven settings
├── src/
│   ├── extract.py        # API client + retry
│   ├── transform.py      # Validation + mapping
│   ├── load.py           # SQLAlchemy loader
│   └── utils.py          # Logging, retry decorator, helpers
├── sql/schema.sql        # Star schema DDL + views
├── tests/                # pytest unit tests (extract, transform)
├── main.py               # Pipeline orchestration
├── docker-compose.yml    # PostgreSQL
└── Makefile              # Common commands
```

## Tech stack

| Area | Technology |
|------|------------|
| Language | Python 3.10+ |
| API | `requests`, OpenWeather Current Weather |
| Database | PostgreSQL 15, star schema |
| ORM / SQL | SQLAlchemy 2.x (raw SQL via `text()`) |
| Config | `python-dotenv`, `config/config.py` |
| Testing | `pytest`, `pytest-mock`, `unittest.mock` |
| Containers | Docker Compose (Postgres) |
| Tooling | `black`, `flake8`, `Makefile`, `pyproject.toml` |

## Star schema

Schema namespace: `weather`.

| Table | Type | Role |
|-------|------|------|
| `cities` | Dimension | City name, country, coordinates |
| `weather_conditions` | Dimension | Condition type (Clear, Rain, …) |
| `weather_measurements` | Fact | Temperature, humidity, wind, etc. per city & time |

**Grain:** one row per `city_id` + `measurement_timestamp` (enforced with `UNIQUE`).

**Idempotency:**

- Dimensions: lookup by natural key, insert only if missing.
- Facts: `ON CONFLICT (city_id, measurement_timestamp) DO NOTHING`.

**Analytics view:** `weather.latest_weather` — most recent measurement per city.

## Prerequisites

- Python 3.10+
- Docker & Docker Compose (for PostgreSQL)
- [OpenWeather API key](https://openweathermap.org/api) (free tier is enough)

## Quick start

### 1. Clone and configure

```bash
git clone <your-repo-url>
cd weather-etl-pipeline
cp .env.example .env
# Edit .env and set OPENWEATHER_API_KEY
```

### 2. Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
make install-dev
```

### 3. Start database and apply schema

```bash
make db-init
```

This starts Postgres on **localhost:5435** and runs `sql/schema.sql`.

> **Port conflict?** If another project uses 5433 (e.g. another Postgres container), this project uses **5435** by default. Set `DB_PORT` in `.env` if you need a different port.

### 4. Run the pipeline

```bash
make run
```

Logs are written to `logs/weather_etl.log` and stdout.

## Makefile commands

| Command | Description |
|---------|-------------|
| `make help` | Show available targets |
| `make install` | Runtime dependencies only |
| `make install-dev` | Runtime + test + lint tools |
| `make db-up` | Start PostgreSQL container |
| `make db-down` | Stop containers |
| `make db-init` | Start DB + apply schema |
| `make run` | Execute ETL pipeline |
| `make test` | Run unit tests |
| `make test-cov` | Tests with coverage report |
| `make lint` | `flake8` + `black --check` |
| `make format` | Auto-format with `black` |

## Testing

```bash
make test
# or with coverage:
make test-cov
```

- **36** unit tests covering extract and transform layers.
- Loader and full pipeline orchestration tests are planned (see [ROADMAP.md](ROADMAP.md)).
- Current coverage is ~**43%** overall (load module not yet under test).

Tests use mocked HTTP responses — no live API key required for `make test`.

## Sample analytics SQL

Connect to the database:

```bash
docker compose exec postgres psql -U postgres -d weather_analytics
```

**Latest weather per city** (uses built-in view):

```sql
SELECT * FROM weather.latest_weather ORDER BY city_name;
```

**Average temperature by city (last 7 days):**

```sql
SELECT
    c.city_name,
    ROUND(AVG(wm.temperature_celsius)::numeric, 2) AS avg_temp_c
FROM weather.weather_measurements wm
JOIN weather.cities c ON wm.city_id = c.city_id
WHERE wm.measurement_timestamp >= NOW() - INTERVAL '7 days'
GROUP BY c.city_name
ORDER BY avg_temp_c DESC;
```

**Hottest city in the latest snapshot:**

```sql
SELECT city_name, temperature_celsius, measurement_timestamp
FROM weather.latest_weather
ORDER BY temperature_celsius DESC
LIMIT 1;
```

**Row counts by table:**

```sql
SELECT 'cities' AS table_name, COUNT(*) FROM weather.cities
UNION ALL
SELECT 'conditions', COUNT(*) FROM weather.weather_conditions
UNION ALL
SELECT 'measurements', COUNT(*) FROM weather.weather_measurements;
```

## Design decisions

| Decision | Rationale |
|----------|-----------|
| Star schema | Separates slowly changing attributes (city, condition) from measurements; familiar to analysts and BI tools |
| Raw SQL in loader | Explicit control over upserts and `ON CONFLICT`; easy to read in interviews |
| Retry decorator | Transient network timeouts on extract; configurable `MAX_RETRIES` / `RETRY_DELAY` |
| Validation in transform | Reject malformed API payloads before they hit the database |
| Env-based config | Twelve-factor style; secrets stay in `.env` (never committed) |
| Mocked unit tests | Fast, deterministic CI without burning API quota |

## Environment variables

See [.env.example](.env.example) for all variables. Required:

| Variable | Description |
|----------|-------------|
| `OPENWEATHER_API_KEY` | API key from OpenWeather |
| `DB_PASSWORD` | PostgreSQL password (defaults work with `docker-compose.yml`) |

## Roadmap

Phase 1 (documentation & tooling) is complete. Planned next steps:

- GitHub Actions CI (lint, test, coverage gate)
- `Dockerfile` and full `docker compose` stack for the ETL app
- Loader unit tests and integration tests with Postgres
- Pydantic models, `etl_runs` metadata table, structured logging
- Optional FastAPI read API and scheduled runs

See [ROADMAP.md](ROADMAP.md) for the full timeline.

## License

MIT (to be added — see roadmap Phase 5).
