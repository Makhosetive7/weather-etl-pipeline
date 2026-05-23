# Resume & LinkedIn Bullets

Copy, trim, or combine for your CV and LinkedIn **Featured** / **About** section.  
Replace bracketed placeholders with your name or metrics if you have fresher numbers.

---

## One-liner (headline / project title)

**Weather ETL Pipeline** — OpenWeather → PostgreSQL star schema → FastAPI analytics API | Python, Docker, CI/CD, pytest

---

## Resume bullets (pick 2–4)

- Designed and implemented an end-to-end **ETL pipeline** ingesting OpenWeather data into a **PostgreSQL star schema** (dimension/fact tables, idempotent loads, analytics views).

- Built **Python** extract/transform/load modules with **retry logic**, parallel API extraction, **Pydantic** validation, and structured logging with per-run metadata (`etl_runs`).

- Delivered a **FastAPI** read layer exposing latest weather, historical measurements, and rolling analytics (temperature summaries, extremes) with auto-generated **OpenAPI** documentation.

- Achieved **~90% test coverage** with **89+ unit tests** and **Postgres integration tests**; enforced quality via **GitHub Actions** (lint, format, coverage gate) and **pre-commit** hooks.

- Containerized the stack with **Docker Compose** (PostgreSQL, ETL worker, API) and documented a **5-minute quick start** for reproducible local and portfolio demos.

---

## LinkedIn post / project blurb (short)

Built a portfolio-grade **data engineering** project: live weather ETL into a **star schema**, observability per pipeline run, and a **REST API** for analytics consumers.

Stack: Python, SQLAlchemy, PostgreSQL, Pydantic, FastAPI, Docker, GitHub Actions, pytest.

Repo: `github.com/Makhosetive7/weather-etl-pipeline`

---

## LinkedIn bullets (Skills section mapping)

| Skill keyword | Where in repo |
|---------------|---------------|
| ETL / ELT | `src/extract.py`, `transform.py`, `load.py`, `main.py` |
| Data modeling | `sql/schema.sql`, README star schema section |
| SQL | Loader raw SQL, `sql/analytics_examples.sql` |
| Python | Entire codebase |
| REST APIs | OpenWeather extract + FastAPI `api/` |
| Docker | `Dockerfile`, `Dockerfile.api`, `docker-compose.yml` |
| CI/CD | `.github/workflows/ci.yml` |
| Unit testing | `tests/` (89 tests) |
| Data validation | `src/models.py` (Pydantic) |

---

## Interview “project summary” (30 sec spoken)

> I built a weather analytics pipeline that extracts from OpenWeather, validates and transforms into a star schema, and loads PostgreSQL with idempotent SQL. I added run metadata for observability, parallel extraction with rate limits, and a FastAPI layer for downstream consumers. It's fully tested, containerized, and runs in CI on every push.

---

*Update test/coverage numbers after major changes: `make test-cov`*
