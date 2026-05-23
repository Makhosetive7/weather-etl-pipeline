# 2-Minute Demo Script — Weather ETL Pipeline

Use this for interviews, portfolio walkthroughs, or screen recordings.  
**Target length:** ~2 minutes at a calm pace.

---

## Setup (before the call)

- Postgres running: `make db-up`
- `.env` has a valid `OPENWEATHER_API_KEY`
- Optional: API already running (`make api-run`) for the last 30 seconds

---

## Script

### 0:00 — Hook (15 sec)

> "I built a small production-style weather ETL: it pulls live data from OpenWeather, validates it with Pydantic, loads a PostgreSQL star schema, and exposes analytics through a FastAPI read layer. Everything is tested and runs in Docker with GitHub Actions CI."

### 0:15 — Architecture (25 sec)

> "The flow is extract, transform, load. Extract hits the REST API with retries and parallel workers plus rate limiting. Transform maps nested JSON into dimension and fact shapes. Load uses idempotent SQL — dimension lookup before insert, and `ON CONFLICT` on the fact table so reruns are safe."

*Optional: show Mermaid diagram in README or draw three boxes on screen.*

### 0:40 — Live run (35 sec)

```bash
make run
```

> "Each run gets a UUID in `weather.etl_runs` with extract, transform, and load counts. Exit code zero means full success; two means partial load failure — useful for schedulers."

Point at log lines: cities fetched → transformed → loaded.

### 1:15 — Warehouse proof (25 sec)

```bash
docker compose exec postgres psql -U postgres -d weather_analytics \
  -c "SELECT city_name, temperature_celsius FROM weather.latest_weather ORDER BY temperature_celsius DESC LIMIT 3;"
```

> "The `latest_weather` view gives one row per city at the latest timestamp — that's the grain we designed for."

### 1:40 — API / full-stack signal (20 sec)

```bash
curl -s http://localhost:8000/api/v1/weather/latest | head -c 200
# or open http://localhost:8000/docs
```

> "The read API mirrors our analytics SQL — latest weather, temperature summary over seven days, hottest and coldest cities, and recent ETL runs. OpenAPI docs are auto-generated."

### 2:00 — Close (10 sec)

> "Under the hood: 89 unit tests, integration tests against Postgres in CI, about ninety percent coverage on the core package, pre-commit hooks, and docker compose for the full stack. Happy to go deeper on idempotency, testing strategy, or schema design."

---

## If they ask one follow-up

| Question | One-line answer |
|----------|-----------------|
| Why star schema? | Separates slowly changing city/condition attributes from repeatable measurements; familiar to analysts. |
| Why raw SQL in the loader? | Clear upserts and `ON CONFLICT`; easy to explain in reviews. |
| How do you test without the API? | Mock `requests` and SQLAlchemy; integration job uses real Postgres. |
| What would you add next? | Scheduled runs, data-quality SQL tests, or Alembic migrations. |

---

## Files to have open

1. `README.md` — architecture + quick start  
2. `sql/schema.sql` — star schema  
3. `main.py` — orchestration + exit codes  
4. `api/main.py` — read endpoints  
5. `tests/` — test pyramid  
