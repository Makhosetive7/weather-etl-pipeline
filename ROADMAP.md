# Weather ETL Pipeline — Development Roadmap

High-level plan to take this project from **working prototype** to **recruiter-ready portfolio**.  
For detailed checkboxes, see local `PROJECT_TODOS.md` (not committed to Git).

**Current baseline (~40% complete):** ETL core, star schema, extract/transform tests, basic logging & retry.

---

## Target scopes

| Scope | Phases | Best for | Effort |
|-------|--------|----------|--------|
| **A — Job-ready** | 1 + 2 | GitHub, resume, first interviews | ~18–26 hours |
| **B — Strong mid-level** | A + 3 (core only) | Standing out vs other portfolios | ~35–50 hours |
| **C — Full build** | A + B + 4 + 5 (all items) | Maximum depth | ~55–75 hours |

**Recommendation:** Finish **Scope A** first (~2 weeks part-time), then add **Scope B** while interviewing.

---

## Timeline overview

### Part-time (~2–3 hours / day)

| Period | Scope | Focus |
|--------|-------|-------|
| **Week 1** | A | Documentation, env setup, Makefile, deps cleanup |
| **Week 2** | A | CI/CD, Docker, lint tools, loader tests |
| **Week 3–4** | B | Pydantic, `etl_runs`, analytics SQL, structured logging |
| **Week 5** | B | Integration tests (Postgres in CI) |
| **Week 6** | B+ | Pick 1–2 Phase 4 items (FastAPI or cron + SQL tests) |
| **Week 7–8** | C (optional) | ADRs, mypy, remaining differentiators, portfolio polish |

| Goal | Calendar @ 2–3 h/day |
|------|----------------------|
| Job-ready (A) | **~2 weeks** |
| Strong portfolio (B) | **~6–8 weeks** |
| Everything (C) | **~8–10 weeks** |

### Focused (~4–5 hours / day)

| Goal | Calendar |
|------|----------|
| Scope A | **4–5 days** |
| Scope B | **8–10 days** |
| Scope C | **12–15 days** |

### Full-time sprint (~6–8 hours / day)

| Goal | Calendar |
|------|----------|
| Scope A | **2–3 days** |
| Scope B | **5–7 days** |
| Scope C | **8–10 days** |

Add **20–30% buffer** for CI, Docker, and integration-test debugging.

---

## Phase 1 — Recruiter-ready visibility ✅ Complete

**Goal:** Anyone can clone, run, and understand the project in under 5 minutes.  
**Effort:** ~6–8 hours | **Scope:** A

| # | Task | Est. |
|---|------|------|
| 1.1 | Write full `README.md` (architecture, quick start, star schema, sample SQL) | 3–4 h |
| 1.2 | Add `.env.example` | 30 m |
| 1.3 | Add `Makefile` (`db-up`, `run`, `test`, `lint`, `format`) | 1–2 h |
| 1.4 | Fix `requirements.txt` (duplicate pytest, unused pandas) | 30 m |
| 1.5 | Add `pyproject.toml` for tool config (optional) | 1 h |

**Milestone:** Repo looks professional without reading source code.

---

## Phase 2 — Real team project signals ✅ Complete

**Goal:** Green CI, containerized stack, complete unit test coverage.  
**Effort:** ~12–18 hours | **Scope:** A

| # | Task | Est. |
|---|------|------|
| 2.1 | GitHub Actions: lint + test + coverage | 2–4 h |
| 2.2 | `Dockerfile` + full `docker-compose` (Postgres + ETL + schema init) | 3–5 h |
| 2.3 | `black`, `flake8`/`ruff`, `pre-commit` | 1–2 h |
| 2.4 | `tests/test_load.py` | 3–4 h |
| 2.5 | `tests/test_utils.py`, `tests/test_pipeline.py` | 2–3 h |
| 2.6 | CI badge in README | 15 m |

**Milestone:** `docker compose up` + green GitHub Actions on every push.

---

## Phase 3 — Mid-level data engineering depth ✅ Complete

**Goal:** Production-style patterns recruiters ask about in interviews.  
**Effort:** ~25–40 hours (core ~15–20 h) | **Scope:** B

### Core (do these)

| # | Task | Est. |
|---|------|------|
| 3.1 | Pydantic models for API + transformed records | 3–4 h |
| 3.2 | `weather.etl_runs` metadata table + logging in `main.py` | 2–3 h |
| 3.3 | Structured JSON logging (`run_id`, stage, duration) | 2–3 h |
| 3.4 | `sql/analytics_examples.sql` | 1–2 h |
| 3.5 | Integration test with Postgres (CI service container) | 4–8 h |
| 3.6 | Parallel extract + rate limiting + exit codes | 4–6 h |

### Optional (skip if time is tight)

| # | Task | Est. |
|---|------|------|
| 3.7 | SQLAlchemy ORM models + loader refactor | 6–10 h |
| 3.8 | Alembic migrations | 3–5 h |
| 3.9 | Dead-letter / quarantine table | 2 h |
| 3.10 | Daily aggregates / `date_dim` | 2–4 h |
| 3.11 | Prometheus metrics | 3–4 h |

**Milestone:** Can explain idempotency, observability, test pyramid, and show analytics queries.

---

## Phase 4 — Differentiators (pick 2–3)

**Goal:** Extra talking points; not required for most roles.  
**Effort:** ~15–24 hours (all items) | **Scope:** C

| Track | Tasks | Est. |
|-------|-------|------|
| **API** | FastAPI read layer + docker service + httpx tests | 6–10 h |
| **Ops** | GitHub Actions cron (nightly ETL) | 1–2 h |
| **Quality** | SQL data tests (`unique`, `not_null`, FK relationships) | 2–3 h |
| **Engineering** | Full type hints + mypy in CI | 4–6 h |
| **Docs** | `docs/adr/` (star schema, idempotency, mocking) | 2–3 h |

**Pick one combo:**

- **Data engineer focus:** cron + SQL data tests + `etl_runs` (from Phase 3)
- **Full-stack signal:** FastAPI + analytics SQL ✅
- **Platform signal:** Docker + CI + structured logs + health endpoint

---

## Phase 5 — Portfolio polish ✅ Complete

**Goal:** Resume, LinkedIn, and live demo alignment.  
**Effort:** ~2–4 hours | **Scope:** C

| # | Task | Est. |
|---|------|------|
| 5.1 | `LICENSE` (MIT) | 15 m |
| 5.2 | Coverage + CI badges in README | 30 m |
| 5.3 | Terminal screenshot / example output | 30 m |
| 5.4 | 2-minute demo script (interview talking points) | 1 h |
| 5.5 | Resume / LinkedIn bullets mapped to repo | 1 h |

---

## Week-by-week plan (default: 2–3 h/day → Scope B in ~6 weeks)

| Week | Days | Deliverables |
|------|------|----------------|
| **1** | Mon–Sun | README, `.env.example`, Makefile, clean `requirements.txt` |
| **2** | Mon–Sun | GitHub Actions green, black/flake8, `test_load.py` |
| **3** | Mon–Wed | Dockerfile, compose with schema init, loader + pipeline tests |
| **3** | Thu–Sun | Docker docs in README, CI badge |
| **4** | Mon–Sun | Pydantic validation, `etl_runs` table |
| **5** | Mon–Sun | Structured logging, `analytics_examples.sql` |
| **6** | Mon–Sun | Integration test + Postgres in CI |
| **7** | Mon–Sun | FastAPI **or** cron + SQL data tests (your choice) |
| **8** | Mon–Sun | License, badges, demo script, resume bullets |

---

## Already complete (do not redo)

- [x] ETL modules: `extract`, `transform`, `load`
- [x] PostgreSQL star schema + `latest_weather` view
- [x] OpenWeather REST + retry decorator
- [x] Environment config (`python-dotenv`, `Config.validate()`)
- [x] File + console logging
- [x] Unit tests: extract, transform (`pytest` + `unittest.mock`)
- [x] `docker-compose` for Postgres

---

## Skip list (saves ~15–25 hours)

If deadlines are tight, defer these without hurting most job posts:

- Great Expectations
- Prometheus / metrics endpoint
- `date_dim` and materialized views
- Full ORM refactor (keep raw SQL; document why)
- Airflow cluster (use GitHub cron instead)
- Every Phase 4 item (choose **one** track only)
- mypy everywhere (add later)

---

## Interview checkpoints

After each scope, you should be able to say:

| After | You can explain |
|-------|-----------------|
| **Scope A** | ETL flow, star schema, Docker, CI, pytest mocking |
| **Scope B** | + idempotency, run metadata, validation, integration tests |
| **Scope C** | + API layer, scheduling, ADRs, data quality SQL |

---

## Files in this repo

| File | Purpose | On GitHub? |
|------|---------|------------|
| `ROADMAP.md` | This timeline (high level) | Yes |
| `PROJECT_TODOS.md` | Detailed checkboxes | No (local only, `.gitignore`) |
| `README.md` | Public project docs | Yes (to be written) |

---

## Quick start for today

1. Open `PROJECT_TODOS.md` and mark Phase 1 items as you go.
2. **Day 1–2:** README + `.env.example`
3. **Day 3–4:** Makefile + requirements cleanup
4. **Day 5–7:** GitHub Actions + first green build

---

*Last updated: 2026-05-21*
