# Contributing

Thanks for your interest in this project. It is primarily a **portfolio / learning** repository, but small improvements are welcome.

## Getting started

```bash
git clone https://github.com/Makhosetive7/weather-etl-pipeline.git
cd weather-etl-pipeline
cp .env.example .env   # add OPENWEATHER_API_KEY
make install-dev
make db-init
```

## Before opening a PR

```bash
make format
make lint
make test
```

Integration tests (optional, requires Docker):

```bash
make test-integration
```

## Scope

Please keep PRs focused. Good contributions:

- Bug fixes with tests
- Documentation improvements
- Small, clear enhancements to ETL reliability or API ergonomics

Large refactors (ORM migration, Airflow cluster, etc.) are better discussed in an issue first.

## Code style

- Python 3.10+
- `black` (line length 100)
- `flake8` as configured in `.flake8`

## Questions

Open a GitHub issue for bugs or suggestions.
