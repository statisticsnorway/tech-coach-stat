# AGENTS.md

## What this is

Meteorological statistics pipeline for Statistics Norway (SSB). Collects weather data from the Frost API, transforms through Norwegian-named data states (`kildedata` → `pre-inndata` → `inndata` → `klargjorte-data` → `pre-edit` → EimerDB). Templated from `ssb-project-template-stat` v1.8.0 via cruft.

## Setup

- Python >=3.12, Poetry >=2.2 (uses `dependency-groups` syntax, not old `[tool.poetry.group]`)
- `poetry install`
- Frost API client ID lives in `.env` (`FROST_CLIENT_ID`)

## Dev commands

```
poetry run pytest                                          # all tests
poetry run pytest -v --cov --cov-report=term-missing       # with coverage
poetry run pytest tests/test_file_abstraction.py           # single test file
poetry run pytest tests/test_a_collect_data.py::test_name  # single test
poetry run ruff check                                      # lint
poetry run ruff check --fix                                # lint + auto-fix
poetry run black .                                         # format
poetry run mypy src                                        # typecheck (strict mode)
poetry run pre-commit run --all-files                      # ruff + black + file checks
poetry run python tests/check_naming_standard.py           # SSB naming standard check
poetry run python src/notebooks/run_all.py                 # full pipeline
poetry run python src/notebooks/a_collect_data.py          # single step
```

## Code structure

- `src/functions/` — core library (file I/O, platform detection, versioning, query)
- `src/notebooks/` — pipeline steps a–f, runnable as scripts or Jupyter via Jupytext
- `src/schemas/` — Pandera validation schemas
- `config/` — Dynaconf settings (4 environments: `default`, `default_test`, `daplalab_files`, `local_files`)
- `tests/` — pytest; test data in `tests/testdata/`
- `experimental/` — prototyping zone, excluded from SonarQube analysis

## Critical quirks to follow

- **Dual path types:** `pathlib.Path` = local filesystem, plain `str` = GCS `gs://...` paths. Every file I/O function dispatches on the type. Never mix them up.
- **Notebooks are .py files:** `.ipynb` is gitignored. Notebooks live as `src/notebooks/*.py` in Jupytext percent format. Edit as `.py`, not `.ipynb`.
- **Kildomat is self-contained:** `b_kildomat.py` duplicates `file_abstraction.py` functions intentionally — it runs in a separate Docker container on Dapla without access to `src/functions/`.
- **Environment switching:** Change `env=` in `config/config.py` (currently `"default"`, use `"local_files"` for local development). Each env casts directory values differently (GCS str, DaplaLab Path, local absolute Path).
- **File versioning:** Pattern is `<name>_p<YYYY-MM-DD>_v<N>.<ext>`. Use `src/functions/versions.py` utilities.
- **Mypy is strict:** `strict = true` plus `warn_unreachable = true`. Add type annotations to all new code.
- **Ruff enforces Google-style docstrings, type annotations, isort (single-line), modern Python syntax.** Tests are exempted from most annotation/docstring rules.
- **7-day PyPI cooldown:** Poetry and uv both configured to wait 7 days before adopting new package releases.
- **Dapla-only tests:** Some tests (`test_versions_get_latest_bucket.py`, parts of `test_file_abstraction.py`) skip when not on Dapla. Dapla means `env="default"` in `config/config.py`. 

## Pipeline data states (Norwegian)

| Step | Input | Output | Script |
|------|-------|--------|--------|
| A | Frost API | `kildedata/` | `a_collect_data.py` |
| B | `kildedata/` | `pre-inndata/` | `b_kildomat.py` |
| C | `pre-inndata/` | `inndata/` | `c_pre_inndata_to_inndata.py` |
| D | `inndata/` | `pre-edit/` | `d_prepare_edit.py` |
| E | `inndata/` | EimerDB | `e_create_eimerdb_*.py` |
| F | EimerDB data | EimerDB | `f_to_eimerdb.py` |

## CI

GitHub Actions: `poetry install` → `pytest -v --cov --cov-report=xml` → SonarQube Cloud scan. Runs on PRs and merge to main/master.
