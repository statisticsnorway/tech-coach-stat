# Copilot Instructions for tech-coach-stat

This guide helps AI coding agents work productively in the `tech-coach-stat` codebase. It summarizes architecture, workflows, conventions, and integration points specific to this project.

## Project Overview
- **Purpose:** Example pipeline for producing meteorological statistics, used by Statistics Norway and tech coaches for best practice experimentation.
- **Platforms:** Runs on Dapla, DaplaLab (Jupyter/vscode), Windows, and Linux.
- **Major Components:**
  - `src/functions/`: Core logic, data collection, file abstraction, versioning, and platform-specific utilities.
  - `config/`: Centralized configuration using Dynaconf (`settings.toml`, `config.py`). Supports multiple environments: `default`, `daplalab_files`, `local_files`.
  - `data/`: Input and processed data, organized by source/type.
  - `schemas/`: Pandera schemas for dataframe validation.
  - `tests/`: Pytest-based unit tests for core logic and workflows.
  - `experimental/`: Freeform code for prototyping, not subject to SonarQube analysis.

## Key Patterns & Conventions
- **Configuration:** Always use `config/settings.toml` and access via Dynaconf (`config/config.py`). Avoid hardcoding paths/parameters.
- **Environment Selection:** Set environment in `config/config.py` to switch between local, Dapla, or DaplaLab setups.
- **Secrets:** Managed via Google Secret Manager or `.env` files (see GSM references in README).
- **Data Versioning:** Utilities in `src/functions/versions.py` and related files. Track processed files and new observations.
- **Validation:** Use Pandera schemas from `schemas/` for dataframe validation.
- **Testing:** All logic should be covered by unit tests in `tests/`. Use `pytest` for running tests. Test data is in `tests/testdata/`. Use the file `tests/test_a_collect_data.py` as pattern when adding new tests.
- **Experimental Code:** Place prototypes in `experimental/`. Organize by contributor if needed. Not included in code quality checks.
- **Documentation:** Ensure all public functions and classes are documented. Use docstrings in google format.
- **Python version:** Assume use of Python 3.11 or later, if not specified otherwise in `pyproject.toml`. Use modern type hints using python 3.11 features.

## Developer Workflows
- **Run Tests:**
  ```powershell
  poetry run pytest
  ```
- **Build/Run Notebooks:** Use Jupyter or VS Code for notebooks in `src/notebooks/`. The files are
stored as .py files in jupytext percent format. Notebooks orchestrate data collection and processing steps.
- **Code Quality:** SonarQube Cloud analyzes all code except `experimental/`. See `sonar-project.properties` for config.
- **Automated Testing:** GitHub Actions run tests on push/PR. Ensure tests pass locally before committing.

## Integration Points
- **External APIs:** Data fetched from Meteorologisk institutt's Frost API.
- **Secrets:** Google Secret Manager integration for sensitive credentials.
- **Config Management:** Dynaconf for environment-aware settings.

## Examples
- To add a new data source, create a module in `src/functions/`, update config, and add validation schema in `schemas/`.
- To switch environment, edit `config/config.py` and adjust `settings.toml` as needed.
- To validate a dataframe, use Pandera schema from `schemas/`.

## References
- See `README.md` for high-level project goals and links to standards.
- See `config/README.md` for configuration best practices.
- See `experimental/README.md` for prototyping guidelines.

---
For unclear or missing sections, ask the user for clarification or examples from their workflow.
