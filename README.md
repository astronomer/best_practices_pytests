# Airflow DAG Best-Practice Pytests

This project contains pytest checks for common Apache Airflow DAG authoring best practices. The goal is to catch DAG patterns that hurt parsing performance, scheduler reliability, security, or operational predictability before code is deployed.

The repository is also a fixture project: `dags/bad_*.py` files intentionally contain bad DAG-authoring patterns so each test has an example failure.

## Project Layout

- `tests/` — pytest checks that can be copied into an Astro/Airflow project.
- `tests/conftest.py` — shared pytest helpers and fixtures required by the test files.
- `dags/bad_*.py` — intentionally bad example DAGs used to demonstrate failures.
- `dags/exampledag.py` — standard example DAG.
- `Dockerfile` — Astro Runtime image for this project.

## Running The Tests

Run collection without requiring Airflow:

```bash
pytest --collect-only -q tests
```

Run the suite:

```bash
pytest -q tests
```

Most tests are static AST checks and do not import or execute DAG files. Tests that require Airflow's `DagBag` skip automatically if Airflow is not installed. Tests that execute DAG files are opt-in and skip by default.

Because this repository includes intentionally bad DAG fixtures, many tests are expected to fail here. In a real Astro project, copy the tests into the project and run them against your production DAGs.

## Test Categories

### Static Tests

These tests parse DAG source code with Python's `ast` module. They are safe to run in normal Python CI because they do not execute DAG files.

| Test | Purpose | Configuration |
| --- | --- | --- |
| `test_duplicate_dag_ids.py` | Detects duplicate static DAG IDs across files. Supports `DAG(...)`, `@dag(...)`, and simple string constants. | None |
| `test_no_globals_injection.py` | Prevents registering DAGs with `globals()[...] = dag`, which hides structure and makes DAG discovery fragile. | None |
| `test_no_hardcoded_secrets.py` | Flags hardcoded secrets, tokens, API keys, and credential-bearing connection strings. | None |
| `test_no_metadata_db_access.py` | Flags direct access to Airflow's metadata DB session APIs, such as `airflow.settings.Session`, `create_session`, and queries on sessions created from those APIs. | None |
| `test_no_nested_loops.py` | Limits static task creation from parse-time loops and recommends dynamic task mapping for large fan-outs. | `AIRFLOW_STATIC_TASK_CREATION_LIMIT`, default `50` |
| `test_no_subdags.py` | Blocks `SubDagOperator` and subdag imports; use TaskGroups or separate DAGs instead. | None |
| `test_no_top_level_execution.py` | Flags top-level side effects such as file I/O, network calls, subprocess calls, sleeps, and Airflow Variable/Connection reads. | None |
| `test_non_deterministic_dag_ids.py` | Prevents random or wall-clock-derived DAG IDs. Handles common aliases for `uuid`, `random`, `datetime`, `pendulum`, and `time`. | None |
| `test_sensor_modes.py` | Requires long-running sensors to use `mode="reschedule"` or `deferrable=True` so worker slots are not held while waiting. | `AIRFLOW_LONG_SENSOR_POKE_INTERVAL_SECONDS`, default `300`; `AIRFLOW_LONG_SENSOR_TIMEOUT_SECONDS`, default `1800` |
| `test_static_start_date.py` | Prevents dynamic `start_date` values such as `datetime.now()`, `pendulum.now()`, and `days_ago(...)`. | None |

### DagBag Tests

These tests load DAGs with Airflow's `DagBag`, so they require Airflow to be installed in the test environment. If Airflow is unavailable, they skip automatically.

| Test | Purpose | Configuration |
| --- | --- | --- |
| `test_catchup_disabled.py` | Requires DAGs to set `catchup=False` to avoid accidental backfills on first deploy. | None |
| `test_task_count_limit.py` | Fails DAGs with too many tasks, which can slow parsing, scheduling, and UI rendering. | `AIRFLOW_DAG_TASK_COUNT_LIMIT`, default `50` |

### Opt-In Runtime/Profiling Tests

These tests execute DAG files to measure import behavior. They are useful diagnostically but should not run by default because importing DAG files can trigger bad top-level code.

| Test | Purpose | How To Enable | Configuration |
| --- | --- | --- | --- |
| `test_import_time_limit.py` | Measures total import time per DAG file. | `AIRFLOW_RUN_IMPORT_TIMING_TESTS=1` | `AIRFLOW_DAG_IMPORT_TIME_LIMIT_S`, default `1.0` |
| `test_dag_parse_profiling.py` | Instruments top-level statements and reports slow lines. | `AIRFLOW_RUN_PARSE_PROFILING_TESTS=1` | `AIRFLOW_DAG_IMPORT_TIME_LIMIT_S`, default `1.0`; `AIRFLOW_STATEMENT_TIME_LIMIT_S`, default `0.5` |

## Configuration Reference

Set these environment variables in local runs or CI to tune policy thresholds:

| Environment variable | Default | Used by | Description |
| --- | --- | --- | --- |
| `AIRFLOW_STATIC_TASK_CREATION_LIMIT` | `50` | `test_no_nested_loops.py` | Maximum estimated number of static tasks that can be created from parse-time loops before the test fails. Lower this to push teams toward dynamic task mapping sooner; raise it for DAGs with intentional small static fan-outs. |
| `AIRFLOW_DAG_TASK_COUNT_LIMIT` | `50` | `test_task_count_limit.py` | Maximum total task count allowed in a parsed DAG. This is a DagBag-based backstop for DAGs that are too large for scheduler/UI performance. |
| `AIRFLOW_LONG_SENSOR_POKE_INTERVAL_SECONDS` | `300` | `test_sensor_modes.py` | Poke interval threshold, in seconds, at or above which a sensor is considered long-running and must use `mode="reschedule"` or `deferrable=True`. |
| `AIRFLOW_LONG_SENSOR_TIMEOUT_SECONDS` | `1800` | `test_sensor_modes.py` | Timeout threshold, in seconds, at or above which a sensor is considered long-running and must use `mode="reschedule"` or `deferrable=True`. |
| `AIRFLOW_DAG_IMPORT_TIME_LIMIT_S` | `1.0` | `test_import_time_limit.py`, `test_dag_parse_profiling.py` | Maximum acceptable DAG file import time in seconds. Also used as the file-level threshold before per-statement profiling reports slow statements. |
| `AIRFLOW_STATEMENT_TIME_LIMIT_S` | `0.5` | `test_dag_parse_profiling.py` | Maximum acceptable runtime for an individual top-level statement during import profiling. Statements slower than this are reported with line numbers. |
| `AIRFLOW_RUN_IMPORT_TIMING_TESTS` | unset | `test_import_time_limit.py` | Set to `1` to enable the import timing test. This test executes DAG files, so keep it disabled in default CI unless you are intentionally profiling parse performance. |
| `AIRFLOW_RUN_PARSE_PROFILING_TESTS` | unset | `test_dag_parse_profiling.py` | Set to `1` to enable per-statement import profiling. This test executes instrumented DAG files, so use it only in controlled environments. |

Example threshold configuration:

```bash
export AIRFLOW_STATIC_TASK_CREATION_LIMIT=50
export AIRFLOW_DAG_TASK_COUNT_LIMIT=50
export AIRFLOW_LONG_SENSOR_POKE_INTERVAL_SECONDS=300
export AIRFLOW_LONG_SENSOR_TIMEOUT_SECONDS=1800
export AIRFLOW_DAG_IMPORT_TIME_LIMIT_S=1.0
export AIRFLOW_STATEMENT_TIME_LIMIT_S=0.5
```

Enable opt-in profiling tests only in a controlled environment:

```bash
export AIRFLOW_RUN_IMPORT_TIMING_TESTS=1
export AIRFLOW_RUN_PARSE_PROFILING_TESTS=1
```

## Adding These Tests To An Astro Project

Copy `tests/conftest.py` and the desired `tests/test_*.py` files into your Astro project's `tests/` directory. `conftest.py` is required because the test files import shared helpers from it, including DAG file discovery, AST call-name parsing, and the `generated_dags` DagBag fixture.

Do not copy `dags/bad_*.py` into production projects. Those files are intentionally broken fixtures for this repository only.

The static tests can run in a lightweight Python CI job with only `pytest`. The DagBag tests should run inside an environment that has your Astro Runtime / Airflow dependencies installed.

Recommended rollout:

1. Start with static tests only.
2. Tune thresholds for your project.
3. Add DagBag tests in an Astro Runtime job.
4. Enable import timing/profiling tests only when investigating parser performance.

## GitHub Actions Example

This two-stage workflow runs static policy tests first, then runs the full copied suite inside the Astro Runtime Docker image.

```yaml
name: DAG best-practice tests

on:
  pull_request:
  push:
    branches: [main]

jobs:
  static-dag-policy-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        # Ensure tests/conftest.py and whichever tests/test_*.py files you use are committed.
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Install pytest
        run: pip install pytest
      - name: Run static DAG policy tests
        run: |
          pytest -q \
            tests/test_duplicate_dag_ids.py \
            tests/test_no_globals_injection.py \
            tests/test_no_hardcoded_secrets.py \
            tests/test_no_metadata_db_access.py \
            tests/test_no_nested_loops.py \
            tests/test_no_subdags.py \
            tests/test_no_top_level_execution.py \
            tests/test_non_deterministic_dag_ids.py \
            tests/test_sensor_modes.py \
            tests/test_static_start_date.py

  airflow-dagbag-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Build Astro Runtime image
        run: docker build -t astro-dag-tests .
      - name: Run DAG tests in Astro Runtime
        run: docker run --rm astro-dag-tests bash -lc "pytest -q tests"
```

If your Runtime image does not include `pytest`, install it in `requirements.txt` or adjust the Docker command:

```yaml
- name: Run DAG tests in Astro Runtime
  run: docker run --rm astro-dag-tests bash -lc "pip install pytest && pytest -q tests"
```

For faster PR feedback, you can skip the opt-in import timing/profiling tests in normal CI. They already skip by default unless explicitly enabled.

## GitLab CI Example

```yaml
stages:
  - test

static-dag-policy-tests:
  image: python:3.11
  stage: test
  # Ensure tests/conftest.py and whichever tests/test_*.py files you use are committed.
  script:
    - pip install pytest
    - >
      pytest -q
      tests/test_duplicate_dag_ids.py
      tests/test_no_globals_injection.py
      tests/test_no_hardcoded_secrets.py
      tests/test_no_metadata_db_access.py
      tests/test_no_nested_loops.py
      tests/test_no_subdags.py
      tests/test_no_top_level_execution.py
      tests/test_non_deterministic_dag_ids.py
      tests/test_sensor_modes.py
      tests/test_static_start_date.py

airflow-dagbag-tests:
  image: docker:27
  stage: test
  services:
    - docker:27-dind
  variables:
    DOCKER_TLS_CERTDIR: ""
  script:
    - docker build -t astro-dag-tests .
    - docker run --rm astro-dag-tests bash -lc "pytest -q tests"
```

## Using With Astro Deploy Pipelines

Add these tests before `astro deploy` in your deployment workflow. The tests should be a deployment gate: if DAG policy tests fail, do not deploy.

Example deployment sequence:

```bash
pytest -q tests
docker build -t astro-dag-tests .
docker run --rm astro-dag-tests bash -lc "pytest -q tests"
astro deploy <deployment-id>
```

Only run `astro deploy` after the test jobs pass.

## Writing New Checks

Prefer static AST checks when possible:

- They do not execute DAG files.
- They run without Airflow installed.
- They are safe for CI and pull requests.

Use DagBag checks when the rule requires Airflow's parsed DAG objects, such as `catchup`, task counts, schedules, or task metadata.

Avoid import-time tests unless the purpose is explicitly to measure import performance. If a test must execute DAG files, make it opt-in with an environment variable and document the risk.
