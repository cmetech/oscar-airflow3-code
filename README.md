# oscar-airflow3-code

OSCAR Airflow 3 DAG and plugin repository. Targets **Apache Airflow 3.1.8+**.

This repo is the AF3 branch of the OSCAR workflow engine code. It is synced into `oscar/oscar-workflow/airflow/` at build time via:

```bash
./oscar workflow sync          # auto-detects version from versions.env
./oscar workflow sync --v3     # force AF3
```

---

## Airflow Version Target

| Item | Value |
|---|---|
| Airflow version | 3.1.8+ |
| Alembic schema head | `509b94a1042d` |
| Python | 3.12 |
| Container | `airflow-apiserver` (not `airflow-webserver`) |
| Auth token URL | `http://airflow-apiserver:6080/airflow/auth/token` |

---

## Import Conventions (AF3.1.8)

AF3.1.8 moved core classes to `airflow.sdk` and standard operators to `airflow.providers.standard`. Always use these paths:

```python
# Operators — airflow.providers.standard.*
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# Base classes — airflow.sdk.bases.*
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.bases.operator import BaseOperator

# Still at original paths (not moved)
from airflow import DAG
from airflow.models import Variable, Connection, TaskInstance
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException, AirflowException

# Provider hooks (unchanged)
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.oracle.hooks.oracle import OracleHook
```

### Old paths that generate DeprecationWarning in AF3.1.8

| Old (do not use) | New |
|---|---|
| `airflow.hooks.base.BaseHook` | `airflow.sdk.bases.hook.BaseHook` |
| `airflow.models.baseoperator.BaseOperator` | `airflow.sdk.bases.operator.BaseOperator` |
| `airflow.operators.python.PythonOperator` | `airflow.providers.standard.operators.python.PythonOperator` |
| `airflow.operators.bash.BashOperator` | `airflow.providers.standard.operators.bash.BashOperator` |
| `airflow.operators.empty.EmptyOperator` | `airflow.providers.standard.operators.empty.EmptyOperator` |
| `airflow.operators.trigger_dagrun.TriggerDagRunOperator` | `airflow.providers.standard.operators.trigger_dagrun.TriggerDagRunOperator` |

---

## Context Variables

AF3 renamed `execution_date` to `logical_date`. Always use the fallback pattern:

```python
# Correct — works on AF3, safe on any shared code path
logical_date = context.get('logical_date') or context.get('execution_date')

# Wrong — generates DeprecationWarning in AF3.1.8
value = context['execution_date']
```

Context variables that are still valid: `ti`, `dag`, `dag_run`, `ds`, `run_id`, `task`, `params`.

---

## Removed in AF3 (never use in this repo)

| Removed | AF2 equivalent |
|---|---|
| `provide_context=True` | Remove entirely — context always passed |
| `@apply_defaults` | Remove decorator |
| `schedule_interval=` | Use `schedule=` |
| `from airflow.utils.dates import days_ago` | Use `datetime(...)` directly |
| `from airflow.operators.dummy import DummyOperator` | Use `EmptyOperator` |

---

## Repo Structure

```
dags/           # DAG files — one per workflow
plugins/
  custom_operators/   # Custom Airflow operators
  helpers/            # Business logic helpers (imported by DAGs)
  hooks/              # Custom Airflow hooks (HTTP, DB, alert, worklog)
statsd/         # StatsD metrics mapping config
```

---

## Compatibility Validation

Run from repo root — all must return 0 results before pushing:

```bash
# Removed APIs
grep -rn "provide_context\|apply_defaults\|schedule_interval\|days_ago\|from airflow\.utils\.dates" . --include="*.py"

# Old import paths (DeprecationWarning)
grep -rn "from airflow\.hooks\.base import\|from airflow\.operators\.python import\|from airflow\.operators\.bash import\|from airflow\.operators\.empty import\|from airflow\.models import BaseOperator\|from airflow\.models\.baseoperator import" . --include="*.py"

# context execution_date direct access
grep -rn "context\[.execution_date.\]" . --include="*.py"
```
