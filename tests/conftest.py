"""Host test lane for DAG helper packages. Airflow is NOT importable here —
only pure packages under dags/ (splunk_closed_loop_lib) are tested."""
import sys
from pathlib import Path

DAGS = Path(__file__).resolve().parents[1] / "dags"
if str(DAGS) not in sys.path:
    sys.path.insert(0, str(DAGS))
