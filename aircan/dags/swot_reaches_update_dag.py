"""Daily incremental updater for every registered SWOT reach region."""
from __future__ import annotations
import sys
from datetime import datetime, timedelta
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from airflow.decorators import dag, task
from airflow.models import Variable

def setting(name: str, default: str) -> str:
    return str(Variable.get(name, default_var=default))

@dag(dag_id="swot_reaches_update", schedule_interval="0 8 * * *",
     start_date=datetime(2026, 9, 4), catchup=False, max_active_runs=1,
     default_args={"owner": "airflow", "retries": 2, "retry_delay": timedelta(minutes=10)},
     tags=["swot", "hydrocron", "reaches", "azure", "ckan", "unesco"])
def swot_reaches_update():
    @task
    def discover() -> list[dict[str, str]]:
        from swot_reaches_update import discover_reach_regions
        return discover_reach_regions(region_filter=setting("SWOT_REACH_REGION_FILTER", "") or None)
    @task(pool="swot_hydrocron", max_active_tis_per_dag=2)
    def update_region(region: dict[str, str]) -> dict:
        from swot_reaches_update import update_reach_region
        return update_reach_region(region=region,
            batch_size=int(setting("SWOT_REACH_BATCH_SIZE", "500")),
            overlap_hours=int(setting("SWOT_REACH_OVERLAP_HOURS", "48")),
            timeout=int(setting("SWOT_REACH_TIMEOUT_S", "60")),
            retries=int(setting("SWOT_REACH_REQUEST_RETRIES", "5")),
            request_workers=int(setting("SWOT_REACH_REQUEST_WORKERS", "4")))
    update_region.expand(region=discover())

dag = swot_reaches_update()
