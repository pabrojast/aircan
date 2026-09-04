"""Weekly atomic updater for the continental SWOT DAWG reference."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from airflow.decorators import dag, task
from airflow.models import Variable

from swot_dawg_update import update_dawg_reference


@dag(
    dag_id="swot_dawg_update", schedule_interval="0 6 * * 0",
    start_date=datetime(2026, 9, 6), catchup=False, max_active_runs=1,
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=30)},
    tags=["swot", "dawg", "earthdata", "azure", "unesco"],
)
def swot_dawg_update():
    @task(execution_timeout=timedelta(hours=12))
    def update() -> dict:
        configured = str(Variable.get("SWOT_DAWG_CONTINENTS", default_var="AF,AS,EU,NA,OC,SA"))
        continents = [item.strip().upper() for item in configured.split(",") if item.strip()]
        return update_dawg_reference(
            continents=continents,
            timeout=int(Variable.get("SWOT_DAWG_TIMEOUT_S", default_var="60")),
        )

    update()


dag = swot_dawg_update()
