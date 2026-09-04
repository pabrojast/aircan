"""Thin Airflow DAG for all registered Azure SWOT node products."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Airflow loads this file from ``aircan/dags`` but this repository's shared
# updater module lives at ``aircan/swot_nodes_update.py``. Some deployments do
# not place the repository root on sys.path automatically.
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from airflow.decorators import dag, task
from airflow.models import Variable

from swot_nodes_update import discover_node_regions, update_node_region


def setting(name: str, default: str) -> str:
    return str(Variable.get(name, default_var=default))


@dag(
    dag_id="swot_nodes_update",
    description="Batched incremental update of every registered Azure SWOT node product",
    schedule_interval="0 10 * * *",
    start_date=datetime(2026, 9, 4),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["swot", "hydrocron", "nodes", "azure", "ckan", "unesco"],
)
def swot_nodes_update():
    @task
    def discover() -> list[dict[str, str]]:
        return discover_node_regions(
            region_filter=setting("SWOT_NODE_REGION_FILTER", "") or None
        )

    @task(pool="swot_hydrocron", max_active_tis_per_dag=2)
    def update_region(region: dict[str, str]) -> dict:
        return update_node_region(
            region=region,
            batch_size=int(setting("SWOT_NODE_BATCH_SIZE", "750")),
            overlap_hours=int(setting("SWOT_NODE_OVERLAP_HOURS", "48")),
            timeout=int(setting("SWOT_NODE_TIMEOUT_S", "60")),
            retries=int(setting("SWOT_NODE_REQUEST_RETRIES", "5")),
            request_workers=int(setting("SWOT_NODE_REQUEST_WORKERS", "4")),
        )

    regions = discover()
    update_region.expand(region=regions)


dag = swot_nodes_update()
