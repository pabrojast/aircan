"""Scheduled AOI inbox provisioner for regional SWOT products."""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0, str(ROOT))

from airflow.decorators import dag, task
from airflow.models import Variable


def setting(name: str, default: str) -> str:
    return str(Variable.get(name, default_var=default))


def ckan_api_key() -> str:
    # Match the proven reach publisher. The project-specific credential is
    # authoritative; a stale generic CKAN_API_KEY must not shadow it.
    value = str(Variable.get(
        "IHP_WINS_CKAN_API_KEY",
        default_var=Variable.get("CKAN_API_KEY", default_var=""),
    )).strip()
    return value[1:-1] if value.startswith('"') and value.endswith('"') else value


@dag(dag_id="swot_region_provisioning", schedule_interval="*/30 * * * *",
     start_date=datetime(2026, 9, 9), catchup=False, max_active_runs=1,
     default_args={"owner": "airflow", "retries": 2, "retry_delay": timedelta(minutes=15)},
     tags=["swot", "provisioning", "aoi", "azure", "ckan", "unesco"])
def swot_region_provisioning():
    @task
    def intake() -> dict:
        from ihpwins_aoi_intake import enqueue_live_dataset
        return enqueue_live_dataset(ckan_api_key=ckan_api_key())

    @task
    def discover(_intake_result: dict) -> list[dict[str, str]]:
        from swot_region_provisioning import discover_submissions
        # The pending inbox is already an explicit work queue. Do not silently
        # suppress valid submissions through a stale deployment filter.
        return discover_submissions()

    @task(pool="swot_hydrocron", max_active_tis_per_dag=1,
          execution_timeout=timedelta(hours=24))
    def provision(descriptor: dict[str, str]) -> dict:
        from swot_region_provisioning import provision_submission
        return provision_submission(descriptor=descriptor,
            workers=int(setting("SWOT_PROVISION_WORKERS", "8")),
            timeout=int(setting("SWOT_PROVISION_TIMEOUT_S", "60")),
            retries=int(setting("SWOT_PROVISION_REQUEST_RETRIES", "5")),
            ckan_api_key=ckan_api_key())

    provision.expand(descriptor=discover(intake()))


dag = swot_region_provisioning()
