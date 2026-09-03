"""Read-only Airflow test DAG for the optimized Dnipro SWOT node updater.

This deliberately uses a distinct DAG ID and a fixed sample size, requires no
new Airflow Variables, and performs no writes to Azure or CKAN.
"""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

from azure.storage.blob import ContainerClient

# Airflow may discover this file from a nested Git-synced DAG directory without
# putting that directory on sys.path. Make the sibling updater import explicit.
THIS_DAG_DIR = str(Path(__file__).resolve().parent)
if THIS_DAG_DIR not in sys.path:
    sys.path.insert(0, THIS_DAG_DIR)

import dnipro_swot_nodes_hydrocron_update as updater

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except Exception:
    DAG = None
    PythonOperator = None


TEST_NODE_LIMIT = 1000
TEST_WORKERS = 8


def run_optimized_node_test(**_context):
    connection = str(updater.vget("AZURE_STORAGE_CONNECTION_STRING", "")).strip()
    if not connection:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is required")

    timeout = int(updater.vget("SWOT_NODE_TIMEOUT_S", updater.DEFAULT_TIMEOUT))
    retries = int(updater.vget("SWOT_NODE_MAX_RETRIES", updater.DEFAULT_RETRIES))
    overlap = updater.DEFAULT_OVERLAP_HOURS
    container = ContainerClient.from_connection_string(connection, updater.AZURE_CONTAINER)
    if container.account_name != updater.AZURE_ACCOUNT:
        raise RuntimeError(f"Refusing unexpected Azure account {container.account_name!r}")
    container.get_container_properties()

    response = updater.get_with_retries(
        updater.NODE_GEOJSON_URL, timeout=timeout, retries=retries
    )
    response.raise_for_status()
    features = response.json().get("features", [])
    records = []
    seen = set()
    for feature in features:
        properties = feature.get("properties", {}) or {}
        node_id = updater.clean_id(properties.get("node_id", ""))
        if node_id and node_id not in seen:
            seen.add(node_id)
            records.append(properties)
        if len(records) == TEST_NODE_LIMIT:
            break

    state = updater.load_json_blob(container, updater.STATE_BLOB) or {}
    initial = str(updater.vget("SWOT_NODE_INITIAL_START", updater.DEFAULT_INITIAL_START))
    global_start = updater.parse_utc(state.get("last_successful_end_utc", initial))
    end = datetime.now(timezone.utc)
    started = datetime.now(timezone.utc)

    results = []
    with ThreadPoolExecutor(max_workers=TEST_WORKERS) as executor:
        futures = [
            executor.submit(
                updater.update_one,
                properties,
                container,
                global_start,
                end,
                overlap,
                timeout,
                retries,
                False,
            )
            for properties in records
        ]
        for future in as_completed(futures):
            results.append(future.result())

    counts = Counter(result.status for result in results)
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    summary = {
        "read_only": True,
        "sample_node_count": len(records),
        "workers": TEST_WORKERS,
        "elapsed_seconds": round(elapsed, 1),
        "nodes_per_second": round(len(records) / elapsed, 2) if elapsed else None,
        "status_counts": dict(counts),
        "historical_csv_downloads": sum(result.blob_downloaded for result in results),
        "would_update_csvs": counts["updated"],
        "production_writes": 0,
    }
    print(f"OPTIMIZED NODE TEST SUMMARY: {summary}")
    return summary


if DAG is not None and PythonOperator is not None:
    with DAG(
        dag_id="dnipro_swot_nodes_hydrocron_optimized_test",
        default_args={
            "owner": "airflow",
            "depends_on_past": False,
            "start_date": datetime(2026, 9, 3),
            "retries": 0,
        },
        description="Read-only 1,000-node benchmark of the optimized Dnipro node updater",
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
        tags=["swot", "nodes", "test", "read-only"],
    ) as dag:
        run_test = PythonOperator(
            task_id="run_read_only_optimized_node_test",
            python_callable=run_optimized_node_test,
        )


if __name__ == "__main__":
    run_optimized_node_test()
