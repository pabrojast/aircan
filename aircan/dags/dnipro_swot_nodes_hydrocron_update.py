"""Airflow DAG / standalone job for daily incremental Dnipro SWOT node updates.

This is the node counterpart to the existing production reach updater. It:
loads node IDs from the published GeoJSON, queries Hydrocron from each node's
latest timestamp with an overlap window, applies the established Version D
quality filter, merges/deduplicates observations, replaces changed Azure CSV
blobs, refreshes node availability metadata, updates the existing Azure and
CKAN GeoJSON objects, and writes an Azure audit log.
"""

from __future__ import annotations

import io
import json
import logging
import os
import random
import tempfile
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContainerClient, ContentSettings
from pandas.errors import EmptyDataError

try:
    from airflow import DAG
    from airflow.models import Variable
    from airflow.operators.python import PythonOperator
except Exception:  # Allows validation and standalone execution outside Airflow.
    DAG = None
    PythonOperator = None

    class Variable:  # type: ignore[override]
        @staticmethod
        def get(_key: str) -> str:
            raise RuntimeError("Airflow Variable.get unavailable outside Airflow")


logger = logging.getLogger(__name__)

CKAN_BASE = "https://ihp-wins.unesco.org"
NODE_RESOURCE_ID = "11343941-0e2c-48fc-af8e-9baa4ec28c49"
NODE_GEOJSON_URL = (
    f"{CKAN_BASE}/dataset/811c5aef-99e8-46e8-a708-12972138b70d/"
    f"resource/{NODE_RESOURCE_ID}/download/dnipro_sword_nodes_version_d.geojson"
)
NODE_GEOJSON_FILENAME = "dnipro_sword_nodes_version_d.geojson"

HYDROCRON_URL = "https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries"
COLLECTION = "SWOT_L2_HR_RiverSP_D"
FIELDS = (
    "node_id,reach_id,time_str,lat,lon,river_name,"
    "wse,wse_u,wse_r_u,width,width_u,node_q,node_q_b,ice_clim_f,xovr_cal_q,"
    "cycle_id,pass_id,crid,sword_version,collection_shortname,"
    "collection_version,granuleUR"
)
OUTPUT_COLUMNS = [
    "node_id", "reach_id", "time_utc", "lat", "lon", "river_name",
    "wse", "wse_u", "wse_r_u", "wse_units", "wse_u_units", "wse_r_u_units",
    "width", "width_u", "width_units", "width_u_units", "node_q", "node_q_b",
    "ice_clim_f", "xovr_cal_q", "cycle_id", "pass_id", "crid", "sword_version",
    "collection_shortname", "collection_version", "granuleUR",
]
REQUIRED_FILTER_FIELDS = {"time_str", "wse", "ice_clim_f", "node_q", "node_q_b", "xovr_cal_q"}
REJECT_NODE_Q_BITS = (13, 14, 19, 23)
FILL_ABS_THRESHOLD = 1.0e10

AZURE_ACCOUNT = "ihpwinsdata"
AZURE_CONTAINER = "swot"
AZURE_PREFIX = "regions/dnipro/nodes"
CSV_PREFIX = f"{AZURE_PREFIX}/timeseries"
GEOJSON_BLOB = f"{AZURE_PREFIX}/nodes.geojson"
LOG_BLOB = "regions/dnipro/logs/node_daily_update.csv"
SUMMARY_BLOB = "regions/dnipro/logs/node_daily_update_summary.json"
STATE_BLOB = "regions/dnipro/logs/node_incremental_state.json"
PUBLIC_CSV_BASE = f"https://{AZURE_ACCOUNT}.blob.core.windows.net/{AZURE_CONTAINER}/{CSV_PREFIX}"

DEFAULT_OVERLAP_HOURS = 48
# The completed historical run ended at this instant. It is only used until a
# successful incremental state file exists.
DEFAULT_INITIAL_START = "2026-08-31T18:46:32Z"
DEFAULT_WORKERS = 8
DEFAULT_TIMEOUT = 60
DEFAULT_RETRIES = 5
_thread_local = threading.local()


@dataclass
class NodeUpdate:
    node_id: str
    status: str
    start_time_utc: str = ""
    end_time_utc: str = ""
    input_rows: int = 0
    accepted_rows: int = 0
    previous_rows: int = 0
    final_rows: int = 0
    first_observation_utc: str = ""
    latest_observation_utc: str = ""
    message: str = ""


def vget(key: str, default: Any) -> Any:
    try:
        value = Variable.get(key)
        return default if value is None or str(value).strip() == "" else value
    except Exception:
        return os.environ.get(key, default)


def utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(value: str) -> datetime:
    parsed = pd.to_datetime(value, errors="raise", utc=True)
    return parsed.to_pydatetime()


def clean_id(value: Any) -> str:
    text = str(value).strip()
    return text[:-2] if text.endswith(".0") else text


def safe_id(value: Any) -> str:
    cleaned = "".join(ch for ch in clean_id(value) if ch.isalnum() or ch in "-_")
    if not cleaned:
        raise ValueError(f"Unsafe node identifier {value!r}")
    return cleaned


def http_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
    return _thread_local.session


def get_with_retries(url: str, *, params=None, timeout=DEFAULT_TIMEOUT, retries=DEFAULT_RETRIES):
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = http_session().get(url, params=params, timeout=timeout)
            if response.status_code in {429, 500, 502, 503, 504}:
                raise RuntimeError(f"transient HTTP {response.status_code}")
            return response
        except Exception as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(min(30, 0.8 * 2**attempt) + random.uniform(0, 0.5))
    raise RuntimeError(f"request failed after {retries} attempts: {last_error}")


def response_frame(text: str) -> pd.DataFrame:
    payload = (text or "").strip()
    if not payload:
        return pd.DataFrame()
    if payload.startswith("{"):
        obj = json.loads(payload)
        payload = str(obj.get("results", {}).get("csv", "") or "").strip()
    if not payload:
        return pd.DataFrame()
    try:
        return pd.read_csv(io.StringIO(payload), dtype={"node_id": "string", "reach_id": "string"})
    except EmptyDataError:
        return pd.DataFrame()


def read_csv_bytes(data: bytes | None) -> pd.DataFrame:
    if not data:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    try:
        return pd.read_csv(io.BytesIO(data), dtype={"node_id": "string", "reach_id": "string"})
    except EmptyDataError:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)


def filter_nodes(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    frame = frame.copy()
    if "xovr_cal_q" not in frame and "xover_cal_q" in frame:
        frame = frame.rename(columns={"xover_cal_q": "xovr_cal_q"})
    missing = sorted(REQUIRED_FILTER_FIELDS - set(frame.columns))
    if missing:
        raise ValueError(f"Hydrocron response missing filter fields: {missing}")

    times = pd.to_datetime(frame["time_str"], errors="coerce", utc=True)
    ice = pd.to_numeric(frame["ice_clim_f"], errors="coerce")
    quality = pd.to_numeric(frame["node_q"], errors="coerce")
    crossover = pd.to_numeric(frame["xovr_cal_q"], errors="coerce")
    wse = pd.to_numeric(frame["wse"], errors="coerce")
    bits_numeric = pd.to_numeric(frame["node_q_b"], errors="coerce")
    bits_valid = bits_numeric.notna() & bits_numeric.ge(0) & bits_numeric.map(
        lambda value: float(value).is_integer() if pd.notna(value) else False
    )
    bits = bits_numeric.fillna(-1).astype("int64")
    accepted = (
        times.notna() & ice.eq(0) & quality.lt(3) & crossover.lt(2)
        & wse.notna() & wse.abs().lt(FILL_ABS_THRESHOLD) & bits_valid
    )
    for bit in REJECT_NODE_Q_BITS:
        accepted &= (bits & (1 << bit)).eq(0)

    output = frame.loc[accepted].copy()
    output["time_utc"] = times.loc[output.index].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    for column in ("node_id", "reach_id"):
        if column in output:
            output[column] = output[column].map(clean_id)
    for column in OUTPUT_COLUMNS:
        if column not in output:
            output[column] = pd.NA
    return (
        output[OUTPUT_COLUMNS]
        .drop_duplicates(["node_id", "time_utc", "cycle_id", "pass_id"], keep="last")
        .sort_values("time_utc")
        .reset_index(drop=True)
    )


def merge_nodes(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    frames = [frame for frame in (existing, incoming) if frame is not None and not frame.empty]
    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    output = pd.concat(frames, ignore_index=True, sort=False)
    for column in OUTPUT_COLUMNS:
        if column not in output:
            output[column] = pd.NA
    output = output[OUTPUT_COLUMNS]
    return (
        output.drop_duplicates(["node_id", "time_utc", "cycle_id", "pass_id"], keep="last")
        .sort_values("time_utc")
        .reset_index(drop=True)
    )


def csv_bytes(frame: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    frame.to_csv(buffer, index=False, lineterminator="\n")
    return buffer.getvalue().encode("utf-8")


def latest_time(frame: pd.DataFrame) -> datetime | None:
    if frame.empty or "time_utc" not in frame:
        return None
    values = pd.to_datetime(frame["time_utc"], errors="coerce", utc=True)
    return None if not values.notna().any() else values.max().to_pydatetime()


def observation_bounds(frame: pd.DataFrame) -> tuple[str, str]:
    if frame.empty:
        return "", ""
    values = pd.to_datetime(frame["time_utc"], errors="coerce", utc=True).dropna()
    return ("", "") if values.empty else (utc_text(values.min().to_pydatetime()), utc_text(values.max().to_pydatetime()))


def download_blob(container: ContainerClient, name: str) -> tuple[bytes | None, str | None]:
    client = container.get_blob_client(name)
    try:
        props = client.get_blob_properties()
        return client.download_blob().readall(), props.etag
    except ResourceNotFoundError:
        return None, None


def upload_blob(container: ContainerClient, name: str, data: bytes, content_type: str) -> None:
    container.get_blob_client(name).upload_blob(
        data, overwrite=True, content_settings=ContentSettings(content_type=content_type)
    )


def update_one(
    node_id: str,
    container: ContainerClient,
    global_start: datetime,
    end: datetime,
    overlap_hours: int,
    timeout: int,
    retries: int,
) -> NodeUpdate:
    blob_name = f"{CSV_PREFIX}/node_{safe_id(node_id)}.csv"
    try:
        previous_bytes, _ = download_blob(container, blob_name)
        existing = read_csv_bytes(previous_bytes)
        last = latest_time(existing)
        start = (last or global_start) - timedelta(hours=overlap_hours)
        if start >= end:
            start = end - timedelta(hours=overlap_hours)
        params = {
            "feature": "Node", "feature_id": node_id,
            "start_time": utc_text(start), "end_time": utc_text(end),
            "output": "csv", "collection_name": COLLECTION, "fields": FIELDS,
        }
        time.sleep(random.uniform(0.05, 0.2))
        response = get_with_retries(HYDROCRON_URL, params=params, timeout=timeout, retries=retries)
        if response.status_code == 400 and existing.empty:
            return NodeUpdate(node_id, "not_found", utc_text(start), utc_text(end))
        response.raise_for_status()
        raw = response_frame(response.text)
        incoming = filter_nodes(raw)
        previous_canonical = merge_nodes(
            pd.DataFrame(columns=OUTPUT_COLUMNS), existing
        )
        final = merge_nodes(existing, incoming)
        final_bytes = csv_bytes(final)
        # Compare canonical table content, not raw bytes. The historical CSVs
        # may use different newline conventions; that alone must not trigger
        # tens of thousands of unnecessary blob replacements.
        changed = final_bytes != csv_bytes(previous_canonical)
        if changed and not final.empty:
            upload_blob(container, blob_name, final_bytes, "text/csv; charset=utf-8")
        first, latest = observation_bounds(final)
        return NodeUpdate(
            node_id=node_id,
            status="updated" if changed and not final.empty else "no_change",
            start_time_utc=utc_text(start), end_time_utc=utc_text(end),
            input_rows=len(raw), accepted_rows=len(incoming), previous_rows=len(existing),
            final_rows=len(final), first_observation_utc=first, latest_observation_utc=latest,
        )
    except Exception as exc:
        return NodeUpdate(node_id=node_id, status="error", message=str(exc), end_time_utc=utc_text(end))


def load_json_blob(container: ContainerClient, name: str) -> dict | None:
    data, _ = download_blob(container, name)
    return None if data is None else json.loads(data)


def run_dnipro_swot_node_update(**_context) -> dict[str, Any]:
    connection = str(vget("AZURE_STORAGE_CONNECTION_STRING", "")).strip()
    ckan_key = str(vget("CKAN_API_KEY", os.environ.get("IHP_WINS_CKAN_API_KEY", ""))).strip()
    if not connection:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is required")
    if not ckan_key:
        raise RuntimeError("CKAN_API_KEY is required")

    workers = int(vget("SWOT_NODE_WORKERS", DEFAULT_WORKERS))
    timeout = int(vget("SWOT_NODE_TIMEOUT_S", DEFAULT_TIMEOUT))
    retries = int(vget("SWOT_NODE_MAX_RETRIES", DEFAULT_RETRIES))
    overlap = int(vget("SWOT_NODE_OVERLAP_HOURS", DEFAULT_OVERLAP_HOURS))
    limit = int(vget("SWOT_NODE_LIMIT", 0))
    if not 1 <= workers <= 32:
        raise ValueError("SWOT_NODE_WORKERS must be between 1 and 32")

    container = ContainerClient.from_connection_string(connection, AZURE_CONTAINER)
    if container.account_name != AZURE_ACCOUNT:
        raise RuntimeError(f"Refusing unexpected Azure account {container.account_name!r}")
    container.get_container_properties()

    geometry_response = get_with_retries(NODE_GEOJSON_URL, timeout=timeout, retries=retries)
    geometry_response.raise_for_status()
    geojson = geometry_response.json()
    features = geojson.get("features", [])
    node_ids = [clean_id(f.get("properties", {}).get("node_id", "")) for f in features]
    node_ids = list(dict.fromkeys(value for value in node_ids if value))
    if limit:
        node_ids = node_ids[:limit]
    if not node_ids:
        raise RuntimeError("No node IDs found in published GeoJSON")

    state = load_json_blob(container, STATE_BLOB) or {}
    initial = str(vget("SWOT_NODE_INITIAL_START", DEFAULT_INITIAL_START))
    global_start = parse_utc(state.get("last_successful_end_utc", initial))
    end = datetime.now(timezone.utc)
    logger.info("Updating %d nodes from global watermark %s to %s", len(node_ids), utc_text(global_start), utc_text(end))

    results: list[NodeUpdate] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(update_one, node_id, container, global_start, end, overlap, timeout, retries): node_id
            for node_id in node_ids
        }
        for completed, future in enumerate(as_completed(futures), 1):
            results.append(future.result())
            if completed % 500 == 0 or completed == len(futures):
                logger.info("Completed %d/%d: %s", completed, len(futures), dict(Counter(r.status for r in results)))

    result_map = {result.node_id: result for result in results}
    for feature in features:
        props = feature.setdefault("properties", {})
        result = result_map.get(clean_id(props.get("node_id", "")))
        if result is None or result.status == "error":
            continue
        props["observation_count"] = result.final_rows
        props["has_data"] = result.final_rows > 0
        props["first_observation_utc"] = result.first_observation_utc or None
        props["latest_observation_utc"] = result.latest_observation_utc or None
        if result.final_rows:
            props["url"] = f"{PUBLIC_CSV_BASE}/node_{safe_id(result.node_id)}.csv"
        else:
            props.pop("url", None)

    counts = Counter(result.status for result in results)
    changed = counts["updated"]
    errors = counts["error"]
    headers = {"Authorization": ckan_key, "X-CKAN-API-Key": ckan_key}
    if changed:
        geometry_bytes = json.dumps(geojson, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        json.loads(geometry_bytes)
        upload_blob(container, GEOJSON_BLOB, geometry_bytes, "application/geo+json; charset=utf-8")
        with tempfile.TemporaryDirectory(prefix="dnipro-node-update-") as temp_dir:
            path = Path(temp_dir) / NODE_GEOJSON_FILENAME
            path.write_bytes(geometry_bytes)
            with path.open("rb") as stream:
                response = requests.post(
                    f"{CKAN_BASE}/api/3/action/resource_update",
                    headers=headers,
                    data={"id": NODE_RESOURCE_ID, "format": "GeoJSON"},
                    files={"upload": (NODE_GEOJSON_FILENAME, stream, "application/geo+json")},
                    timeout=180,
                )
            response.raise_for_status()
            if not response.json().get("success"):
                raise RuntimeError(response.text)

    log_frame = pd.DataFrame(asdict(result) for result in sorted(results, key=lambda item: item.node_id))
    upload_blob(container, LOG_BLOB, csv_bytes(log_frame), "text/csv; charset=utf-8")
    summary = {
        "run_started_from_utc": utc_text(global_start),
        "run_finished_utc": utc_text(end),
        "node_count": len(node_ids),
        "status_counts": dict(counts),
        "ckan_resource_updated": bool(changed),
        "collection": COLLECTION,
        "overlap_hours": overlap,
    }
    upload_blob(container, SUMMARY_BLOB, json.dumps(summary, indent=2).encode(), "application/json; charset=utf-8")
    if errors == 0 and not limit:
        upload_blob(
            container,
            STATE_BLOB,
            json.dumps({"last_successful_end_utc": utc_text(end)}, indent=2).encode(),
            "application/json; charset=utf-8",
        )
    else:
        logger.warning("Watermark not advanced: errors=%d limit=%d", errors, limit)
    logger.info("Node update summary: %s", summary)
    return summary


if DAG is not None and PythonOperator is not None:
    with DAG(
        dag_id="dnipro_swot_nodes_hydrocron_update",
        default_args={
            "owner": "airflow",
            "depends_on_past": False,
            "start_date": datetime(2026, 9, 2),
            "retries": 1,
            "retry_delay": timedelta(minutes=10),
        },
        description="Daily incremental update of Dnipro SWOT node CSVs and CKAN GeoJSON",
        schedule_interval="0 10 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["swot", "hydrocron", "nodes", "azure", "ckan", "unesco", "dnipro"],
    ) as dag:
        run_update = PythonOperator(
            task_id="run_dnipro_swot_node_update",
            python_callable=run_dnipro_swot_node_update,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run_dnipro_swot_node_update()
