"""Simple global incremental updater for all registered Azure SWOT nodes.

One Airflow DAG discovers all regions. One bounded Airflow task updates each
region, processing ordinary in-process batches. Recovery uses a regional
watermark plus a compact failed-node queue that preserves the original query
start for failures lasting longer than the normal overlap.
"""

from __future__ import annotations

import io
import json
import logging
import os
import random
import re
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContainerClient, ContentSettings
from pandas.errors import EmptyDataError


logger = logging.getLogger(__name__)
AZURE_ACCOUNT = "ihpwinsdata"
AZURE_CONTAINER = "swot"
HYDROCRON_URL = "https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries"
COLLECTION = "SWOT_L2_HR_RiverSP_D"
CKAN_BASE = "https://ihp-wins.unesco.org"
DEFAULT_INITIAL_START = "2023-03-30T00:00:00Z"
FIELDS = (
    "node_id,reach_id,time_str,lat,lon,river_name,wse,wse_u,wse_r_u,"
    "width,width_u,node_q,node_q_b,ice_clim_f,xovr_cal_q,cycle_id,pass_id,"
    "crid,sword_version,collection_shortname,collection_version,granuleUR"
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
_thread_local = threading.local()


@dataclass
class NodeResult:
    node_id: str
    status: str
    query_start_utc: str
    query_end_utc: str
    input_rows: int = 0
    accepted_rows: int = 0
    previous_rows: int = 0
    final_rows: int = 0
    first_observation_utc: str = ""
    latest_observation_utc: str = ""
    blob_changed: bool = False
    message: str = ""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(value: str) -> datetime:
    return pd.to_datetime(value, errors="raise", utc=True).to_pydatetime()


def clean_id(value: Any) -> str:
    text = str(value).strip()
    return text[:-2] if text.endswith(".0") else text


def safe_id(value: Any) -> str:
    cleaned = "".join(ch for ch in clean_id(value) if ch.isalnum() or ch in "-_")
    if not cleaned:
        raise ValueError(f"Unsafe node identifier {value!r}")
    return cleaned


def safe_region(value: str) -> str:
    region = value.strip().lower()
    if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", region):
        raise ValueError(f"Unsafe region identifier {value!r}")
    return region


def runtime_secret(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if value:
        return value
    try:
        from airflow.models import Variable

        return str(Variable.get(name, default_var="")).strip()
    except Exception:
        return ""


def get_container(connection_string_env: str = "AZURE_STORAGE_CONNECTION_STRING") -> ContainerClient:
    connection = runtime_secret(connection_string_env)
    if not connection:
        raise RuntimeError(f"{connection_string_env} is required")
    container = ContainerClient.from_connection_string(connection, AZURE_CONTAINER)
    if container.account_name != AZURE_ACCOUNT:
        raise RuntimeError(f"Refusing unexpected Azure account {container.account_name!r}")
    container.get_container_properties()
    return container


def download_blob(container: ContainerClient, name: str) -> bytes | None:
    try:
        return container.get_blob_client(name).download_blob().readall()
    except ResourceNotFoundError:
        return None


def upload_bytes(container: ContainerClient, name: str, data: bytes, content_type: str) -> None:
    container.get_blob_client(name).upload_blob(
        data, overwrite=True, content_settings=ContentSettings(content_type=content_type)
    )


def load_json(container: ContainerClient, name: str) -> dict[str, Any] | None:
    data = download_blob(container, name)
    return None if data is None else json.loads(data)


def upload_json(container: ContainerClient, name: str, value: Any) -> None:
    upload_bytes(
        container, name,
        json.dumps(value, ensure_ascii=False, indent=2, default=str).encode("utf-8"),
        "application/json; charset=utf-8",
    )


def http_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
    return _thread_local.session


def get_with_retries(params: dict[str, str], timeout: int, retries: int) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = http_session().get(HYDROCRON_URL, params=params, timeout=timeout)
            if response.status_code in {429, 500, 502, 503, 504}:
                raise RuntimeError(f"transient HTTP {response.status_code}")
            return response
        except Exception as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(min(30.0, 0.8 * 2**attempt) + random.uniform(0, 0.5))
    raise RuntimeError(f"request failed after {retries} attempts: {last_error}")


def response_frame(text: str) -> pd.DataFrame:
    payload = (text or "").strip()
    if not payload:
        return pd.DataFrame()
    if payload.startswith("{"):
        payload = str(json.loads(payload).get("results", {}).get("csv", "") or "").strip()
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
    """Apply the established fail-closed Version D node-quality filter."""
    if frame.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    frame = frame.copy()
    if "xovr_cal_q" not in frame and "xover_cal_q" in frame:
        frame = frame.rename(columns={"xover_cal_q": "xovr_cal_q"})
    missing = sorted(REQUIRED_FILTER_FIELDS - set(frame.columns))
    if missing:
        raise ValueError(f"Hydrocron response missing filter fields: {missing}")
    times = pd.to_datetime(frame["time_str"], format="mixed", errors="coerce", utc=True)
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
        .sort_values("time_utc").reset_index(drop=True)
    )


def merge_nodes(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    frames = [frame for frame in (existing, incoming) if frame is not None and not frame.empty]
    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    output = pd.concat(frames, ignore_index=True, sort=False)
    for column in OUTPUT_COLUMNS:
        if column not in output:
            output[column] = pd.NA
    return (
        output[OUTPUT_COLUMNS]
        .drop_duplicates(["node_id", "time_utc", "cycle_id", "pass_id"], keep="last")
        .sort_values("time_utc").reset_index(drop=True)
    )


def csv_bytes(frame: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    frame.to_csv(buffer, index=False, lineterminator="\n")
    return buffer.getvalue().encode("utf-8")


def observation_bounds(frame: pd.DataFrame) -> tuple[str, str]:
    if frame.empty:
        return "", ""
    times = pd.to_datetime(frame["time_utc"], format="mixed", errors="coerce", utc=True).dropna()
    if times.empty:
        return "", ""
    return utc_text(times.min().to_pydatetime()), utc_text(times.max().to_pydatetime())


def node_resource_id(manifest: dict[str, Any]) -> str | None:
    ckan = manifest.get("ckan") or {}
    return ckan.get("node_resource_id") or ckan.get("nod_resource_id")


def discover_node_regions(
    connection_string_env: str = "AZURE_STORAGE_CONNECTION_STRING",
    region_filter: str | None = None,
) -> list[dict[str, str]]:
    """Return one small descriptor per active Azure node region."""
    container = get_container(connection_string_env)
    allowed = {
        safe_region(item) for item in (region_filter or "").split(",") if item.strip()
    }
    regions = []
    for item in container.list_blobs(name_starts_with="regions/"):
        if not item.name.endswith("/manifest.json"):
            continue
        parts = item.name.split("/")
        if len(parts) != 3:
            continue
        if allowed and safe_region(parts[1]) not in allowed:
            continue
        manifest = load_json(container, item.name) or {}
        if str(manifest.get("status", "active")).lower() not in {"active", "published", "historical_built"}:
            continue
        nodes = (manifest.get("products") or {}).get("nodes") or {}
        if nodes.get("enabled") is False or not nodes.get("geometry_blob"):
            continue
        regions.append({"region_id": safe_region(parts[1]), "manifest_blob": item.name})
    return sorted(regions, key=lambda item: item["region_id"])


def bootstrap_watermark(
    container: ContainerClient, region_id: str, manifest: dict[str, Any]
) -> str:
    legacy = load_json(container, f"regions/{region_id}/logs/node_incremental_state.json") or {}
    if legacy.get("last_successful_end_utc"):
        return str(legacy["last_successful_end_utc"])
    historical = manifest.get("historical_summary") or {}
    return str(
        (historical.get("window") or {}).get("end")
        or historical.get("run_finished_utc")
        or DEFAULT_INITIAL_START
    )


def query_start_for_node(
    node_id: str, regional_watermark: str, retry_map: dict[str, dict[str, Any]], overlap_hours: int
) -> str:
    retry = retry_map.get(node_id)
    if retry and retry.get("query_start_utc"):
        return str(retry["query_start_utc"])
    return utc_text(parse_utc(regional_watermark) - timedelta(hours=overlap_hours))


def update_one_node(
    *, container: ContainerClient, node_properties: dict[str, Any], blob: str,
    query_start_utc: str, query_end_utc: str, timeout: int, retries: int,
) -> NodeResult:
    node_id = clean_id(node_properties.get("node_id", ""))
    previous_count = int(node_properties.get("observation_count") or 0)
    previous_first = str(node_properties.get("first_observation_utc") or "")
    previous_latest = str(node_properties.get("latest_observation_utc") or "")
    try:
        params = {
            "feature": "Node", "feature_id": node_id,
            "start_time": query_start_utc, "end_time": query_end_utc,
            "output": "csv", "collection_name": COLLECTION, "fields": FIELDS,
        }
        time.sleep(random.uniform(0.05, 0.2))
        response = get_with_retries(params, timeout, retries)
        if response.status_code == 400:
            text = (response.text or "")[:400]
            if "not found" in text.lower() and previous_count == 0:
                return NodeResult(node_id, "not_found", query_start_utc, query_end_utc, message=text)
            if "not found" in text.lower():
                # Hydrocron uses HTTP 400 both for an unknown/never-observed
                # feature and for a valid feature with no observations in the
                # requested window. Existing historical data distinguishes the
                # common incremental no-data case.
                if previous_count == 0:
                    return NodeResult(
                        node_id, "not_found", query_start_utc, query_end_utc,
                        final_rows=0, message=text,
                    )
                return NodeResult(
                    node_id, "success_no_data", query_start_utc, query_end_utc,
                    previous_rows=previous_count, final_rows=previous_count,
                    first_observation_utc=previous_first,
                    latest_observation_utc=previous_latest, message=text,
                )
            return NodeResult(node_id, "retryable_failure", query_start_utc, query_end_utc, message=text)
        response.raise_for_status()
        raw = response_frame(response.text)
        incoming = filter_nodes(raw)
        if incoming.empty:
            return NodeResult(
                node_id, "success_no_data", query_start_utc, query_end_utc,
                input_rows=len(raw), previous_rows=previous_count, final_rows=previous_count,
                first_observation_utc=previous_first, latest_observation_utc=previous_latest,
            )
        previous_bytes = download_blob(container, blob)
        if previous_bytes is None and previous_count > 0:
            raise RuntimeError("GeoJSON reports observations but the historical CSV is missing")
        existing = read_csv_bytes(previous_bytes)
        previous_canonical = merge_nodes(pd.DataFrame(columns=OUTPUT_COLUMNS), existing)
        final = merge_nodes(existing, incoming)
        encoded = csv_bytes(final)
        changed = encoded != csv_bytes(previous_canonical)
        if changed:
            upload_bytes(container, blob, encoded, "text/csv; charset=utf-8")
        first, latest = observation_bounds(final)
        return NodeResult(
            node_id, "success_with_data", query_start_utc, query_end_utc,
            input_rows=len(raw), accepted_rows=len(incoming), previous_rows=len(existing),
            final_rows=len(final), first_observation_utc=first,
            latest_observation_utc=latest, blob_changed=changed,
        )
    except Exception as exc:
        return NodeResult(
            node_id, "retryable_failure", query_start_utc, query_end_utc,
            previous_rows=previous_count, final_rows=previous_count,
            first_observation_utc=previous_first, latest_observation_utc=previous_latest,
            message=str(exc)[:1000],
        )


def update_ckan_resource(resource_id: str, geometry: bytes, filename: str, timeout: int) -> None:
    api_key = runtime_secret("CKAN_API_KEY")
    if not api_key:
        raise RuntimeError("CKAN_API_KEY is required when node GeoJSON changes")
    response = requests.post(
        f"{CKAN_BASE}/api/3/action/resource_update",
        headers={"Authorization": api_key, "X-CKAN-API-Key": api_key},
        data={"id": resource_id, "format": "GeoJSON"},
        files={"upload": (filename, io.BytesIO(geometry), "application/geo+json")},
        timeout=timeout,
    )
    response.raise_for_status()
    if not response.json().get("success"):
        raise RuntimeError(response.text)


def update_node_region(
    *, region: dict[str, str], connection_string_env: str = "AZURE_STORAGE_CONNECTION_STRING",
    overlap_hours: int = 48, batch_size: int = 750, request_workers: int = 4,
    timeout: int = 60, retries: int = 5, run_end_utc: str | None = None,
    ckan_timeout: int = 180,
) -> dict[str, Any]:
    """Update every node in one region using internal, sequential batches."""
    if not 1 <= batch_size <= 5000:
        raise ValueError("batch_size must be between 1 and 5000")
    if not 1 <= request_workers <= 16:
        raise ValueError("request_workers must be between 1 and 16")
    container = get_container(connection_string_env)
    region_id = safe_region(region["region_id"])
    manifest = load_json(container, region["manifest_blob"])
    if not manifest:
        raise RuntimeError(f"Missing manifest for {region_id}")
    nodes = manifest["products"]["nodes"]
    geometry_blob = str(nodes["geometry_blob"])
    geometry = load_json(container, geometry_blob)
    if not geometry or not isinstance(geometry.get("features"), list):
        raise RuntimeError(f"Invalid node GeoJSON for {region_id}")
    state_name = f"regions/{region_id}/state/node_update.json"
    state = load_json(container, state_name) or {}
    watermark = str(state.get("last_successful_end_utc") or bootstrap_watermark(container, region_id, manifest))
    retry_items = list(state.get("retry_nodes") or [])
    retry_map = {clean_id(item.get("node_id", "")): item for item in retry_items}
    end = parse_utc(run_end_utc) if run_end_utc else utc_now()
    end_text = utc_text(end)
    run_id = f"{end.strftime('%Y%m%dT%H%M%SZ')}-{region_id}"
    prefix = str(nodes.get("timeseries_prefix") or f"regions/{region_id}/nodes/timeseries/").rstrip("/")
    filename = str(nodes.get("filename") or nodes.get("timeseries_filename") or "node_{node_id}.csv")

    records = []
    blob_by_id: dict[str, str] = {}
    seen = set()
    for feature in geometry["features"]:
        props = feature.get("properties") or {}
        node_id = clean_id(props.get("node_id", ""))
        if not node_id or node_id in seen:
            continue
        seen.add(node_id)
        blob = f"{prefix}/{filename.format(node_id=safe_id(node_id))}"
        blob_by_id[node_id] = blob
        records.append((node_id, props, blob))
    records.sort(key=lambda item: (0 if item[0] in retry_map else 1, item[0]))
    if not records:
        raise RuntimeError(f"No node IDs for {region_id}")

    results: list[NodeResult] = []
    for offset in range(0, len(records), batch_size):
        batch = records[offset:offset + batch_size]
        with ThreadPoolExecutor(max_workers=request_workers) as executor:
            futures = [executor.submit(
                update_one_node,
                container=container, node_properties=props, blob=blob,
                query_start_utc=query_start_for_node(node_id, watermark, retry_map, overlap_hours),
                query_end_utc=end_text, timeout=timeout, retries=retries,
            ) for node_id, props, blob in batch]
            for future in as_completed(futures):
                results.append(future.result())
        logger.info("%s nodes %d/%d: %s", region_id, min(offset + len(batch), len(records)),
                    len(records), dict(Counter(item.status for item in results)))

    by_id = {result.node_id: result for result in results}
    geometry_changed = False
    for feature in geometry["features"]:
        props = feature.get("properties") or {}
        result = by_id.get(clean_id(props.get("node_id", "")))
        if not result or result.status not in {"success_with_data", "success_no_data", "not_found"}:
            continue
        if result.status == "success_with_data":
            before = (props.get("observation_count"), props.get("latest_observation_utc"), props.get("url"))
            props["observation_count"] = result.final_rows
            props["has_data"] = result.final_rows > 0
            props["first_observation_utc"] = result.first_observation_utc or None
            props["latest_observation_utc"] = result.latest_observation_utc or None
            if result.final_rows:
                props["url"] = (
                    f"https://{AZURE_ACCOUNT}.blob.core.windows.net/{AZURE_CONTAINER}/"
                    f"{blob_by_id[result.node_id]}"
                )
            after = (props.get("observation_count"), props.get("latest_observation_utc"), props.get("url"))
            geometry_changed |= before != after

    if geometry_changed:
        geometry_bytes = json.dumps(geometry, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        resource_id = node_resource_id(manifest)
        if resource_id:
            update_ckan_resource(resource_id, geometry_bytes, f"{region_id}_sword_nodes_version_d.geojson", ckan_timeout)
        # Azure GeoJSON is the commit immediately after CKAN succeeds. If CKAN
        # fails, the old geometry remains and the next run can safely retry it.
        upload_bytes(container, geometry_blob, geometry_bytes, "application/geo+json; charset=utf-8")

    next_retry = []
    for result in results:
        if result.status != "retryable_failure":
            continue
        previous = retry_map.get(result.node_id) or {}
        next_retry.append({
            "node_id": result.node_id,
            "query_start_utc": previous.get("query_start_utc") or result.query_start_utc,
            "consecutive_failures": int(previous.get("consecutive_failures") or 0) + 1,
            "last_error": result.message,
            "last_attempt_utc": end_text,
        })

    log_frame = pd.DataFrame(asdict(result) for result in sorted(results, key=lambda item: item.node_id))
    upload_bytes(
        container, f"regions/{region_id}/logs/node_updates/{run_id}.csv",
        csv_bytes(log_frame), "text/csv; charset=utf-8",
    )
    counts = Counter(result.status for result in results)
    summary = {
        "schema_version": 1, "region_id": region_id, "run_id": run_id,
        "previous_watermark_utc": watermark, "run_end_utc": end_text,
        "node_count": len(records), "batch_size": batch_size,
        "batch_count": (len(records) + batch_size - 1) // batch_size,
        "request_workers": request_workers, "overlap_hours": overlap_hours,
        "status_counts": dict(counts), "changed_csvs": sum(item.blob_changed for item in results),
        "retry_queue_size": len(next_retry), "geometry_updated": geometry_changed,
        "ckan_updated": bool(geometry_changed and node_resource_id(manifest)),
    }
    upload_json(container, f"regions/{region_id}/logs/node_update_latest.json", summary)
    # The state is the final commit marker. Failed nodes retain their original
    # query start while the healthy regional stream advances normally.
    upload_json(container, state_name, {
        "schema_version": 1, "region_id": region_id,
        "last_successful_end_utc": end_text, "updated_utc": utc_text(utc_now()),
        "last_run_id": run_id, "retry_nodes": next_retry,
    })
    return summary


def summarize_node_regions(region_results: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "completed_utc": utc_text(utc_now()),
        "region_count": len(region_results),
        "node_count": sum(int(item.get("node_count") or 0) for item in region_results),
        "changed_csvs": sum(int(item.get("changed_csvs") or 0) for item in region_results),
        "retry_queue_size": sum(int(item.get("retry_queue_size") or 0) for item in region_results),
        "regions": region_results,
    }
