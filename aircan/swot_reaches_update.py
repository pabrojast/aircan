"""Simple global incremental updater for all registered Azure SWOT reaches."""

from __future__ import annotations

import json
import random
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Any

import pandas as pd

from swot_nodes_update import (
    AZURE_ACCOUNT, AZURE_CONTAINER, clean_id, csv_bytes, download_blob,
    get_container, get_with_retries, load_json, observation_bounds,
    parse_utc, response_frame, safe_id, safe_region, update_ckan_resource,
    upload_bytes, upload_json, utc_now, utc_text,
)

REACH_FIELDS = (
    "reach_id,time_str,cycle_id,pass_id,wse,slope,width,area_total,"
    "dschg_gm,dschg_gm_q,reach_q,reach_q_b,river_name,crid,sword_version,"
    "collection_shortname,collection_version,granuleUR"
)
REACH_OUTPUT_COLUMNS = [
    "reach_id", "time_utc", "wse", "wse_units", "slope", "slope_units",
    "width", "width_units", "area_total", "area_total_units", "dschg_gm",
    "dschg_gm_units", "dschg_gm_q", "reach_q", "reach_q_b", "consensus_q",
    "consensus_q_units", "cycle_id", "pass_id", "river_name", "crid",
    "sword_version", "collection_shortname", "collection_version", "granuleUR",
]


def normalize_reaches(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize Hydrocron reach rows while retaining the full CSV contract."""
    if frame.empty:
        return pd.DataFrame(columns=REACH_OUTPUT_COLUMNS)
    if "time_str" not in frame:
        raise ValueError("Hydrocron reach response has no time_str field")
    output = frame.copy()
    parsed = pd.to_datetime(output["time_str"], format="mixed", errors="coerce", utc=True)
    output = output.loc[parsed.notna()].copy()
    output["time_utc"] = parsed.loc[output.index].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if "reach_id" in output:
        output["reach_id"] = output["reach_id"].map(clean_id)
    output["consensus_q"] = pd.NA
    output["consensus_q_units"] = "m^3/s"
    for column in REACH_OUTPUT_COLUMNS:
        if column not in output:
            output[column] = pd.NA
    return (output[REACH_OUTPUT_COLUMNS]
            .drop_duplicates(["reach_id", "time_utc", "cycle_id", "pass_id"], keep="last")
            .sort_values("time_utc").reset_index(drop=True))


@dataclass
class ReachResult:
    reach_id: str
    status: str
    query_start_utc: str
    query_end_utc: str
    input_rows: int = 0
    previous_rows: int = 0
    final_rows: int = 0
    first_observation_utc: str = ""
    latest_observation_utc: str = ""
    blob_changed: bool = False
    message: str = ""


def reach_resource_id(manifest: dict[str, Any]) -> str | None:
    return (manifest.get("ckan") or {}).get("reach_resource_id")


def discover_reach_regions(
    connection_string_env: str = "AZURE_STORAGE_CONNECTION_STRING",
    region_filter: str | None = None,
) -> list[dict[str, str]]:
    container = get_container(connection_string_env)
    allowed = {safe_region(item) for item in (region_filter or "").split(",") if item.strip()}
    regions = []
    for item in container.list_blobs(name_starts_with="regions/"):
        if not item.name.endswith("/manifest.json") or len(item.name.split("/")) != 3:
            continue
        region_id = safe_region(item.name.split("/")[1])
        if allowed and region_id not in allowed:
            continue
        manifest = load_json(container, item.name) or {}
        reaches = (manifest.get("products") or {}).get("reaches") or {}
        if str(manifest.get("status", "active")).lower() not in {"active", "published", "historical_built"}:
            continue
        if reaches.get("enabled") is False:
            continue
        geometry_blob = reaches.get("geometry_blob") or f"regions/{region_id}/reaches/reaches.geojson"
        if download_blob(container, str(geometry_blob)) is None:
            continue
        regions.append({"region_id": region_id, "manifest_blob": item.name})
    return sorted(regions, key=lambda item: item["region_id"])


def merge_reaches(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    keys = ["reach_id", "time_utc", "cycle_id", "pass_id"]
    existing = existing.copy() if existing is not None else pd.DataFrame()
    incoming = incoming.copy() if incoming is not None else pd.DataFrame()
    # Hydrocron does not supply DAWG values. Preserve an existing consensus_q
    # when an overlapping Hydrocron observation is revised.
    if not existing.empty and not incoming.empty and all(key in existing and key in incoming for key in keys):
        dawg = existing[keys + ["consensus_q", "consensus_q_units"]].drop_duplicates(keys, keep="last")
        incoming = incoming.drop(columns=["consensus_q", "consensus_q_units"], errors="ignore").merge(
            dawg, on=keys, how="left"
        )
        incoming["consensus_q_units"] = incoming["consensus_q_units"].fillna("m^3/s")
    frames = [frame for frame in (existing, incoming) if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=REACH_OUTPUT_COLUMNS)
    output = pd.concat(frames, ignore_index=True, sort=False)
    for column in REACH_OUTPUT_COLUMNS:
        if column not in output:
            output[column] = pd.NA
    return output[REACH_OUTPUT_COLUMNS].drop_duplicates(keys, keep="last").sort_values("time_utc").reset_index(drop=True)


def update_one_reach(
    *, container, properties: dict[str, Any], blob: str, query_start_utc: str,
    query_end_utc: str, timeout: int, retries: int,
) -> ReachResult:
    reach_id = clean_id(properties.get("reach_id", ""))
    count = int(properties.get("observation_count") or 0)
    first = str(properties.get("first_observation_utc") or "")
    latest = str(properties.get("latest_observation_utc") or "")
    try:
        params = {"feature": "Reach", "feature_id": reach_id, "start_time": query_start_utc,
                  "end_time": query_end_utc, "output": "csv",
                  "collection_name": "SWOT_L2_HR_RiverSP_D", "fields": REACH_FIELDS}
        time.sleep(random.uniform(0.05, 0.2))
        response = get_with_retries(params, timeout, retries)
        if response.status_code == 400:
            message = (response.text or "")[:400]
            if "not found" in message.lower():
                status = "not_found" if count == 0 else "success_no_data"
                return ReachResult(reach_id, status, query_start_utc, query_end_utc,
                                   previous_rows=count, final_rows=count,
                                   first_observation_utc=first, latest_observation_utc=latest,
                                   message=message)
            return ReachResult(reach_id, "retryable_failure", query_start_utc, query_end_utc, message=message)
        response.raise_for_status()
        raw = response_frame(response.text)
        incoming = normalize_reaches(raw)
        if incoming.empty:
            return ReachResult(reach_id, "success_no_data", query_start_utc, query_end_utc,
                               input_rows=len(raw), previous_rows=count, final_rows=count,
                               first_observation_utc=first, latest_observation_utc=latest)
        previous_bytes = download_blob(container, blob)
        if previous_bytes is None and count > 0:
            raise RuntimeError("GeoJSON reports observations but the historical CSV is missing")
        existing = pd.read_csv(__import__("io").BytesIO(previous_bytes), dtype={"reach_id": "string"}) if previous_bytes else pd.DataFrame(columns=REACH_OUTPUT_COLUMNS)
        canonical = merge_reaches(pd.DataFrame(columns=REACH_OUTPUT_COLUMNS), existing)
        final = merge_reaches(existing, incoming)
        encoded = csv_bytes(final)
        changed = encoded != csv_bytes(canonical)
        if changed:
            upload_bytes(container, blob, encoded, "text/csv; charset=utf-8")
        first, latest = observation_bounds(final)
        return ReachResult(reach_id, "success_with_data", query_start_utc, query_end_utc,
                           len(raw), len(existing), len(final), first, latest, changed)
    except Exception as exc:
        return ReachResult(reach_id, "retryable_failure", query_start_utc, query_end_utc,
                           previous_rows=count, final_rows=count,
                           first_observation_utc=first, latest_observation_utc=latest,
                           message=str(exc)[:1000])


def update_reach_region(
    *, region: dict[str, str], connection_string_env: str = "AZURE_STORAGE_CONNECTION_STRING",
    overlap_hours: int = 48, batch_size: int = 500, request_workers: int = 4,
    timeout: int = 60, retries: int = 5, run_end_utc: str | None = None,
) -> dict[str, Any]:
    container = get_container(connection_string_env)
    region_id = safe_region(region["region_id"])
    manifest = load_json(container, region["manifest_blob"])
    reaches = manifest["products"]["reaches"]
    geometry_blob = str(reaches.get("geometry_blob") or f"regions/{region_id}/reaches/reaches.geojson")
    geometry = load_json(container, geometry_blob)
    state_blob = f"regions/{region_id}/state/reach_update.json"
    diagnostic_blob = f"regions/{region_id}/logs/reach_update_diagnostic_latest.json"
    state = load_json(container, state_blob) or {}
    historical = manifest.get("historical_summary") or {}
    watermark = str(state.get("last_successful_end_utc") or (historical.get("window") or {}).get("end") or "2023-03-30T00:00:00Z")
    retry_map = {clean_id(item["reach_id"]): item for item in state.get("retry_reaches", [])}
    end = parse_utc(run_end_utc) if run_end_utc else utc_now()
    end_text = utc_text(end)
    prefix = str(reaches.get("timeseries_prefix") or f"regions/{region_id}/reaches/timeseries/").rstrip("/")
    filename = str(reaches.get("filename") or reaches.get("timeseries_filename") or "reach_{reach_id}.csv")
    records, blob_by_id, seen = [], {}, set()
    for feature in geometry["features"]:
        props = feature.get("properties") or {}
        reach_id = clean_id(props.get("reach_id", ""))
        if not reach_id or reach_id in seen:
            continue
        seen.add(reach_id)
        blob = f"{prefix}/{filename.format(reach_id=safe_id(reach_id))}"
        blob_by_id[reach_id] = blob
        records.append((reach_id, props, blob))
    records.sort(key=lambda item: (0 if item[0] in retry_map else 1, item[0]))
    upload_json(container, diagnostic_blob, {
        "region_id": region_id, "phase": "querying_hydrocron", "run_end_utc": end_text,
        "reach_count": len(records), "updated_utc": utc_text(utc_now()),
    })
    results = []
    for offset in range(0, len(records), batch_size):
        with ThreadPoolExecutor(max_workers=request_workers) as executor:
            futures = []
            for reach_id, props, blob in records[offset:offset + batch_size]:
                start = retry_map.get(reach_id, {}).get("query_start_utc") or utc_text(parse_utc(watermark) - timedelta(hours=overlap_hours))
                futures.append(executor.submit(update_one_reach, container=container, properties=props,
                                               blob=blob, query_start_utc=start, query_end_utc=end_text,
                                               timeout=timeout, retries=retries))
            results.extend(future.result() for future in as_completed(futures))
        upload_json(container, diagnostic_blob, {
            "region_id": region_id, "phase": "querying_hydrocron",
            "run_end_utc": end_text, "completed_reaches": len(results),
            "reach_count": len(records),
            "status_counts": dict(Counter(item.status for item in results)),
            "updated_utc": utc_text(utc_now()),
        })
    by_id = {item.reach_id: item for item in results}
    geometry_changed = False
    for feature in geometry["features"]:
        props = feature.get("properties") or {}
        result = by_id.get(clean_id(props.get("reach_id", "")))
        if not result or result.status != "success_with_data":
            continue
        before = (props.get("observation_count"), props.get("latest_observation_utc"), props.get("url"))
        props.update({"observation_count": result.final_rows, "has_data": result.final_rows > 0,
                      "first_observation_utc": result.first_observation_utc or None,
                      "latest_observation_utc": result.latest_observation_utc or None})
        if result.final_rows:
            props["url"] = f"https://{AZURE_ACCOUNT}.blob.core.windows.net/{AZURE_CONTAINER}/{blob_by_id[result.reach_id]}"
        geometry_changed |= before != (props.get("observation_count"), props.get("latest_observation_utc"), props.get("url"))
    run_id = f"{end.strftime('%Y%m%dT%H%M%SZ')}-{region_id}"
    # Persist per-reach diagnostics before publication, because CKAN failures
    # must remain debuggable even when the Airflow pod logs are unavailable.
    upload_bytes(container, f"regions/{region_id}/logs/reach_updates/{run_id}.csv",
                 csv_bytes(pd.DataFrame(asdict(item) for item in results)), "text/csv; charset=utf-8")
    if geometry_changed:
        encoded = json.dumps(geometry, separators=(",", ":")).encode("utf-8")
        resource_id = reach_resource_id(manifest)
        if resource_id:
            upload_json(container, diagnostic_blob, {
                "region_id": region_id, "phase": "publishing_ckan",
                "run_end_utc": end_text, "resource_id": resource_id,
                "updated_utc": utc_text(utc_now()),
            })
            try:
                update_ckan_resource(resource_id, encoded, f"{region_id}_sword_reaches_version_d.geojson", 180)
            except Exception as exc:
                upload_json(container, diagnostic_blob, {
                    "region_id": region_id, "phase": "failed_publishing_ckan",
                    "run_end_utc": end_text, "resource_id": resource_id,
                    "error_type": type(exc).__name__, "error": str(exc)[:2000],
                    "updated_utc": utc_text(utc_now()),
                })
                raise
        upload_json(container, diagnostic_blob, {
            "region_id": region_id, "phase": "publishing_azure_geojson",
            "run_end_utc": end_text, "geometry_blob": geometry_blob,
            "updated_utc": utc_text(utc_now()),
        })
        upload_bytes(container, geometry_blob, encoded, "application/geo+json; charset=utf-8")
    retry = []
    for result in results:
        if result.status == "retryable_failure":
            prior = retry_map.get(result.reach_id) or {}
            retry.append({"reach_id": result.reach_id,
                          "query_start_utc": prior.get("query_start_utc") or result.query_start_utc,
                          "consecutive_failures": int(prior.get("consecutive_failures") or 0) + 1,
                          "last_error": result.message, "last_attempt_utc": end_text})
    current_dawg = load_json(container, "reference/dawg/current.json") or {}
    summary = {"schema_version": 1, "region_id": region_id, "run_id": run_id,
               "previous_watermark_utc": watermark, "run_end_utc": end_text,
               "reach_count": len(records), "batch_size": batch_size,
               "batch_count": (len(records) + batch_size - 1) // batch_size,
               "request_workers": request_workers, "overlap_hours": overlap_hours,
               "status_counts": dict(Counter(item.status for item in results)),
               "changed_csvs": sum(item.blob_changed for item in results),
               "retry_queue_size": len(retry), "geometry_updated": geometry_changed,
               "ckan_updated": bool(geometry_changed and reach_resource_id(manifest)),
               "dawg_current_updated_utc": current_dawg.get("updated_utc")}
    upload_json(container, f"regions/{region_id}/logs/reach_update_latest.json", summary)
    upload_json(container, state_blob, {"schema_version": 1, "region_id": region_id,
                                      "last_successful_end_utc": end_text,
                                      "updated_utc": utc_text(utc_now()), "last_run_id": run_id,
                                      "retry_reaches": retry})
    upload_json(container, diagnostic_blob, {
        "region_id": region_id, "phase": "complete", "run_id": run_id,
        "run_end_utc": end_text, "status_counts": summary["status_counts"],
        "retry_queue_size": len(retry), "updated_utc": utc_text(utc_now()),
    })
    return summary
