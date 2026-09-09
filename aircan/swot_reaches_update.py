"""Regional daily SWOT reach updater.

Each reach CSV is updated independently using the proven Dnipro ingestion
logic. After all reaches finish, feature metadata is applied to one regional
GeoJSON and that single payload is published to CKAN and Azure.
"""

from __future__ import annotations

import io
import hashlib
import json
import random
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Any
from urllib.parse import urlsplit, unquote

import pandas as pd
import requests
from pandas.errors import EmptyDataError
from reach_dawg_cache import refresh_rows

from swot_nodes_update import (
    AZURE_ACCOUNT, AZURE_CONTAINER, CKAN_BASE, clean_id, csv_bytes, download_blob,
    get_container, get_with_retries,
    load_json, observation_bounds, parse_utc, response_frame, safe_id,
    runtime_secret, safe_region, update_ckan_resource, upload_bytes, upload_json,
    utc_now, utc_text,
)

REACH_FIELDS = "time_str,wse,slope,width,reach_q"
REACH_OUTPUT_COLUMNS = [
    "time_utc", "wse", "slope", "width", "reach_q",
    "wse_units", "slope_units", "width_units", "consensus_q",
]
FILL_VALUE_THRESHOLD = -1.0e9


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
    discharge_count: int = 0
    chart_changed: bool = False
    content_revision: str = ""
    source_latest_utc: str = ""
    chart_revision: str = ""
    stored_hydrocron_latest_utc: str = ""


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
        if str(manifest.get("status", "active")).lower() not in {
            "active", "published", "historical_built",
        }:
            continue
        if reaches.get("enabled") is False:
            continue
        geometry_blob = reaches.get("geometry_blob") or f"regions/{region_id}/reaches/reaches.geojson"
        if download_blob(container, str(geometry_blob)) is None:
            continue
        regions.append({"region_id": region_id, "manifest_blob": item.name})
    return sorted(regions, key=lambda item: item["region_id"])


def read_reach_csv(data: bytes | None) -> pd.DataFrame:
    if not data:
        return pd.DataFrame(columns=REACH_OUTPUT_COLUMNS)
    try:
        return pd.read_csv(io.BytesIO(data))
    except EmptyDataError:
        return pd.DataFrame(columns=REACH_OUTPUT_COLUMNS)


def normalize_reaches(frame: pd.DataFrame) -> pd.DataFrame:
    """Enforce the exact nine-column contract used by the proven updater."""
    if frame is None or frame.empty:
        return pd.DataFrame(columns=REACH_OUTPUT_COLUMNS)
    output = frame.copy()
    if "time_str" in output and "time_utc" not in output:
        output = output.rename(columns={"time_str": "time_utc"})
    if "time_utc" not in output:
        raise ValueError("Reach CSV has no time_utc or time_str field")
    parsed = pd.to_datetime(output["time_utc"], format="mixed", errors="coerce", utc=True)
    output = output.loc[parsed.notna()].copy()
    output["time_utc"] = parsed.loc[output.index].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    for column in ("wse", "slope", "width", "reach_q", "consensus_q"):
        if column in output:
            output[column] = pd.to_numeric(output[column], errors="coerce")
            output.loc[output[column] <= FILL_VALUE_THRESHOLD, column] = pd.NA
    for column, value in {
        "wse_units": "m", "slope_units": "m/m", "width_units": "m",
    }.items():
        if column not in output:
            output[column] = value
        else:
            output[column] = output[column].fillna(value)
    if "consensus_q" not in output:
        output["consensus_q"] = pd.NA
    for column in REACH_OUTPUT_COLUMNS:
        if column not in output:
            output[column] = pd.NA
    data_columns = ["wse", "slope", "width", "reach_q", "consensus_q"]
    output = output.loc[output[data_columns].notna().any(axis=1)]
    return (
        output[REACH_OUTPUT_COLUMNS]
        .drop_duplicates(["time_utc"], keep="last")
        .sort_values("time_utc").reset_index(drop=True)
    )


def merge_reaches(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    existing = normalize_reaches(existing)
    incoming = normalize_reaches(incoming)
    if not existing.empty and not incoming.empty:
        prior_q = existing[["time_utc", "consensus_q"]].drop_duplicates("time_utc", keep="last")
        incoming = incoming.drop(columns=["consensus_q"], errors="ignore").merge(
            prior_q, on="time_utc", how="left"
        )
    frames = [frame for frame in (existing, incoming) if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=REACH_OUTPUT_COLUMNS)
    return normalize_reaches(pd.concat(frames, ignore_index=True, sort=False))


def latest_observation(frame: pd.DataFrame) -> str | None:
    if frame.empty:
        return None
    values = pd.to_datetime(frame["time_utc"], format="mixed", errors="coerce", utc=True).dropna()
    return None if values.empty else utc_text(values.max().to_pydatetime())


def merge_discharge(frame, discharge):
    """Outer-merge DAWG-only dates without erasing Hydrocron measurements."""
    if discharge is None or discharge.empty:
        return frame
    q = normalize_reaches(discharge)[['time_utc', 'consensus_q']]
    merged = frame.merge(q, on='time_utc', how='outer', suffixes=('', '_new'))
    merged['consensus_q'] = merged['consensus_q_new'].combine_first(merged['consensus_q'])
    return normalize_reaches(merged.drop(columns='consensus_q_new'))


def chart_blob_path(url, full_blob):
    parsed = urlsplit(url)
    path = unquote(parsed.path).lstrip('/')
    regional_prefix = '/'.join(full_blob.split('/')[:3]) + '/'
    expected = f'{AZURE_CONTAINER}/{regional_prefix}'
    if (parsed.scheme != 'https' or parsed.netloc != f'{AZURE_ACCOUNT}.blob.core.windows.net'
            or not path.startswith(expected) or '..' in path.split('/') or '\\' in path
            or parsed.fragment or (parsed.query and not parsed.query.startswith('v='))):
        raise ValueError('Chart URL is outside the permitted regional Azure prefix')
    name = path[len(AZURE_CONTAINER) + 1:]
    suffix = name[len(regional_prefix):].split('/')
    if (len(suffix) != 2 or suffix[0] not in {'charts', 'chart-timeseries'}
            or suffix[1] != full_blob.rsplit('/', 1)[-1] or not name.endswith('.csv')):
        raise ValueError('Chart CSV must use its dedicated chart directory and reach filename')
    return name


def update_one_reach(
    *, container, properties: dict[str, Any], blob: str, query_end_utc: str,
    overlap_hours: int, backfill_days_if_empty: int, timeout: int, retries: int,
    query_start_utc: str | None = None,
    discharge: pd.DataFrame | None = None,
) -> ReachResult:
    reach_id = clean_id(properties.get("reach_id", ""))
    start_text = query_start_utc or ""
    try:
        previous_bytes = download_blob(container, blob)
        existing = normalize_reaches(read_reach_csv(previous_bytes))
        latest = latest_observation(existing.loc[existing[['wse', 'slope', 'width', 'reach_q']].notna().any(axis=1)])
        end = parse_utc(query_end_utc)
        start = (
            parse_utc(query_start_utc) if query_start_utc else
            parse_utc(latest) - timedelta(hours=overlap_hours)
            if latest else end - timedelta(days=backfill_days_if_empty)
        )
        # A successful empty request is not a data watermark. Match the proven
        # per-CSV boundary when the source has not yet delivered newer data.
        if latest:
            start = min(start, parse_utc(latest) - timedelta(hours=overlap_hours))
        if start >= end:
            raise ValueError("Query start must precede query end")
        start_text = utc_text(start)
        params = {
            "feature": "Reach", "feature_id": reach_id,
            "start_time": start_text, "end_time": query_end_utc,
            "output": "csv", "collection_name": "SWOT_L2_HR_RiverSP_D",
            "fields": REACH_FIELDS,
        }
        time.sleep(random.uniform(0.05, 0.2))
        response = get_with_retries(params, timeout, retries)
        if response.status_code == 400:
            if "were not found" not in response.text.lower() and response.text.strip().lower() != "not found":
                raise RuntimeError(f"Hydrocron HTTP 400: {response.text[:1000]}")
            raw = pd.DataFrame()
        else:
            response.raise_for_status()
            if not response.text.strip():
                raise ValueError('Hydrocron returned an empty HTTP 200 body')
            if response.text.lstrip().startswith('{'):
                payload = json.loads(response.text)
                if not isinstance(payload.get('results'), dict) or 'csv' not in payload['results']:
                    raise ValueError('Hydrocron success response is missing results.csv')
            raw = response_frame(response.text)
            if not raw.empty and not {'time_str', 'wse', 'slope', 'width', 'reach_q'}.issubset(raw.columns):
                raise ValueError('Hydrocron CSV is missing requested columns')
            if not raw.empty:
                raw = raw.loc[
                    raw['time_str'].astype(str).str.strip().str.lower().ne('no_data')
                ].copy()
        incoming = normalize_reaches(raw)
        if not raw.empty and pd.to_datetime(raw['time_str'], format='mixed', errors='coerce', utc=True).notna().sum() == 0:
            raise ValueError('Hydrocron returned no parseable observation dates')
        existing_times = set(existing["time_utc"].astype(str))
        incoming_times = set(incoming["time_utc"].astype(str))
        novel_count = len(incoming_times - existing_times)
        # Merge the complete overlap, as the working updater does. Hydrocron
        # may revise an already-known timestamp; novelty alone is not enough
        # to decide whether the CSV changed.
        final = merge_reaches(existing, incoming)
        if discharge is not None:
            discharge = discharge.loc[pd.to_datetime(discharge.time_utc, utc=True) <= end]
        final = merge_discharge(final, discharge)
        encoded = csv_bytes(final)
        empty_bytes = csv_bytes(pd.DataFrame(columns=REACH_OUTPUT_COLUMNS))
        changed = encoded != (previous_bytes or empty_bytes)
        if changed and (previous_bytes is not None or not final.empty):
            upload_bytes(container, blob, encoded, "text/csv; charset=utf-8")
        chart_changed = False
        chart_revision = ''
        if properties.get('chart_url'):
            chart_blob = chart_blob_path(properties['chart_url'], blob)
            chart = final[['time_utc', 'wse', 'width', 'consensus_q']].copy()
            chart['slope_cm_per_km'] = pd.to_numeric(final['slope'], errors='coerce') * 100000
            chart_bytes = csv_bytes(chart[['time_utc','wse','slope_cm_per_km','width','consensus_q']])
            chart_revision = hashlib.sha256(chart_bytes).hexdigest()[:16]
            chart_changed = download_blob(container, chart_blob) != chart_bytes
            if chart_changed:
                upload_bytes(container, chart_blob, chart_bytes, 'text/csv; charset=utf-8')
        first, last = observation_bounds(final)
        return ReachResult(
            reach_id, "success_with_data" if changed else ("success_no_data" if len(final) or response.status_code != 400 else "not_found"),
            start_text, query_end_utc, input_rows=len(raw), previous_rows=len(existing),
            final_rows=len(final), first_observation_utc=first,
            latest_observation_utc=last, blob_changed=changed,
            message=(response.text[:400] if response.status_code == 400 else f"novel_timestamps={novel_count}"),
            discharge_count=int(final['consensus_q'].notna().sum()),
            chart_changed=chart_changed,
            content_revision=hashlib.sha256(encoded).hexdigest()[:16],
            source_latest_utc=latest_observation(incoming) or '',
            chart_revision=chart_revision,
            stored_hydrocron_latest_utc=latest_observation(final.loc[final[['wse','slope','width','reach_q']].notna().any(axis=1)]) or '',
        )
    except Exception as exc:
        return ReachResult(reach_id, "retryable_failure", start_text, query_end_utc, message=str(exc)[:1000])


def update_reach_region(
    *, region: dict[str, str], connection_string_env: str = "AZURE_STORAGE_CONNECTION_STRING",
    overlap_hours: int = 0, backfill_days_if_empty: int = 2,
    batch_size: int = 500, request_workers: int = 4,
    timeout: int = 60, retries: int = 5, run_end_utc: str | None = None,
    ckan_timeout: int = 900, ckan_api_key: str | None = None, **_ignored: Any,
) -> dict[str, Any]:
    container = get_container(connection_string_env)
    region_id = safe_region(region["region_id"])
    manifest = load_json(container, region["manifest_blob"])
    if not manifest:
        raise RuntimeError(f"Missing manifest for {region_id}")
    reaches = manifest["products"]["reaches"]
    geometry_blob = str(reaches.get("geometry_blob") or f"regions/{region_id}/reaches/reaches.geojson")
    geometry = load_json(container, geometry_blob)
    if not geometry or not isinstance(geometry.get("features"), list):
        raise RuntimeError(f"Invalid reach GeoJSON for {region_id}")
    prefix = str(reaches.get("timeseries_prefix") or f"regions/{region_id}/reaches/timeseries/").rstrip("/")
    filename = str(reaches.get("filename") or reaches.get("timeseries_filename") or "reach_{reach_id}.csv")
    end = parse_utc(run_end_utc) if run_end_utc else utc_now()
    end_text = utc_text(end)
    run_id = f"{end.strftime('%Y%m%dT%H%M%SZ')}-{region_id}"
    state_blob = f"regions/{region_id}/state/reach_update.json"
    previous_state = load_json(container, state_blob) or {}
    successful_end = previous_state.get("last_successful_end_utc")
    retry_starts = {str(item['reach_id']): item.get('query_start_utc')
                    for item in previous_state.get('retry_reaches', [])}
    # Old state contains no reliable query endpoint. Bootstrap once from each
    # CSV's latest observation; never invent a successful historical checkpoint.
    overlap_hours = 0
    diagnostic_blob = f"regions/{region_id}/logs/reach_update_diagnostic_latest.json"

    ckan = manifest.get("ckan") or {}
    dataset_id = str(ckan.get("dataset_id") or "").strip()
    resource_id = str(ckan.get("reach_resource_id") or "").strip()
    azure_only = reaches.get("publication_mode") == "azure_url"
    api_key = ""
    if not azure_only:
        api_key = (ckan_api_key or "").strip()
        if not api_key:
            api_key = runtime_secret("CKAN_API_KEY")
        fingerprint = hashlib.sha256(api_key.encode()).hexdigest()[:16] if api_key else "missing"
        if not dataset_id or not resource_id or not api_key:
            upload_json(container, diagnostic_blob, {
                "region_id": region_id, "phase": "ckan_preflight_failed",
                "run_id": run_id, "dataset_id": dataset_id,
                "resource_id": resource_id, "key_fingerprint": fingerprint,
                "error": "Missing CKAN dataset ID, resource ID, or API key",
                "updated_utc": utc_text(utc_now()),
            })
            raise RuntimeError("Missing CKAN dataset ID, reach resource ID, or API key")
        preflight = requests.post(
            f"{CKAN_BASE}/api/3/action/resource_show",
            headers={"Authorization": api_key, "X-CKAN-API-Key": api_key},
            data={"id": resource_id}, timeout=60,
        )
        if not preflight.ok:
            detail = (preflight.text or "").strip().replace("\x00", "")[:2000]
            upload_json(container, diagnostic_blob, {
                "region_id": region_id, "phase": "ckan_preflight_failed",
                "run_id": run_id, "dataset_id": dataset_id,
                "resource_id": resource_id, "key_fingerprint": fingerprint,
                "key_length": len(api_key), "http_status": preflight.status_code,
                "error": detail, "updated_utc": utc_text(utc_now()),
            })
            raise RuntimeError(
                f"CKAN preflight failed with HTTP {preflight.status_code}: {detail}"
            )

    records = []
    blob_by_id: dict[str, str] = {}
    seen = set()
    for feature in geometry["features"]:
        props = feature.get("properties") or {}
        reach_id = clean_id(props.get("reach_id", ""))
        if not reach_id or reach_id in seen:
            continue
        seen.add(reach_id)
        blob = f"{prefix}/{filename.format(reach_id=safe_id(reach_id))}"
        blob_by_id[reach_id] = blob
        records.append((props, blob))
    if not records:
        raise RuntimeError(f"No reach IDs for {region_id}")

    prior_dawg_revisions = previous_state.get('dawg_revisions', {})
    selection_hash = hashlib.sha256('\n'.join(sorted(blob_by_id)).encode()).hexdigest()
    # A newly selected reach needs discharge even if the continental file did
    # not change. Older states are refreshed once to establish this signature.
    applied_revisions = (prior_dawg_revisions if previous_state.get('dawg_selection_hash') == selection_hash else {})
    dawg_rows, dawg_revisions, missing_dawg = refresh_rows(
        container, load_json(container, 'reference/dawg/current.json'),
        applied_revisions, list(blob_by_id),
    )

    upload_json(container, diagnostic_blob, {
        "region_id": region_id, "phase": "updating_reach_csvs",
        "run_end_utc": end_text, "reach_count": len(records),
        "csv_columns": REACH_OUTPUT_COLUMNS, "updated_utc": utc_text(utc_now()),
    })
    results: list[ReachResult] = []
    for offset in range(0, len(records), batch_size):
        with ThreadPoolExecutor(max_workers=request_workers) as executor:
            futures = [executor.submit(
                update_one_reach, container=container, properties=props, blob=blob,
                query_end_utc=end_text, overlap_hours=overlap_hours,
                query_start_utc=retry_starts.get(clean_id(props.get('reach_id', ''))) or successful_end,
                discharge=dawg_rows.get(clean_id(props.get('reach_id', ''))),
                backfill_days_if_empty=backfill_days_if_empty,
                timeout=timeout, retries=retries,
            ) for props, blob in records[offset:offset + batch_size]]
            results.extend(future.result() for future in as_completed(futures))
        upload_json(container, diagnostic_blob, {
            "region_id": region_id, "phase": "updating_reach_csvs",
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
        if not result or result.status == "retryable_failure":
            continue
        before = dict(props)
        props["observation_count"] = result.final_rows
        props["has_data"] = result.final_rows > 0
        props["first_observation_utc"] = result.first_observation_utc or None
        props["latest_observation_utc"] = result.latest_observation_utc or None
        props['has_discharge'] = result.discharge_count > 0
        props['discharge_count'] = result.discharge_count
        if result.final_rows:
            props["url"] = (
                f"https://{AZURE_ACCOUNT}.blob.core.windows.net/{AZURE_CONTAINER}/"
                f"{blob_by_id[result.reach_id]}"
            )
            if result.content_revision:
                props['url'] += '?v=' + result.content_revision
            if 'URL' in props:
                props['URL'] = props['url']
        else:
            props.pop("url", None)
            props.pop('URL', None)
        if props.get('chart_url') and result.chart_revision:
            props['chart_url'] = props['chart_url'].split('?')[0] + '?v=' + result.chart_revision
        geometry_changed |= before != props

    ckan_updated = False
    if geometry_changed:
        geometry_bytes = json.dumps(
            geometry, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        if not azure_only:
            # Publish the identical payload once to each backend. CKAN goes first
            # so a CKAN failure leaves Azure metadata old and retryable next run.
            upload_json(container, diagnostic_blob, {
                "region_id": region_id, "phase": "publishing_ckan_geojson",
                "run_id": run_id, "run_end_utc": end_text,
                "resource_id": resource_id, "geojson_bytes": len(geometry_bytes),
                "ckan_timeout_seconds": ckan_timeout,
                "updated_utc": utc_text(utc_now()),
            })
            try:
                update_ckan_resource(
                    resource_id, geometry_bytes,
                    f"{region_id}_sword_reaches_version_d.geojson", ckan_timeout,
                    api_key=api_key,
                )
            except Exception as exc:
                upload_json(container, diagnostic_blob, {
                    "region_id": region_id, "phase": "publishing_ckan_geojson_failed",
                    "run_id": run_id, "run_end_utc": end_text,
                    "resource_id": resource_id, "geojson_bytes": len(geometry_bytes),
                    "ckan_timeout_seconds": ckan_timeout,
                    "error_type": type(exc).__name__, "error": str(exc)[:4000],
                    "updated_utc": utc_text(utc_now()),
                })
                raise
        upload_bytes(container, geometry_blob, geometry_bytes, "application/geo+json; charset=utf-8")
        ckan_updated = not azure_only

    upload_bytes(
        container, f"regions/{region_id}/logs/reach_updates/{run_id}.csv",
        csv_bytes(pd.DataFrame(asdict(item) for item in sorted(results, key=lambda item: item.reach_id))),
        "text/csv; charset=utf-8",
    )
    counts = Counter(item.status for item in results)
    summary = {
        "schema_version": 4, "region_id": region_id, "run_id": run_id,
        "previous_successful_end_utc": successful_end,
        "query_policy": "earlier_of_successful_run_and_stored_hydrocron_no_overlap",
        "run_end_utc": end_text, "reach_count": len(records),
        "batch_size": batch_size,
        "batch_count": (len(records) + batch_size - 1) // batch_size,
        "request_workers": request_workers, "overlap_hours": overlap_hours,
        "backfill_days_if_empty": backfill_days_if_empty,
        "csv_columns": REACH_OUTPUT_COLUMNS,
        "status_counts": dict(counts),
        "changed_csvs": sum(item.blob_changed for item in results),
        "retry_queue_size": counts.get("retryable_failure", 0),
        "geometry_updated": geometry_changed, "ckan_updated": ckan_updated,
        "changed_chart_csvs": sum(item.chart_changed for item in results),
        "source_rows_received": sum(item.input_rows for item in results),
        "latest_source_observation_utc": max((item.source_latest_utc for item in results), default='') or None,
        "latest_stored_hydrocron_observation_utc": max((item.stored_hydrocron_latest_utc for item in results), default='') or None,
        "query_start_min_utc": min((item.query_start_utc for item in results if item.query_start_utc), default=None),
        "dawg_status": "missing_continental_reference" if missing_dawg else "available",
        "dawg_missing_continents": missing_dawg,
        "dawg_refreshed_reaches": len(dawg_rows),
        "dawg_revisions": dawg_revisions,
    }
    summary['data_update_outcome'] = ('incomplete' if counts.get('retryable_failure', 0) else
                                      'updated' if summary['changed_csvs'] else 'no_csv_changes')
    summary['no_change_reason'] = ('Source responses produced no changes to stored CSVs; '
                                   'a successful check does not imply new measurements.'
                                   if not summary['changed_csvs'] else None)
    upload_json(container, f"regions/{region_id}/logs/reach_update_latest.json", summary)
    upload_json(container, f"regions/{region_id}/state/reach_update.json", {
        "schema_version": 4, "region_id": region_id, "last_run_id": run_id,
        "last_successful_end_utc": end_text if not counts.get('retryable_failure', 0) else successful_end,
        "updated_utc": utc_text(utc_now()), "per_reach_csv_watermarks": False,
        "query_policy": "earlier_of_successful_run_and_stored_hydrocron_no_overlap",
        "dawg_revisions": prior_dawg_revisions if counts.get('retryable_failure', 0) else dawg_revisions,
        "dawg_selection_hash": previous_state.get('dawg_selection_hash') if counts.get('retryable_failure', 0) else selection_hash,
        "retry_reaches": [asdict(item) for item in results if item.status == "retryable_failure"],
    })
    upload_json(container, diagnostic_blob, {
        "region_id": region_id, "phase": "complete", "run_id": run_id,
        "run_end_utc": end_text, "status_counts": dict(counts),
        "changed_csvs": summary["changed_csvs"],
        "geometry_updated": geometry_changed, "ckan_updated": ckan_updated,
        "updated_utc": utc_text(utc_now()),
    })
    return summary
