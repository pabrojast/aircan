"""
Airflow DAG: SWOT Hydrocron -> Azure incremental per-reach CSV updater

What this DAG does (daily):
  1) Downloads reach_ids from a public GeoJSON (CKAN resource URL)
  2) For each reach_id:
      - downloads existing per-reach CSV from Azure Blob Storage (if present)
      - finds latest time_str in that CSV
      - queries Hydrocron for [last_time - overlap, now]
      - cleans + de-dups by time_str
      - uploads updated CSV back to Azure
  3) Writes a run log CSV to Azure: {AZURE_PREFIX}/update_log_daily.csv

Required Airflow Variables (recommended):
  - AZURE_STORAGE_CONNECTION_STRING   (string)

Optional Airflow Variables (override defaults without redeploying code):
  - SWOT_GEOJSON_URL
  - SWOT_REACH_ID_FIELD
  - SWOT_HYDROCRON_URL
  - SWOT_COLLECTION_NAME
  - SWOT_FIELDS
  - SWOT_AZURE_CONTAINER
  - SWOT_AZURE_PREFIX
  - SWOT_OVERLAP_HOURS
  - SWOT_DEFAULT_BACKFILL_DAYS_IF_EMPTY
  - SWOT_MAX_RETRIES
  - SWOT_TIMEOUT_S
  - SWOT_SLEEP_MIN_S
  - SWOT_SLEEP_MAX_S

Notes:
  - This file must live in your Airflow DAGs folder (e.g., dags/swot_hydrocron_update.py)
  - Do NOT execute logic at import time; everything runs inside run_swot_hydrocron_incremental_update().
"""

import os
import io
import json
import time
import random
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

import pandas as pd
import requests
from pandas.errors import EmptyDataError

from azure.storage.blob import ContainerClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# -----------------------------------------------------------------------------
# Airflow logging
# -----------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# Defaults (can be overridden by Airflow Variables)
# -----------------------------------------------------------------------------
DEFAULT_GEOJSON_URL = (
    "https://ihp-wins.unesco.org/dataset/a3d41691-c16c-4f82-ad83-d12fb38df910/"
    "resource/6a2426b1-4c65-4dab-bf32-b252aa206adc/download/dnipro_sword_reaches_clip.geojson"
)
DEFAULT_REACH_ID_FIELD = "reach_id"

DEFAULT_HYDROCRON_URL = "https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries"
DEFAULT_COLLECTION_NAME = "SWOT_L2_HR_RiverSP_D"
DEFAULT_FIELDS = "reach_id,time_str,cycle_id,pass_id,wse,slope,width,dschg_gm,dschg_gm_q,reach_q"

DEFAULT_TIMEOUT_S = 60
DEFAULT_SLEEP_MIN_S = 0.2
DEFAULT_SLEEP_MAX_S = 0.6
DEFAULT_MAX_RETRIES = 5

DEFAULT_OVERLAP_HOURS = 48
DEFAULT_BACKFILL_DAYS_IF_EMPTY = 2

DEFAULT_AZURE_CONTAINER = "data"
DEFAULT_AZURE_PREFIX = "SWOT/hydrocron_timeseries_by_reach"  # no trailing slash

# -----------------------------------------------------------------------------
# Helper: read Airflow Variable with fallback
# -----------------------------------------------------------------------------
def vget(key: str, default: Any) -> Any:
    """
    Airflow Variable getter with fallback.
    - If Variable doesn't exist, returns default.
    - If exists but empty, returns default.
    """
    try:
        val = Variable.get(key)
        if val is None:
            return default
        if isinstance(val, str) and val.strip() == "":
            return default
        return val
    except Exception:
        return default


# -----------------------------------------------------------------------------
# Core HTTP + parsing helpers
# -----------------------------------------------------------------------------
def request_with_retries(url: str, params: Optional[Dict[str, Any]] = None,
                         timeout_s: int = DEFAULT_TIMEOUT_S,
                         max_retries: int = DEFAULT_MAX_RETRIES) -> requests.Response:
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params or {}, timeout=timeout_s)
            # transient-ish response codes that are worth retrying
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code}")
            return r
        except Exception as e:
            last_err = e
            backoff = min(30, (2 ** (attempt - 1)) * 0.7) + random.uniform(0, 0.5)
            time.sleep(backoff)
    raise RuntimeError(f"Failed after {max_retries} retries. Last error: {last_err}")


def hydrocron_response_to_df(text: str) -> pd.DataFrame:
    """
    Hydrocron can return:
      - plain CSV text (when output=csv)
      - JSON payload containing results.csv
    This safely handles both.
    """
    text = (text or "").strip()
    if not text:
        return pd.DataFrame()

    if text.startswith("{"):
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            return pd.DataFrame()
        csv_text = (obj.get("results", {}).get("csv", "") or "").strip()
        if not csv_text:
            return pd.DataFrame()
        try:
            return pd.read_csv(io.StringIO(csv_text))
        except EmptyDataError:
            return pd.DataFrame()

    try:
        return pd.read_csv(io.StringIO(text))
    except EmptyDataError:
        return pd.DataFrame()


def safe_read_csv_text(text: Optional[str]) -> pd.DataFrame:
    if text is None:
        return pd.DataFrame()
    s = str(text)
    if s.strip() == "":
        return pd.DataFrame()
    try:
        return pd.read_csv(io.StringIO(s))
    except EmptyDataError:
        return pd.DataFrame()


def load_reach_ids_from_geojson(url: str, field: str,
                                timeout_s: int,
                                max_retries: int) -> List[str]:
    r = request_with_retries(url, timeout_s=timeout_s, max_retries=max_retries)
    obj = r.json()

    feats = obj.get("features", []) or []
    out: List[str] = []
    for f in feats:
        props = f.get("properties", {}) or {}
        val = props.get(field, None)
        if val is None:
            continue
        s = str(val).strip()
        # normalize integers that got serialized as float-looking strings (e.g., "22511300021.0")
        if s.endswith(".0"):
            s = s[:-2]
        if s:
            out.append(s)

    # unique, stable order
    seen = set()
    uniq: List[str] = []
    for rid in out:
        if rid in seen:
            continue
        seen.add(rid)
        uniq.append(rid)

    if not uniq:
        raise RuntimeError(f"No reach IDs found in GeoJSON for field '{field}'.")
    return uniq


def get_last_time_dt(existing: pd.DataFrame) -> Optional[datetime]:
    if existing is None or existing.empty or "time_str" not in existing.columns:
        return None

    s = existing["time_str"].astype(str).str.strip()
    s = s[(s != "") & (s.str.lower() != "nan") & (s != "no_data")]
    if len(s) == 0:
        return None

    ts = pd.to_datetime(s, format="%Y-%m-%dT%H:%M:%SZ", errors="coerce", utc=True)
    if ts.isna().any():
        ts2 = pd.to_datetime(s[ts.isna()], utc=True, errors="coerce")
        ts.loc[ts.isna()] = ts2

    if ts.notna().any():
        return ts.max().to_pydatetime()
    return None


def clean_new_df(df: pd.DataFrame, rid: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    if "reach_id" not in df.columns:
        df["reach_id"] = rid

    if "time_str" in df.columns:
        df = df[df["time_str"] != "no_data"]
        t = pd.to_datetime(df["time_str"], errors="coerce", utc=True)
        df = df.loc[t.notna()].copy()
        df["time_str"] = t.loc[t.notna()].dt.strftime("%Y-%m-%dT%H:%M:%SZ").values

    if "wse" in df.columns:
        # filter Hydrocron fill values
        df = df[df["wse"] > -1e11]

    return df.reset_index(drop=True)


def append_dedup(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    if existing is None or existing.empty:
        out = new.copy()
    elif new is None or new.empty:
        out = existing.copy()
    else:
        out = pd.concat([existing, new], ignore_index=True, sort=False)

    if "time_str" in out.columns:
        out["time_str"] = out["time_str"].astype(str)
        out = out.drop_duplicates(subset=["time_str"], keep="last")
        out = out.sort_values("time_str")

    return out.reset_index(drop=True)


# -----------------------------------------------------------------------------
# Azure helpers
# -----------------------------------------------------------------------------
def get_container(conn_str: str, container_name: str) -> ContainerClient:
    if not conn_str.strip():
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is empty or not set.")
    return ContainerClient.from_connection_string(conn_str, container_name)


def download_blob_text(container: ContainerClient, blob_name: str) -> Optional[str]:
    try:
        bc = container.get_blob_client(blob_name)
        return bc.download_blob().readall().decode("utf-8", errors="replace")
    except ResourceNotFoundError:
        return None


def upload_blob_text(container: ContainerClient, blob_name: str, text: str) -> None:
    bc = container.get_blob_client(blob_name)
    bc.upload_blob(
        text.encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="text/csv; charset=utf-8"),
    )


# -----------------------------------------------------------------------------
# Main callable for Airflow
# -----------------------------------------------------------------------------
def run_swot_hydrocron_incremental_update(**context) -> Dict[str, Any]:
    """
    Airflow task entrypoint.
    Returns a small dict summary (visible in task logs / XCom if enabled).
    """

    # --- Load config (Airflow Variables override defaults) ---
    geojson_url = vget("SWOT_GEOJSON_URL", DEFAULT_GEOJSON_URL)
    reach_id_field = vget("SWOT_REACH_ID_FIELD", DEFAULT_REACH_ID_FIELD)

    hydrocron_url = vget("SWOT_HYDROCRON_URL", DEFAULT_HYDROCRON_URL)
    collection_name = vget("SWOT_COLLECTION_NAME", DEFAULT_COLLECTION_NAME)
    fields = vget("SWOT_FIELDS", DEFAULT_FIELDS)

    timeout_s = int(vget("SWOT_TIMEOUT_S", DEFAULT_TIMEOUT_S))
    max_retries = int(vget("SWOT_MAX_RETRIES", DEFAULT_MAX_RETRIES))
    sleep_min_s = float(vget("SWOT_SLEEP_MIN_S", DEFAULT_SLEEP_MIN_S))
    sleep_max_s = float(vget("SWOT_SLEEP_MAX_S", DEFAULT_SLEEP_MAX_S))

    overlap_hours = int(vget("SWOT_OVERLAP_HOURS", DEFAULT_OVERLAP_HOURS))
    backfill_days = int(vget("SWOT_DEFAULT_BACKFILL_DAYS_IF_EMPTY", DEFAULT_BACKFILL_DAYS_IF_EMPTY))

    azure_container_name = vget("SWOT_AZURE_CONTAINER", DEFAULT_AZURE_CONTAINER)
    azure_prefix = vget("SWOT_AZURE_PREFIX", DEFAULT_AZURE_PREFIX).rstrip("/")
    azure_log_blob = f"{azure_prefix}/update_log_daily.csv"

    # connection string: prefer Airflow Variable, then environment variable
    conn_str = vget("AZURE_STORAGE_CONNECTION_STRING", os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")).strip()
    if not conn_str:
        raise RuntimeError(
            "Missing Azure connection string. Set Airflow Variable 'AZURE_STORAGE_CONNECTION_STRING' "
            "or environment variable AZURE_STORAGE_CONNECTION_STRING."
        )

    # --- Run window ---
    end_dt = datetime.now(timezone.utc)
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info("Azure Incremental Update (GeoJSON-driven)")
    logger.info("End time (UTC): %s", end_iso)
    logger.info("GeoJSON: %s", geojson_url)
    logger.info("Azure container: %s", azure_container_name)
    logger.info("Azure prefix: %s", azure_prefix)

    reach_ids = load_reach_ids_from_geojson(
        geojson_url,
        reach_id_field,
        timeout_s=timeout_s,
        max_retries=max_retries,
    )
    logger.info("Reaches: %d", len(reach_ids))

    container = get_container(conn_str, azure_container_name)

    log_rows: List[Dict[str, Any]] = []
    appended_count = 0
    no_change_count = 0
    error_count = 0

    for i, rid in enumerate(reach_ids, 1):
        blob = f"{azure_prefix}/reach_{rid}.csv"

        try:
            existing_text = download_blob_text(container, blob)
            existing_df = safe_read_csv_text(existing_text)

            last_dt = get_last_time_dt(existing_df)
            if last_dt is not None:
                start_dt = last_dt - timedelta(hours=overlap_hours)
            else:
                start_dt = end_dt - timedelta(days=backfill_days)

            if start_dt >= end_dt:
                start_dt = end_dt - timedelta(days=1)

            params = {
                "feature": "Reach",
                "feature_id": rid,
                "start_time": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_time": end_iso,
                "output": "csv",
                "collection_name": collection_name,
                "fields": fields,
            }

            time.sleep(random.uniform(sleep_min_s, sleep_max_s))
            r = request_with_retries(
                hydrocron_url,
                params=params,
                timeout_s=timeout_s,
                max_retries=max_retries,
            )

            # Don’t kill full run on a single bad reach
            if r.status_code == 400:
                msg = (r.text or "").strip()[:250]
                no_change_count += 1
                log_rows.append({
                    "reach_id": rid,
                    "status": f"400_bad_request: {msg}",
                    "start": params["start_time"],
                    "end": params["end_time"],
                    "new_rows": 0,
                    "total_rows": len(existing_df),
                })
                logger.warning("[%d/%d] %s HTTP 400 (skipping)", i, len(reach_ids), rid)
                continue

            r.raise_for_status()

            new_df = clean_new_df(hydrocron_response_to_df(r.text), rid)

            if new_df.empty:
                no_change_count += 1
                log_rows.append({
                    "reach_id": rid,
                    "status": "no_change",
                    "start": params["start_time"],
                    "end": params["end_time"],
                    "new_rows": 0,
                    "total_rows": len(existing_df),
                })
                logger.info("[%d/%d] %s no change (0 rows)", i, len(reach_ids), rid)
                continue

            # timestamp-based “truly new” check
            if (not existing_df.empty) and ("time_str" in existing_df.columns) and ("time_str" in new_df.columns):
                existing_times = set(existing_df["time_str"].astype(str).tolist())
                incoming_times = set(new_df["time_str"].astype(str).tolist())
                if len(incoming_times - existing_times) == 0:
                    no_change_count += 1
                    log_rows.append({
                        "reach_id": rid,
                        "status": "no_change",
                        "start": params["start_time"],
                        "end": params["end_time"],
                        "new_rows": 0,
                        "total_rows": len(existing_df),
                    })
                    logger.info("[%d/%d] %s no change (timestamps already present)", i, len(reach_ids), rid)
                    continue

            out_df = append_dedup(existing_df, new_df)

            buf = io.StringIO()
            out_df.to_csv(buf, index=False)
            upload_blob_text(container, blob, buf.getvalue())

            appended_count += 1
            log_rows.append({
                "reach_id": rid,
                "status": "appended",
                "start": params["start_time"],
                "end": params["end_time"],
                "new_rows": len(new_df),
                "total_rows": len(out_df),
            })
            logger.info("[%d/%d] %s appended new=%d total=%d", i, len(reach_ids), rid, len(new_df), len(out_df))

        except Exception as e:
            error_count += 1
            log_rows.append({
                "reach_id": rid,
                "status": f"error: {e}",
                "start": "",
                "end": end_iso,
                "new_rows": "",
                "total_rows": "",
            })
            logger.exception("[%d/%d] %s ERROR", i, len(reach_ids), rid)

    # Upload run log to Azure
    log_df = pd.DataFrame(log_rows)
    upload_blob_text(container, azure_log_blob, log_df.to_csv(index=False))

    logger.info("SUMMARY appended=%d no_change=%d errors=%d", appended_count, no_change_count, error_count)
    logger.info("Log uploaded: %s", azure_log_blob)

    return {
        "appended": appended_count,
        "no_change": no_change_count,
        "errors": error_count,
        "log_blob": azure_log_blob,
        "end_time_utc": end_iso,
        "reach_count": len(reach_ids),
    }


# -----------------------------------------------------------------------------
# Airflow DAG definition
# -----------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # pick a start_date that makes sense for your deployment; no catchup anyway
    "start_date": datetime(2026, 2, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="swot_hydrocron_incremental_update",
    default_args=default_args,
    description="Daily incremental update of SWOT RiverSP per-reach Hydrocron time series to Azure Blob",
    schedule_interval="0 8 * * *",  # daily 08:00 UTC (adjust as needed)
    catchup=False,
    max_active_runs=1,
    tags=["swot", "hydrocron", "azure", "unesco"],
) as dag:

    run_update = PythonOperator(
        task_id="run_swot_hydrocron_incremental_update",
        python_callable=run_swot_hydrocron_incremental_update,
    )