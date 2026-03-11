"""
Airflow DAG / standalone script: Dnipro SWOT Hydrocron + DAWG updater

What this script does (daily):
  1) Downloads reach_ids from the public Dnipro GeoJSON CKAN resource
  2) For each reach_id:
      - downloads the existing per-reach CSV from Azure Blob Storage (if present)
      - finds the latest timestamp in that CSV
      - queries Hydrocron for the incremental window
      - downloads the latest EU DAWG NetCDF once and merges consensus_q for that window
      - appends + de-dups rows by time_utc
      - uploads the updated CSV back to Azure
  3) Downloads the same GeoJSON resource, refreshes each feature's discharge flags from
     its CSV URL, and updates the CKAN resource in place
  4) Writes a run log CSV to Azure: {AZURE_PREFIX}/update_log_daily.csv

Source mapping for the merged logic:
  - Script structure, config pattern, retry helpers, Azure helpers, and DAG shape:
      context_files/update_SWOT_reaches_hydrocron.py
  - Incremental Hydrocron + DAWG append workflow:
      context_files/riverSP_DAWG_append.ipynb
  - reach_q handling used in the initial download workflow:
      context_files/riverSP_DAWG_download.ipynb
  - GeoJSON discharge annotation + CKAN resource update:
      context_files/update_dnipro_discharge_availability.ipynb
"""

import io
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import requests
from pandas.errors import EmptyDataError

import earthaccess
import netCDF4 as nc
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContainerClient, ContentSettings

try:
    from airflow import DAG
    from airflow.models import Variable
    from airflow.operators.python import PythonOperator
except Exception:  # pragma: no cover - allows standalone execution outside Airflow
    DAG = None
    PythonOperator = None

    class Variable:  # type: ignore[override]
        @staticmethod
        def get(_key: str) -> str:
            raise RuntimeError("Airflow Variable.get unavailable outside Airflow.")


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


DEFAULT_GEOJSON_URL = (
    "https://ihp-wins.unesco.org/dataset/811c5aef-99e8-46e8-a708-12972138b70d/"
    "resource/e5982971-3e89-4f81-a9e3-67333e168e17/download/"
    "dnipro_sword_reaches_clip_with_discharge.geojson"
)
DEFAULT_CKAN_RESOURCE_ID = "e5982971-3e89-4f81-a9e3-67333e168e17"
DEFAULT_GEOJSON_FILENAME = "dnipro_sword_reaches_clip_with_discharge.geojson"
DEFAULT_ORIGINAL_GEOJSON_FILENAME = "dnipro_sword_reaches_clip_with_discharge.original.geojson"
DEFAULT_UPDATED_GEOJSON_FILENAME = "dnipro_sword_reaches_clip_with_discharge.updated.geojson"
DEFAULT_REACH_ID_FIELD = "reach_id"

DEFAULT_HYDROCRON_URL = "https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries"
DEFAULT_COLLECTION_NAME = "SWOT_L2_HR_RiverSP_D"
DEFAULT_FIELDS = "reach_id,time_str,cycle_id,pass_id,wse,slope,width,reach_q"

DEFAULT_TIMEOUT_S = 60
DEFAULT_SLEEP_MIN_S = 0.2
DEFAULT_SLEEP_MAX_S = 0.6
DEFAULT_MAX_RETRIES = 5

DEFAULT_OVERLAP_HOURS = 48
DEFAULT_BACKFILL_DAYS_IF_EMPTY = 2

DEFAULT_AZURE_CONTAINER = "data"
DEFAULT_AZURE_PREFIX = "SWOT/SWOT_dnipro_reach_hydrocron_DAWG"
DEFAULT_PUBLIC_CSV_BASE_URL = "https://ihpwinsdata.blob.core.windows.net/data/SWOT/SWOT_dnipro_reach_hydrocron_DAWG"

DEFAULT_DAWG_SHORT_NAME = "SWOT_L4_HR_DAWG_SOS_DISCHARGE_V3"
DEFAULT_DAWG_REGION_PREFIX = "eu_"
DEFAULT_DAWG_DOWNLOAD_DIR = "./downloaded_files"
DEFAULT_CKAN_API_KEY_ENV_VAR = "IHP_WINS_CKAN_API_KEY"


def vget(key: str, default: Any) -> Any:
    try:
        val = Variable.get(key)
        if val is None:
            return default
        if isinstance(val, str) and val.strip() == "":
            return default
        return val
    except Exception:
        return default


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def request_with_retries(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    max_retries: int = DEFAULT_MAX_RETRIES,
    session: Optional[requests.Session] = None,
) -> requests.Response:
    last_err = None
    client = session or requests
    for attempt in range(1, max_retries + 1):
        try:
            response = client.get(url, params=params or {}, timeout=timeout_s)
            if response.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {response.status_code}")
            return response
        except Exception as exc:
            last_err = exc
            backoff = min(30, (2 ** (attempt - 1)) * 0.7) + random.uniform(0, 0.5)
            time.sleep(backoff)
    raise RuntimeError(f"Failed after {max_retries} retries. Last error: {last_err}")


def earthaccess_login_non_interactive() -> None:
    env_error: Optional[Exception] = None
    netrc_error: Optional[Exception] = None

    try:
        earthaccess.login(strategy="environment")
        logger.info("Authenticated with Earthaccess using environment credentials.")
        return
    except Exception as exc:
        env_error = exc
        logger.info("Earthaccess environment authentication unavailable: %s", exc)

    try:
        earthaccess.login(strategy="netrc")
        logger.info("Authenticated with Earthaccess using .netrc fallback.")
        return
    except Exception as exc:
        netrc_error = exc

    raise RuntimeError(
        "Earthaccess authentication failed. Tried strategy='environment' first "
        "(expects EARTHDATA_USERNAME and EARTHDATA_PASSWORD in the runtime), then "
        "strategy='netrc' as a local-development fallback. Interactive authentication "
        f"is disabled. Environment error: {env_error}; netrc error: {netrc_error}"
    )


def hydrocron_response_to_df(text: str) -> pd.DataFrame:
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


def safe_reach_filename(reach_id: str) -> str:
    s = str(reach_id).strip()
    cleaned = "".join(ch for ch in s if ch.isalnum() or ch in ("-", "_"))
    return cleaned or "unknown_reach"


def load_reach_ids_from_geojson(
    url: str,
    field: str,
    timeout_s: int,
    max_retries: int,
    session: Optional[requests.Session] = None,
) -> List[str]:
    response = request_with_retries(
        url,
        timeout_s=timeout_s,
        max_retries=max_retries,
        session=session,
    )
    obj = response.json()

    features = obj.get("features", []) or []
    out: List[str] = []
    for feature in features:
        props = feature.get("properties", {}) or {}
        val = props.get(field)
        if val is None:
            continue
        s = str(val).strip()
        if s.endswith(".0"):
            s = s[:-2]
        if s:
            out.append(s)

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
    for column in ("time_utc", "time_str"):
        if existing is None or existing.empty or column not in existing.columns:
            continue

        s = existing[column].astype(str).str.strip()
        s = s[(s != "") & (s.str.lower() != "nan") & (s != "no_data")]
        if len(s) == 0:
            continue

        ts = pd.to_datetime(s, errors="coerce", utc=True)
        if ts.notna().any():
            return ts.max().to_pydatetime()

    return None


def normalize_hydrocron_new(df: pd.DataFrame, rid: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["reach_id", "_dt", "cycle_id", "pass_id", "wse", "slope", "width", "reach_q"])

    df = df.copy()
    if "time_str" not in df.columns:
        return pd.DataFrame(columns=["reach_id", "_dt", "cycle_id", "pass_id", "wse", "slope", "width", "reach_q"])

    df = df[df["time_str"] != "no_data"].copy()
    t = pd.to_datetime(df["time_str"], errors="coerce", utc=True)
    df = df.loc[t.notna()].copy()
    df["_dt"] = t.loc[t.notna()].values

    if "wse" in df.columns:
        df = df[df["wse"] > -1e11]

    if "reach_id" not in df.columns:
        df["reach_id"] = str(rid)
    else:
        df["reach_id"] = df["reach_id"].fillna(str(rid)).astype(str)

    preferred = ["reach_id", "_dt", "cycle_id", "pass_id", "wse", "slope", "width", "reach_q"]
    cols = [c for c in preferred if c in df.columns]
    return df[cols].reset_index(drop=True)


def fetch_hydrocron_window(
    rid: str,
    start_dt: datetime,
    end_dt: datetime,
    hydrocron_url: str,
    collection_name: str,
    fields: str,
    timeout_s: int,
    max_retries: int,
    sleep_min_s: float,
    sleep_max_s: float,
) -> pd.DataFrame:
    params = {
        "feature": "Reach",
        "feature_id": rid,
        "start_time": iso_z(start_dt),
        "end_time": iso_z(end_dt),
        "output": "csv",
        "collection_name": collection_name,
        "fields": fields,
    }
    time.sleep(random.uniform(sleep_min_s, sleep_max_s))
    response = request_with_retries(
        hydrocron_url,
        params=params,
        timeout_s=timeout_s,
        max_retries=max_retries,
    )
    response.raise_for_status()
    raw = hydrocron_response_to_df(response.text)
    return normalize_hydrocron_new(raw, rid)


def download_latest_dawg_eu_netcdf(short_name: str, region_prefix: str, download_dir: str) -> str:
    earthaccess_login_non_interactive()
    granules = earthaccess.search_data(short_name=short_name, count=500)
    eu_granules = [g for g in granules if g["meta"]["native-id"].startswith(region_prefix)]
    if not eu_granules:
        raise RuntimeError(f"No DAWG SOS granules found with native-id starting '{region_prefix}'.")

    os.makedirs(download_dir, exist_ok=True)
    paths = earthaccess.download(eu_granules, local_path=download_dir)
    if not paths:
        raise RuntimeError("earthaccess.download returned no files.")

    paths_sorted = sorted(paths, key=lambda p: os.path.basename(p))
    return paths_sorted[-1]


def open_dawg(nc_path: str):
    ds = nc.Dataset(nc_path, "r")
    reaches = ds.groups["reaches"]
    consensus = ds.groups["consensus"]

    nc_reach_ids = reaches.variables["reach_id"][:].astype("int64")
    id_to_idx = {int(rid): int(i) for i, rid in enumerate(nc_reach_ids)}

    qvar = consensus.variables["consensus_q"]
    time_var = consensus.variables["time_int"]

    missing = None
    if "_FillValue" in qvar.ncattrs():
        missing = qvar.getncattr("_FillValue")
    elif "missing_value" in qvar.ncattrs():
        missing = qvar.getncattr("missing_value")

    return ds, id_to_idx, time_var, qvar, missing


def fetch_dawg_window(
    rid: str,
    id_to_idx: dict,
    time_var,
    qvar,
    missing,
    start_dt: datetime,
    end_dt: datetime,
) -> pd.DataFrame:
    cols = ["_dt", "consensus_q"]
    try:
        rid_int = int(float(str(rid).strip()))
    except Exception:
        return pd.DataFrame(columns=cols)

    if rid_int not in id_to_idx:
        return pd.DataFrame(columns=cols)

    idx = id_to_idx[rid_int]

    times = np.asarray(time_var[idx], dtype="float64")
    valid_time = times > -9.0e10
    times_valid = times[valid_time].astype("int64")
    if times_valid.size == 0:
        return pd.DataFrame(columns=cols)

    dt64 = np.array(
        [np.datetime64("2000-01-01T00:00:00") + np.timedelta64(int(t), "s") for t in times_valid]
    )
    dt = pd.to_datetime(dt64.astype("datetime64[ns]"), utc=True)

    q = np.asarray(qvar[idx], dtype="float64")[valid_time]
    if missing is not None:
        q[q == missing] = np.nan
    q[q <= -9.0e10] = np.nan

    df = pd.DataFrame({"_dt": dt, "consensus_q": q}).dropna(subset=["_dt"])

    start_dt = start_dt.astimezone(timezone.utc)
    end_dt = end_dt.astimezone(timezone.utc)
    return df[(df["_dt"] >= start_dt) & (df["_dt"] <= end_dt)].copy()


def build_output_chunk(rid: str, hydro_df: pd.DataFrame, dawg_df: pd.DataFrame) -> pd.DataFrame:
    hydro_df = hydro_df.copy()
    dawg_df = dawg_df.copy()

    hydro_df["_dt"] = pd.to_datetime(hydro_df.get("_dt"), errors="coerce", utc=True)
    dawg_df["_dt"] = pd.to_datetime(dawg_df.get("_dt"), errors="coerce", utc=True)

    if hydro_df.empty and dawg_df.empty:
        return pd.DataFrame(
            columns=["reach_id", "time_utc", "cycle_id", "pass_id", "wse", "slope", "width", "reach_q", "consensus_q"]
        )

    if hydro_df.empty:
        merged = dawg_df.copy()
    elif dawg_df.empty:
        merged = hydro_df.copy()
    else:
        merged = pd.merge(hydro_df, dawg_df[["_dt", "consensus_q"]], on="_dt", how="outer")

    merged["reach_id"] = str(rid)
    merged = merged.drop_duplicates(subset=["_dt"], keep="last").sort_values("_dt")
    merged["time_utc"] = pd.to_datetime(merged["_dt"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    merged = merged.drop(columns=["_dt"], errors="ignore")

    front = ["reach_id", "time_utc", "cycle_id", "pass_id", "wse", "slope", "width", "reach_q", "consensus_q"]
    for column in front:
        if column not in merged.columns:
            merged[column] = pd.NA
    remaining = [c for c in merged.columns if c not in front]
    return merged[front + remaining].reset_index(drop=True)


def append_dedup_time_utc(existing: pd.DataFrame, new_chunk: pd.DataFrame) -> pd.DataFrame:
    if existing is None or existing.empty:
        out = new_chunk.copy()
    elif new_chunk is None or new_chunk.empty:
        out = existing.copy()
    else:
        out = pd.concat([existing, new_chunk], ignore_index=True, sort=False)

    if "time_utc" in out.columns:
        out["time_utc"] = out["time_utc"].astype(str)
        out = out.drop_duplicates(subset=["time_utc"], keep="last")
        out = out.sort_values("time_utc")

    return out.reset_index(drop=True)


def get_container(conn_str: str, container_name: str) -> ContainerClient:
    if not conn_str.strip():
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is empty or not set.")
    return ContainerClient.from_connection_string(conn_str, container_name)


def download_blob_text(container: ContainerClient, blob_name: str) -> Optional[str]:
    try:
        blob_client = container.get_blob_client(blob_name)
        return blob_client.download_blob().readall().decode("utf-8", errors="replace")
    except ResourceNotFoundError:
        return None


def upload_blob_text(container: ContainerClient, blob_name: str, text: str) -> None:
    blob_client = container.get_blob_client(blob_name)
    blob_client.upload_blob(
        text.encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="text/csv; charset=utf-8"),
    )


def get_ckan_base_url(resource_url: str) -> str:
    parsed = urlparse(resource_url)
    return f"{parsed.scheme}://{parsed.netloc}"


def download_geojson(
    download_url: str,
    timeout_s: int,
    max_retries: int,
    session: Optional[requests.Session] = None,
) -> dict:
    response = request_with_retries(
        download_url,
        timeout_s=timeout_s,
        max_retries=max_retries,
        session=session,
    )
    response.raise_for_status()
    return response.json()


def build_public_csv_url(public_csv_base_url: str, rid: str) -> str:
    return f"{public_csv_base_url.rstrip('/')}/reach_{safe_reach_filename(rid)}.csv"


def set_feature_csv_url(properties: Dict[str, Any], csv_url: str) -> None:
    if "URL" in properties:
        properties["URL"] = csv_url
    properties["url"] = csv_url


def get_feature_csv_url(feature: dict) -> Optional[str]:
    properties = feature.get("properties") or {}
    return properties.get("URL") or properties.get("url")


def load_csv_dataframe(download_session: requests.Session, csv_url: str, timeout_s: int):
    try:
        response = download_session.get(csv_url, timeout=timeout_s)
        response.raise_for_status()
        return pd.read_csv(io.StringIO(response.text)), None
    except Exception:
        return None, "unreachable"


def compute_discharge_attributes(
    download_session: requests.Session,
    csv_url: str,
    timeout_s: int,
) -> Tuple[bool, int, Optional[str]]:
    dataframe, load_error = load_csv_dataframe(download_session, csv_url, timeout_s)
    if dataframe is None:
        return False, 0, load_error or "unreachable"

    if "consensus_q" not in dataframe.columns:
        return False, 0, "missing_consensus_q"

    numeric_values = pd.to_numeric(dataframe["consensus_q"], errors="coerce")
    discharge_count = int(numeric_values.notna().sum())
    return discharge_count > 0, discharge_count, None


def annotate_geojson(
    download_session: requests.Session,
    geojson_data: dict,
    public_csv_base_url: str,
    timeout_s: int,
) -> Tuple[dict, Dict[str, int]]:
    summary = {
        "total_reaches": 0,
        "reaches_with_discharge": 0,
        "reaches_without_discharge": 0,
        "missing_or_unreachable_csv_urls": 0,
        "csvs_missing_consensus_q": 0,
    }

    for feature in geojson_data.get("features", []):
        summary["total_reaches"] += 1
        properties = feature.setdefault("properties", {})
        rid = str(properties.get("reach_id", "")).strip()
        if rid.endswith(".0"):
            rid = rid[:-2]

        if rid:
            set_feature_csv_url(properties, build_public_csv_url(public_csv_base_url, rid))

        csv_url = get_feature_csv_url(feature)
        if not csv_url:
            properties["has_discharge"] = False
            properties["discharge_count"] = 0
            summary["reaches_without_discharge"] += 1
            summary["missing_or_unreachable_csv_urls"] += 1
            continue

        has_discharge, discharge_count, error_type = compute_discharge_attributes(
            download_session,
            csv_url,
            timeout_s,
        )
        properties["has_discharge"] = has_discharge
        properties["discharge_count"] = discharge_count

        if has_discharge:
            summary["reaches_with_discharge"] += 1
        else:
            summary["reaches_without_discharge"] += 1

        if error_type == "unreachable":
            summary["missing_or_unreachable_csv_urls"] += 1
        elif error_type == "missing_consensus_q":
            summary["csvs_missing_consensus_q"] += 1

    return geojson_data, summary


def save_geojson(geojson_data: dict, output_path: Path) -> None:
    output_path.write_text(json.dumps(geojson_data, ensure_ascii=False, indent=2), encoding="utf-8")


def save_response_text(text: str, output_path: Path) -> None:
    output_path.write_text(text, encoding="utf-8")


def upload_geojson_resource(
    session: requests.Session,
    ckan_base_url: str,
    resource_id: str,
    output_filename: str,
    local_geojson_path: Path,
    timeout_s: int,
) -> requests.Response:
    data = {
        "id": resource_id,
        "format": "GeoJSON",
    }

    with local_geojson_path.open("rb") as file_handle:
        response = session.post(
            f"{ckan_base_url}/api/3/action/resource_update",
            data=data,
            files={"upload": (output_filename, file_handle, "application/geo+json")},
            timeout=timeout_s,
        )
    return response


def format_ckan_error(exc: Exception) -> str:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        response = exc.response
        try:
            return json.dumps(response.json(), ensure_ascii=False)
        except ValueError:
            return response.text.strip() or str(exc)
    return str(exc)


def run_dnipro_swot_hydrocron_discharge_update(**context) -> Dict[str, Any]:
    geojson_url = vget("SWOT_GEOJSON_URL", DEFAULT_GEOJSON_URL)
    ckan_resource_id = vget("SWOT_CKAN_RESOURCE_ID", DEFAULT_CKAN_RESOURCE_ID)
    geojson_filename = vget("SWOT_GEOJSON_FILENAME", DEFAULT_GEOJSON_FILENAME)
    original_geojson_filename = vget("SWOT_ORIGINAL_GEOJSON_FILENAME", DEFAULT_ORIGINAL_GEOJSON_FILENAME)
    updated_geojson_filename = vget("SWOT_UPDATED_GEOJSON_FILENAME", DEFAULT_UPDATED_GEOJSON_FILENAME)
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
    public_csv_base_url = vget("SWOT_PUBLIC_CSV_BASE_URL", DEFAULT_PUBLIC_CSV_BASE_URL).rstrip("/")
    azure_log_blob = f"{azure_prefix}/update_log_daily.csv"

    dawg_short_name = vget("SWOT_DAWG_SHORT_NAME", DEFAULT_DAWG_SHORT_NAME)
    dawg_region_prefix = vget("SWOT_DAWG_REGION_PREFIX", DEFAULT_DAWG_REGION_PREFIX)
    dawg_download_dir = vget("SWOT_DAWG_DOWNLOAD_DIR", DEFAULT_DAWG_DOWNLOAD_DIR)

    conn_str = vget(
        "AZURE_STORAGE_CONNECTION_STRING",
        os.environ.get("AZURE_STORAGE_CONNECTION_STRING", ""),
    ).strip()
    if not conn_str:
        raise RuntimeError(
            "Missing Azure connection string. Set Airflow Variable 'AZURE_STORAGE_CONNECTION_STRING' "
            "or environment variable AZURE_STORAGE_CONNECTION_STRING."
        )

    ckan_api_key_env_var = vget("SWOT_CKAN_API_KEY_ENV_VAR", DEFAULT_CKAN_API_KEY_ENV_VAR).strip()
    ckan_api_key = vget(
        "CKAN_API_KEY",
        os.environ.get(ckan_api_key_env_var, os.environ.get("CKAN_API_KEY", "")),
    ).strip()
    if not ckan_api_key:
        raise RuntimeError(
            f"Missing CKAN API key. Set Airflow Variable 'CKAN_API_KEY' or environment variable "
            f"{ckan_api_key_env_var}."
        )

    base_dir = Path(__file__).resolve().parent
    original_geojson_path = base_dir / original_geojson_filename
    updated_geojson_path = base_dir / updated_geojson_filename
    ckan_base_url = get_ckan_base_url(geojson_url)

    end_dt = datetime.now(timezone.utc)
    end_iso = iso_z(end_dt)

    logger.info("Dnipro SWOT Hydrocron + discharge update")
    logger.info("End time (UTC): %s", end_iso)
    logger.info("GeoJSON URL: %s", geojson_url)
    logger.info("Azure container: %s", azure_container_name)
    logger.info("Azure prefix: %s", azure_prefix)
    logger.info("CKAN resource id: %s", ckan_resource_id)

    reach_ids = load_reach_ids_from_geojson(
        geojson_url,
        reach_id_field,
        timeout_s=timeout_s,
        max_retries=max_retries,
    )
    logger.info("Reaches: %d", len(reach_ids))

    container = get_container(conn_str, azure_container_name)

    try:
        dawg_nc_path = download_latest_dawg_eu_netcdf(
            short_name=dawg_short_name,
            region_prefix=dawg_region_prefix,
            download_dir=dawg_download_dir,
        )
        logger.info("Using DAWG NetCDF: %s", dawg_nc_path)
        ds, id_to_idx, time_var, qvar, missing = open_dawg(dawg_nc_path)
    except Exception as exc:
        logger.warning("DAWG download/open failed; proceeding with Hydrocron-only append: %s", exc)
        ds = None
        id_to_idx = time_var = qvar = missing = None

    log_rows: List[Dict[str, Any]] = []
    appended_count = 0
    no_change_count = 0
    error_count = 0

    try:
        for i, rid in enumerate(reach_ids, 1):
            blob_name = f"{azure_prefix}/reach_{safe_reach_filename(rid)}.csv"

            try:
                existing_text = download_blob_text(container, blob_name)
                existing_df = safe_read_csv_text(existing_text)

                last_dt = get_last_time_dt(existing_df)
                if last_dt is not None:
                    start_dt = last_dt - timedelta(hours=overlap_hours)
                else:
                    start_dt = end_dt - timedelta(days=backfill_days)

                if start_dt >= end_dt:
                    start_dt = end_dt - timedelta(days=1)

                hydro_new = fetch_hydrocron_window(
                    rid=rid,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    hydrocron_url=hydrocron_url,
                    collection_name=collection_name,
                    fields=fields,
                    timeout_s=timeout_s,
                    max_retries=max_retries,
                    sleep_min_s=sleep_min_s,
                    sleep_max_s=sleep_max_s,
                )

                if ds is not None:
                    dawg_new = fetch_dawg_window(
                        rid=rid,
                        id_to_idx=id_to_idx,
                        time_var=time_var,
                        qvar=qvar,
                        missing=missing,
                        start_dt=start_dt,
                        end_dt=end_dt,
                    )
                else:
                    dawg_new = pd.DataFrame(columns=["_dt", "consensus_q"])

                new_chunk = build_output_chunk(rid, hydro_new, dawg_new)
                if new_chunk.empty:
                    no_change_count += 1
                    log_rows.append(
                        {
                            "reach_id": rid,
                            "status": "no_change",
                            "start": iso_z(start_dt),
                            "end": end_iso,
                            "hydro_rows": len(hydro_new),
                            "dawg_rows": len(dawg_new),
                            "new_rows": 0,
                            "total_rows": len(existing_df),
                        }
                    )
                    logger.info("[%d/%d] %s no change (hydro=0 dawg=0)", i, len(reach_ids), rid)
                    continue

                if not existing_df.empty and "time_utc" in existing_df.columns:
                    existing_times = set(existing_df["time_utc"].astype(str).tolist())
                    incoming_times = set(new_chunk["time_utc"].astype(str).tolist())
                    if len(incoming_times - existing_times) == 0:
                        no_change_count += 1
                        log_rows.append(
                            {
                                "reach_id": rid,
                                "status": "no_change",
                                "start": iso_z(start_dt),
                                "end": end_iso,
                                "hydro_rows": len(hydro_new),
                                "dawg_rows": len(dawg_new),
                                "new_rows": 0,
                                "total_rows": len(existing_df),
                            }
                        )
                        logger.info("[%d/%d] %s no change (timestamps already present)", i, len(reach_ids), rid)
                        continue

                out_df = append_dedup_time_utc(existing_df, new_chunk)
                buffer = io.StringIO()
                out_df.to_csv(buffer, index=False)
                upload_blob_text(container, blob_name, buffer.getvalue())

                appended_count += 1
                log_rows.append(
                    {
                        "reach_id": rid,
                        "status": "appended",
                        "start": iso_z(start_dt),
                        "end": end_iso,
                        "hydro_rows": len(hydro_new),
                        "dawg_rows": len(dawg_new),
                        "new_rows": len(new_chunk),
                        "total_rows": len(out_df),
                    }
                )
                logger.info(
                    "[%d/%d] %s appended hydro=%d dawg=%d total=%d",
                    i,
                    len(reach_ids),
                    rid,
                    len(hydro_new),
                    len(dawg_new),
                    len(out_df),
                )

            except Exception as exc:
                error_count += 1
                log_rows.append(
                    {
                        "reach_id": rid,
                        "status": f"error: {exc}",
                        "start": "",
                        "end": end_iso,
                        "hydro_rows": "",
                        "dawg_rows": "",
                        "new_rows": "",
                        "total_rows": "",
                    }
                )
                logger.exception("[%d/%d] %s ERROR", i, len(reach_ids), rid)
    finally:
        if ds is not None:
            ds.close()

    log_df = pd.DataFrame(log_rows)
    upload_blob_text(container, azure_log_blob, log_df.to_csv(index=False))
    logger.info("Run log uploaded: %s", azure_log_blob)

    download_session = requests.Session()
    ckan_session = requests.Session()
    ckan_session.headers.update({"Authorization": ckan_api_key, "X-CKAN-API-Key": ckan_api_key})

    geojson_response = request_with_retries(
        geojson_url,
        timeout_s=timeout_s,
        max_retries=max_retries,
        session=download_session,
    )
    geojson_response.raise_for_status()
    save_response_text(geojson_response.text, original_geojson_path)

    geojson_data = geojson_response.json()
    updated_geojson, discharge_summary = annotate_geojson(
        download_session=download_session,
        geojson_data=geojson_data,
        public_csv_base_url=public_csv_base_url,
        timeout_s=timeout_s,
    )
    save_geojson(updated_geojson, updated_geojson_path)

    try:
        update_response = upload_geojson_resource(
            session=ckan_session,
            ckan_base_url=ckan_base_url,
            resource_id=ckan_resource_id,
            output_filename=geojson_filename,
            local_geojson_path=updated_geojson_path,
            timeout_s=timeout_s,
        )
        update_response.raise_for_status()
        payload = update_response.json()
        if not payload.get("success"):
            raise RuntimeError(json.dumps(payload, ensure_ascii=False))
        geojson_update_succeeded = True
    except Exception as exc:
        geojson_update_succeeded = False
        logger.error("CKAN resource update failed: %s", format_ckan_error(exc))
        raise

    logger.info(
        "SUMMARY appended=%d no_change=%d errors=%d reaches_with_discharge=%d reaches_without_discharge=%d",
        appended_count,
        no_change_count,
        error_count,
        discharge_summary["reaches_with_discharge"],
        discharge_summary["reaches_without_discharge"],
    )

    return {
        "appended": appended_count,
        "no_change": no_change_count,
        "errors": error_count,
        "log_blob": azure_log_blob,
        "end_time_utc": end_iso,
        "reach_count": len(reach_ids),
        "original_geojson_output": str(original_geojson_path),
        "updated_geojson_output": str(updated_geojson_path),
        "geojson_update_succeeded": geojson_update_succeeded,
        "reaches_with_discharge": discharge_summary["reaches_with_discharge"],
        "reaches_without_discharge": discharge_summary["reaches_without_discharge"],
        "missing_or_unreachable_csv_urls": discharge_summary["missing_or_unreachable_csv_urls"],
        "csvs_missing_consensus_q": discharge_summary["csvs_missing_consensus_q"],
    }


if DAG is not None and PythonOperator is not None:
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2026, 3, 10),
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    }

    with DAG(
        dag_id="dnipro_swot_hydrocron_discharge_update",
        default_args=default_args,
        description="Daily incremental update of Dnipro SWOT per-reach CSVs plus CKAN GeoJSON discharge flags",
        schedule_interval="0 8 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["swot", "hydrocron", "dawg", "azure", "ckan", "unesco", "dnipro"],
    ) as dag:
        run_update = PythonOperator(
            task_id="run_dnipro_swot_hydrocron_discharge_update",
            python_callable=run_dnipro_swot_hydrocron_discharge_update,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run_dnipro_swot_hydrocron_discharge_update()
