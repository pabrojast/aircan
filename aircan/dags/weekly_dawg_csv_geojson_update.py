"""
Airflow-compatible weekly DAWG consensus_q updater.

This script only runs downstream updates when a newer EU DAWG NetCDF is found in
Earthaccess than the DAWG `.nc` currently stored in Azure. When triggered, it:
  1) downloads and uploads the new DAWG NetCDF to Azure,
  2) deletes the previous Azure DAWG `.nc`,
  3) updates only `consensus_q` in the existing reach CSVs,
  4) updates the existing discharge-availability GeoJSON resource in CKAN.

It does not update Hydrocron variables or any non-DAWG CSV fields.

Required runtime credentials:
  - AZURE_STORAGE_CONNECTION_STRING
  - CKAN_API_KEY (or IHP_WINS_CKAN_API_KEY)
  - NASA_USERNAME
  - NASA_PASSWORD
"""

import io
import json
import logging
import os
import random
import stat
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import earthaccess
import netCDF4 as nc
import numpy as np
import pandas as pd
import requests
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContainerClient, ContentSettings
from pandas.errors import EmptyDataError

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Keep runtime logs readable in Airflow task output.
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.getLogger("earthaccess").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


DEFAULT_AZURE_CONTAINER = "data"
DEFAULT_AZURE_DAWG_BLOB_PREFIX = "SWOT/DAWG_discharge_raw data"
DEFAULT_AZURE_CSV_PREFIX = "SWOT/SWOT_dnipro_reach_hydrocron_DAWG"
DEFAULT_PUBLIC_CSV_BASE_URL = (
    "https://ihpwinsdata.blob.core.windows.net/data/SWOT/SWOT_dnipro_reach_hydrocron_DAWG"
)

DEFAULT_DAWG_SHORT_NAME = "SWOT_L4_HR_DAWG_SOS_DISCHARGE_V3"
DEFAULT_DAWG_REGION_PREFIX = "eu_"
DEFAULT_DAWG_DOWNLOAD_TMP_DIR = ""

DEFAULT_GEOJSON_URL = (
    "https://ihp-wins.unesco.org/dataset/811c5aef-99e8-46e8-a708-12972138b70d/"
    "resource/e5982971-3e89-4f81-a9e3-67333e168e17/download/"
    "dnipro_sword_reaches_clip_with_discharge.geojson"
)
DEFAULT_CKAN_RESOURCE_ID = "e5982971-3e89-4f81-a9e3-67333e168e17"
DEFAULT_GEOJSON_FILENAME = "dnipro_sword_reaches_clip_with_discharge.geojson"

DEFAULT_TIMEOUT_S = 60
DEFAULT_MAX_RETRIES = 5
DEFAULT_OVERLAP_HOURS = 48
DEFAULT_BACKFILL_DAYS_IF_EMPTY = 2


def vget(key: str, default: Any) -> Any:
    env_val = os.environ.get(key)
    if env_val is not None and str(env_val).strip() != "":
        return env_val

    if Variable is not None:
        try:
            val = Variable.get(key)
            if val is not None and str(val).strip() != "":
                return val
        except Exception:
            pass

    return default


def require_value(name: str, value: str) -> str:
    if not str(value or "").strip():
        raise RuntimeError(f"Missing required runtime configuration: {name}")
    return str(value).strip()


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


# DAWG detection/download/upload helpers adapted from weekly_update_dawg_latest_to_azure.ipynb.
TIMESTAMP_RE = __import__("re").compile(r"(\d{8}T\d{6})")


def sort_key_from_filename(name: str) -> Tuple[str, ...]:
    return tuple(TIMESTAMP_RE.findall(name))


def safe_blob_join(prefix: str, filename: str) -> str:
    return f"{prefix.rstrip('/')}/{filename}"


def safe_reach_filename(reach_id: str) -> str:
    s = str(reach_id).strip()
    cleaned = "".join(ch for ch in s if ch.isalnum() or ch in ("-", "_"))
    return cleaned or "unknown_reach"


def strip_nc_extension(name: Optional[str]) -> Optional[str]:
    if name is None:
        return None
    return name[:-3] if name.lower().endswith(".nc") else name


def add_nc_extension(name: str) -> str:
    return name if name.lower().endswith(".nc") else f"{name}.nc"


def get_container(conn_str: str, container_name: str) -> ContainerClient:
    return BlobServiceClient.from_connection_string(conn_str).get_container_client(container_name)


def list_nc_blobs_in_prefix(container: ContainerClient, prefix: str) -> List[str]:
    prefix_with_slash = prefix.rstrip("/") + "/"
    blobs: List[str] = []
    for blob in container.list_blobs(name_starts_with=prefix_with_slash):
        if blob.name.lower().endswith(".nc"):
            blobs.append(blob.name)
    return blobs


def get_current_azure_latest_filename(container: ContainerClient, prefix: str) -> Optional[str]:
    blobs = list_nc_blobs_in_prefix(container, prefix)
    if not blobs:
        return None
    basenames = [Path(blob_name).name for blob_name in blobs]
    return sorted(basenames, key=sort_key_from_filename)[-1]


def create_runtime_netrc(nasa_username: str, nasa_password: str) -> Path:
    home = Path.home()
    netrc_path = home / ".netrc"
    contents = (
        "machine urs.earthdata.nasa.gov\n"
        f"login {nasa_username}\n"
        f"password {nasa_password}\n"
    )
    netrc_path.write_text(contents, encoding="utf-8")
    try:
        os.chmod(netrc_path, stat.S_IRUSR | stat.S_IWUSR)
    except Exception:
        logger.warning("Could not tighten permissions on %s; continuing.", netrc_path)
    return netrc_path


def earthaccess_login_airflow_style(nasa_username: str, nasa_password: str) -> Path:
    netrc_path = create_runtime_netrc(nasa_username, nasa_password)
    earthaccess.login(strategy="netrc")
    return netrc_path


def get_latest_matching_earthaccess_granule(short_name: str, region_prefix: str):
    granules = earthaccess.search_data(short_name=short_name, count=500)
    eu_granules = []
    for granule in granules:
        native_id = granule["meta"].get("native-id")
        if native_id and native_id.startswith(region_prefix):
            eu_granules.append((native_id, granule))

    if not eu_granules:
        raise RuntimeError(
            f"No DAWG SOS granules found with native-id starting '{region_prefix}'."
        )

    latest_name, latest_granule = sorted(
        eu_granules,
        key=lambda x: sort_key_from_filename(x[0]),
    )[-1]
    return latest_name, latest_granule


def get_runtime_temp_dir(configured_dir: str = "") -> Path:
    if configured_dir.strip():
        temp_dir = Path(configured_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        return temp_dir
    return Path(tempfile.mkdtemp(prefix="weekly_dawg_"))


def download_granule_to_local(granule, local_tmp_dir: Path) -> Path:
    local_tmp_dir.mkdir(parents=True, exist_ok=True)
    paths = earthaccess.download([granule], local_path=str(local_tmp_dir))
    if not paths:
        raise RuntimeError("earthaccess.download returned no files.")
    return Path(paths[0])


def upload_file_to_blob(container: ContainerClient, local_path: Path, blob_name: str) -> None:
    with local_path.open("rb") as file_handle:
        container.get_blob_client(blob_name).upload_blob(file_handle, overwrite=True)


def delete_other_nc_blobs_in_prefix(
    container: ContainerClient,
    prefix: str,
    keep_filename: str,
) -> List[str]:
    keep_blob_name = safe_blob_join(prefix, keep_filename)
    deleted = []
    for blob_name in list_nc_blobs_in_prefix(container, prefix):
        if blob_name != keep_blob_name:
            container.delete_blob(blob_name)
            deleted.append(blob_name)
    return deleted


# CSV update helpers adapted from context/riverSP_DAWG_append.ipynb.
def safe_read_csv_text(text: Optional[str]) -> pd.DataFrame:
    if text is None or str(text).strip() == "":
        return pd.DataFrame()
    try:
        return pd.read_csv(io.StringIO(str(text)))
    except EmptyDataError:
        return pd.DataFrame()


def download_blob_text(container: ContainerClient, blob_name: str) -> Optional[str]:
    try:
        return container.get_blob_client(blob_name).download_blob().readall().decode(
            "utf-8",
            errors="replace",
        )
    except ResourceNotFoundError:
        return None


def upload_blob_text(container: ContainerClient, blob_name: str, text: str) -> None:
    container.get_blob_client(blob_name).upload_blob(
        text.encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="text/csv; charset=utf-8"),
    )


def get_last_time_dt(existing: pd.DataFrame) -> Optional[datetime]:
    if existing is None or existing.empty or "time_utc" not in existing.columns:
        return None

    s = existing["time_utc"].astype(str).str.strip()
    s = s[(s != "") & (s.str.lower() != "nan") & (s != "no_data")]
    if len(s) == 0:
        return None

    ts = pd.to_datetime(s, errors="coerce", utc=True)
    if ts.notna().any():
        return ts.max().to_pydatetime()
    return None


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
    try:
        rid_int = int(float(str(rid).strip()))
    except Exception:
        return pd.DataFrame(columns=["time_utc", "consensus_q"])

    if rid_int not in id_to_idx:
        return pd.DataFrame(columns=["time_utc", "consensus_q"])

    idx = id_to_idx[rid_int]
    times = np.asarray(time_var[idx], dtype="float64")
    valid_time = times > -9.0e10
    times_valid = times[valid_time].astype("int64")
    if times_valid.size == 0:
        return pd.DataFrame(columns=["time_utc", "consensus_q"])

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
    df = df[(df["_dt"] >= start_dt) & (df["_dt"] <= end_dt)].copy()
    df["time_utc"] = pd.to_datetime(df["_dt"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return df[["time_utc", "consensus_q"]].reset_index(drop=True)


def update_existing_csv_consensus_q(existing_df: pd.DataFrame, dawg_df: pd.DataFrame) -> pd.DataFrame:
    if existing_df is None or existing_df.empty:
        return existing_df.copy()

    out_df = existing_df.copy()
    original_columns = list(out_df.columns)

    if "time_utc" not in out_df.columns:
        raise RuntimeError("Existing CSV is missing required column 'time_utc'.")

    if "consensus_q" not in out_df.columns:
        raise RuntimeError("Existing CSV is missing required column 'consensus_q'.")

    if dawg_df is None or dawg_df.empty:
        return out_df[original_columns].reset_index(drop=True)

    dawg_indexed = dawg_df.drop_duplicates(subset=["time_utc"], keep="last").set_index("time_utc")
    out_df = out_df.copy()
    out_df["time_utc"] = out_df["time_utc"].astype(str)

    existing_mask = out_df["time_utc"].isin(dawg_indexed.index)
    if existing_mask.any():
        matched_values = dawg_indexed.loc[out_df.loc[existing_mask, "time_utc"], "consensus_q"].to_numpy()
        out_df.loc[existing_mask, "consensus_q"] = matched_values

    new_times = [t for t in dawg_indexed.index.tolist() if t not in set(out_df["time_utc"].tolist())]
    if new_times:
        new_rows = pd.DataFrame(columns=original_columns)
        for column in original_columns:
            if column == "time_utc":
                new_rows[column] = new_times
            elif column == "consensus_q":
                new_rows[column] = dawg_indexed.loc[new_times, "consensus_q"].to_numpy()
            else:
                new_rows[column] = pd.NA
        if "reach_id" in new_rows.columns and "reach_id" in out_df.columns and not out_df["reach_id"].dropna().empty:
            new_rows["reach_id"] = out_df["reach_id"].dropna().iloc[0]
        out_df = pd.concat([out_df, new_rows], ignore_index=True, sort=False)

    out_df = out_df.drop_duplicates(subset=["time_utc"], keep="last")
    out_df = out_df.sort_values("time_utc").reset_index(drop=True)
    return out_df[original_columns]


def load_reach_ids_from_geojson(
    geojson_url: str,
    timeout_s: int,
    max_retries: int,
) -> List[str]:
    session = requests.Session()
    response = request_with_retries(
        geojson_url,
        timeout_s=timeout_s,
        max_retries=max_retries,
        session=session,
    )
    response.raise_for_status()
    geojson_data = response.json()

    reach_ids: List[str] = []
    seen = set()
    for feature in geojson_data.get("features", []):
        rid = str((feature.get("properties") or {}).get("reach_id", "")).strip()
        if rid.endswith(".0"):
            rid = rid[:-2]
        if rid and rid not in seen:
            seen.add(rid)
            reach_ids.append(rid)
    return reach_ids


# GeoJSON update helpers adapted from context/update_dnipro_discharge_availability.ipynb.
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


def get_ckan_base_url(resource_url: str) -> str:
    parsed = urlparse(resource_url)
    return f"{parsed.scheme}://{parsed.netloc}"


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


def update_reach_csvs(
    container: ContainerClient,
    azure_csv_prefix: str,
    geojson_url: str,
    local_nc_path: Path,
    run_end_dt: datetime,
    overlap_hours: int,
    backfill_days_if_empty: int,
    timeout_s: int,
    max_retries: int,
) -> Dict[str, int]:
    reach_ids = load_reach_ids_from_geojson(geojson_url, timeout_s, max_retries)
    ds, id_to_idx, time_var, qvar, missing = open_dawg(str(local_nc_path))

    updated_count = 0
    no_change_count = 0
    missing_csv_count = 0
    error_count = 0

    try:
        for i, rid in enumerate(reach_ids, start=1):
            blob_name = f"{azure_csv_prefix}/reach_{safe_reach_filename(rid)}.csv"
            existing_text = download_blob_text(container, blob_name)

            if existing_text is None:
                missing_csv_count += 1
                logger.info("[%d/%d] %s missing CSV in Azure, skipping", i, len(reach_ids), rid)
                continue

            existing_df = safe_read_csv_text(existing_text)
            if existing_df.empty:
                start_dt = run_end_dt - timedelta(days=backfill_days_if_empty)
            else:
                last_dt = get_last_time_dt(existing_df)
                if last_dt is None:
                    start_dt = run_end_dt - timedelta(days=backfill_days_if_empty)
                else:
                    start_dt = last_dt - timedelta(hours=overlap_hours)

            if start_dt >= run_end_dt:
                start_dt = run_end_dt - timedelta(days=1)

            try:
                dawg_new = fetch_dawg_window(
                    rid,
                    id_to_idx,
                    time_var,
                    qvar,
                    missing,
                    start_dt,
                    run_end_dt,
                )
                if dawg_new.empty:
                    no_change_count += 1
                    logger.info("[%d/%d] %s no DAWG rows in window", i, len(reach_ids), rid)
                    continue

                out_df = update_existing_csv_consensus_q(existing_df, dawg_new)
                if out_df.equals(existing_df):
                    no_change_count += 1
                    logger.info("[%d/%d] %s no change", i, len(reach_ids), rid)
                    continue

                buffer = io.StringIO()
                out_df.to_csv(buffer, index=False)
                upload_blob_text(container, blob_name, buffer.getvalue())

                updated_count += 1
                logger.info("[%d/%d] %s updated rows=%d", i, len(reach_ids), rid, len(out_df))
            except Exception as exc:
                error_count += 1
                logger.exception("[%d/%d] %s CSV update failed: %s", i, len(reach_ids), rid, exc)
    finally:
        ds.close()

    return {
        "total_reaches": len(reach_ids),
        "updated": updated_count,
        "no_change": no_change_count,
        "missing_csv": missing_csv_count,
        "errors": error_count,
    }


def update_geojson_resource(
    geojson_url: str,
    resource_id: str,
    geojson_filename: str,
    public_csv_base_url: str,
    ckan_api_key: str,
    timeout_s: int,
    max_retries: int,
    runtime_temp_dir: Path,
) -> Dict[str, Any]:
    download_session = requests.Session()
    ckan_session = requests.Session()
    ckan_session.headers.update({"Authorization": ckan_api_key, "X-CKAN-API-Key": ckan_api_key})

    ckan_base_url = get_ckan_base_url(geojson_url)
    geojson_response = request_with_retries(
        geojson_url,
        timeout_s=timeout_s,
        max_retries=max_retries,
        session=download_session,
    )
    geojson_response.raise_for_status()

    geojson_data = geojson_response.json()
    updated_geojson, summary = annotate_geojson(
        download_session,
        geojson_data,
        public_csv_base_url,
        timeout_s,
    )

    local_geojson_path = runtime_temp_dir / geojson_filename
    save_geojson(updated_geojson, local_geojson_path)

    response = upload_geojson_resource(
        session=ckan_session,
        ckan_base_url=ckan_base_url,
        resource_id=resource_id,
        output_filename=geojson_filename,
        local_geojson_path=local_geojson_path,
        timeout_s=timeout_s,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success"):
        raise RuntimeError(json.dumps(payload, ensure_ascii=False))

    summary["upload_succeeded"] = True
    summary["local_geojson_path"] = str(local_geojson_path.resolve())
    return summary


def run_weekly_dawg_csv_geojson_update() -> Dict[str, Any]:
    azure_storage_connection_string = require_value(
        "AZURE_STORAGE_CONNECTION_STRING",
        vget("AZURE_STORAGE_CONNECTION_STRING", ""),
    )
    ckan_api_key = require_value(
        "CKAN_API_KEY",
        vget("CKAN_API_KEY", vget("IHP_WINS_CKAN_API_KEY", "")),
    )
    nasa_username = require_value("NASA_USERNAME", vget("NASA_USERNAME", ""))
    nasa_password = require_value("NASA_PASSWORD", vget("NASA_PASSWORD", ""))

    azure_container_name = vget("SWOT_AZURE_CONTAINER", DEFAULT_AZURE_CONTAINER)
    azure_dawg_blob_prefix = vget("SWOT_DAWG_BLOB_PREFIX", DEFAULT_AZURE_DAWG_BLOB_PREFIX).rstrip("/")
    azure_csv_prefix = vget("SWOT_AZURE_PREFIX", DEFAULT_AZURE_CSV_PREFIX).rstrip("/")
    public_csv_base_url = vget("SWOT_PUBLIC_CSV_BASE_URL", DEFAULT_PUBLIC_CSV_BASE_URL).rstrip("/")

    dawg_short_name = vget("SWOT_DAWG_SHORT_NAME", DEFAULT_DAWG_SHORT_NAME)
    dawg_region_prefix = vget("SWOT_DAWG_REGION_PREFIX", DEFAULT_DAWG_REGION_PREFIX)
    runtime_temp_dir = get_runtime_temp_dir(vget("SWOT_DAWG_DOWNLOAD_TMP_DIR", DEFAULT_DAWG_DOWNLOAD_TMP_DIR))

    geojson_url = vget("SWOT_GEOJSON_URL", DEFAULT_GEOJSON_URL)
    ckan_resource_id = vget("SWOT_CKAN_RESOURCE_ID", DEFAULT_CKAN_RESOURCE_ID)
    geojson_filename = vget("SWOT_GEOJSON_FILENAME", DEFAULT_GEOJSON_FILENAME)

    timeout_s = int(vget("SWOT_TIMEOUT_S", DEFAULT_TIMEOUT_S))
    max_retries = int(vget("SWOT_MAX_RETRIES", DEFAULT_MAX_RETRIES))
    overlap_hours = int(vget("SWOT_OVERLAP_HOURS", DEFAULT_OVERLAP_HOURS))
    backfill_days = int(vget("SWOT_DEFAULT_BACKFILL_DAYS_IF_EMPTY", DEFAULT_BACKFILL_DAYS_IF_EMPTY))

    run_end_dt = datetime.now(timezone.utc)
    result: Dict[str, Any] = {
        "new_dawg_detected": False,
        "current_azure_filename": None,
        "latest_remote_native_id": None,
        "uploaded_blob_name": None,
        "deleted_old_blobs": [],
        "csv_summary": None,
        "geojson_summary": None,
    }

    logger.info("Starting weekly DAWG CSV + GeoJSON update.")
    earthaccess_login_airflow_style(nasa_username, nasa_password)

    container = get_container(azure_storage_connection_string, azure_container_name)

    current_azure_filename = get_current_azure_latest_filename(container, azure_dawg_blob_prefix)
    current_azure_name_no_ext = strip_nc_extension(current_azure_filename)
    result["current_azure_filename"] = current_azure_filename
    logger.info("Current Azure DAWG file: %s", current_azure_filename)

    latest_remote_native_id, latest_remote_granule = get_latest_matching_earthaccess_granule(
        dawg_short_name,
        dawg_region_prefix,
    )
    result["latest_remote_native_id"] = latest_remote_native_id
    logger.info("Latest Earthaccess EU native-id: %s", latest_remote_native_id)

    if current_azure_name_no_ext == latest_remote_native_id:
        logger.info("Azure already has the newest DAWG file. Exiting without CSV or GeoJSON updates.")
        return result

    result["new_dawg_detected"] = True

    local_nc_path = download_granule_to_local(latest_remote_granule, runtime_temp_dir)
    logger.info("Downloaded new DAWG file: %s", local_nc_path)

    target_filename = add_nc_extension(latest_remote_native_id)
    target_blob_name = safe_blob_join(azure_dawg_blob_prefix, target_filename)

    upload_file_to_blob(container, local_nc_path, target_blob_name)
    result["uploaded_blob_name"] = target_blob_name
    logger.info("Uploaded new DAWG blob: %s", target_blob_name)

    deleted_old_blobs = delete_other_nc_blobs_in_prefix(container, azure_dawg_blob_prefix, target_filename)
    result["deleted_old_blobs"] = deleted_old_blobs
    if deleted_old_blobs:
        logger.info("Deleted %d older DAWG blob(s).", len(deleted_old_blobs))

    result["csv_summary"] = update_reach_csvs(
        container=container,
        azure_csv_prefix=azure_csv_prefix,
        geojson_url=geojson_url,
        local_nc_path=local_nc_path,
        run_end_dt=run_end_dt,
        overlap_hours=overlap_hours,
        backfill_days_if_empty=backfill_days,
        timeout_s=timeout_s,
        max_retries=max_retries,
    )
    logger.info("CSV update summary: %s", result["csv_summary"])

    try:
        result["geojson_summary"] = update_geojson_resource(
            geojson_url=geojson_url,
            resource_id=ckan_resource_id,
            geojson_filename=geojson_filename,
            public_csv_base_url=public_csv_base_url,
            ckan_api_key=ckan_api_key,
            timeout_s=timeout_s,
            max_retries=max_retries,
            runtime_temp_dir=runtime_temp_dir,
        )
    except Exception as exc:
        raise RuntimeError(f"CKAN GeoJSON update failed: {format_ckan_error(exc)}") from exc

    logger.info("GeoJSON update summary: %s", result["geojson_summary"])
    logger.info("Weekly DAWG CSV + GeoJSON update completed successfully.")
    return result


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 13),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="weekly_dawg_csv_geojson_update",
    default_args=default_args,
    description="Weekly DAWG NetCDF rotation plus DAWG-only consensus_q CSV and CKAN GeoJSON updates",
    schedule_interval="0 8 * * 1",
    catchup=False,
    max_active_runs=1,
    tags=["swot", "dawg", "azure", "ckan", "unesco", "dnipro", "weekly"],
) as dag:
    run_update = PythonOperator(
        task_id="run_weekly_dawg_csv_geojson_update",
        python_callable=run_weekly_dawg_csv_geojson_update,
    )
