"""Provision uploaded AOIs into regional SWOT products.

Submission descriptors live at ``incoming/pending/*.submission.json`` in the
``swot`` container.  Each descriptor names one AOI blob and supplies the
stable region ID and display name.  Successful and failed submissions are
archived with a durable receipt; no empty Azure folders need to be created.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

import geopandas as gpd
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import ContainerClient, ContentSettings

from inspect_sword_aoi import DEFAULT_REFERENCE_ROOT, select_layer
from publish_swot_region import DATASET, publish_region
from swot_historical_aoi import DEFAULT_START, run_historical_aoi_pipeline, validate_region_id

AZURE_ACCOUNT = "ihpwinsdata"
AZURE_CONTAINER = "swot"
PENDING_PREFIX = "incoming/pending/"
PROCESSING_PREFIX = "incoming/processing/"
COMPLETED_PREFIX = "incoming/completed/"
FAILED_PREFIX = "incoming/failed/"
CONTINENTS = ("AF", "AS", "EU", "NA", "OC", "SA")
SUPPORTED_SUFFIXES = {".geojson", ".json", ".gpkg", ".kml", ".zip"}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_container(connection_string_env: str = "AZURE_STORAGE_CONNECTION_STRING") -> ContainerClient:
    connection = os.environ.get(connection_string_env, "").strip()
    if not connection: raise ValueError(f"Environment variable {connection_string_env!r} is empty")
    container = ContainerClient.from_connection_string(connection, AZURE_CONTAINER)
    if container.account_name != AZURE_ACCOUNT: raise ValueError(f"Refusing unexpected Azure account {container.account_name!r}")
    container.get_container_properties()
    return container


def safe_blob(name: str, required_prefix: str | None = None) -> str:
    normalized = str(PurePosixPath(name.replace("\\", "/")))
    if normalized.startswith("/") or ".." in PurePosixPath(normalized).parts:
        raise ValueError(f"Unsafe blob path: {name}")
    if required_prefix and not normalized.startswith(required_prefix):
        raise ValueError(f"Blob must be below {required_prefix}: {name}")
    return normalized


def submission_id(metadata_blob: str) -> str:
    name = PurePosixPath(metadata_blob).name
    if not name.endswith(".submission.json"): raise ValueError("Submission metadata must end in .submission.json")
    value = name[:-len(".submission.json")]
    if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", value):
        raise ValueError("Submission filename must use lowercase letters, numbers, and hyphens")
    return value


def validate_submission(payload: dict[str, Any], metadata_blob: str) -> dict[str, Any]:
    required = [key for key in ("region_id", "display_name", "aoi_blob") if not str(payload.get(key, "")).strip()]
    if required: raise ValueError(f"Submission is missing: {', '.join(required)}")
    result = dict(payload)
    result["submission_id"] = submission_id(metadata_blob)
    result["region_id"] = validate_region_id(str(payload["region_id"]))
    result["display_name"] = str(payload["display_name"]).strip()
    result["aoi_blob"] = safe_blob(str(payload["aoi_blob"]), PENDING_PREFIX)
    suffix = Path(result["aoi_blob"]).suffix.lower()
    if suffix not in SUPPORTED_SUFFIXES: raise ValueError(f"Unsupported AOI type {suffix}; use GeoJSON, GeoPackage, KML, or zipped shapefile")
    continent = str(payload.get("continent") or "AUTO").strip().upper()
    if continent != "AUTO" and continent not in CONTINENTS: raise ValueError(f"Unknown continent {continent}")
    result["continent"] = continent
    result["dataset_id"] = str(payload.get("dataset_id") or DATASET).strip()
    result["historical_start"] = str(payload.get("historical_start") or DEFAULT_START).strip()
    return result


def discover_submissions(connection_string_env: str = "AZURE_STORAGE_CONNECTION_STRING",
                         submission_filter: str | None = None) -> list[dict[str, str]]:
    container = get_container(connection_string_env)
    allowed = {item.strip() for item in (submission_filter or "").split(",") if item.strip()}
    found = []
    for blob in container.list_blobs(name_starts_with=PENDING_PREFIX):
        if not blob.name.endswith(".submission.json"): continue
        sid = submission_id(blob.name)
        if allowed and sid not in allowed: continue
        found.append({"submission_id": sid, "metadata_blob": blob.name})
    return sorted(found, key=lambda item: item["submission_id"])


def _download(container: ContainerClient, blob: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as handle:
        container.get_blob_client(blob).download_blob().readinto(handle)


def _upload_json(container: ContainerClient, blob: str, payload: dict[str, Any], overwrite: bool = True) -> None:
    container.get_blob_client(blob).upload_blob(
        json.dumps(payload, ensure_ascii=False, indent=2).encode(), overwrite=overwrite,
        content_settings=ContentSettings(content_type="application/json; charset=utf-8"))


def _copy_bytes(container: ContainerClient, source: str, destination: str) -> None:
    data = container.get_blob_client(source).download_blob().readall()
    container.get_blob_client(destination).upload_blob(data, overwrite=True)


def _extract_aoi(downloaded: Path, directory: Path) -> Path:
    if downloaded.suffix.lower() != ".zip": return downloaded
    extract_root = directory / "unzipped"; extract_root.mkdir()
    with zipfile.ZipFile(downloaded) as archive:
        for member in archive.infolist():
            target = (extract_root / member.filename).resolve()
            if extract_root.resolve() not in target.parents and target != extract_root.resolve():
                raise ValueError(f"Unsafe ZIP member: {member.filename}")
        archive.extractall(extract_root)
    shapefiles = list(extract_root.rglob("*.shp"))
    if len(shapefiles) != 1: raise ValueError(f"A zipped shapefile must contain exactly one .shp; found {len(shapefiles)}")
    stem = shapefiles[0].with_suffix("")
    missing = [suffix for suffix in (".shx", ".dbf", ".prj") if not stem.with_suffix(suffix).exists()]
    if missing: raise ValueError(f"Zipped shapefile is missing required sidecars: {', '.join(missing)}")
    return shapefiles[0]


def detect_continent(aoi_path: Path) -> tuple[str, dict[str, int]]:
    frame = gpd.read_file(aoi_path)
    if frame.empty or frame.crs is None: raise ValueError("AOI must contain geometry and have a defined CRS")
    frame = frame.to_crs("EPSG:4326")
    mask = frame.geometry.union_all()
    counts = {}
    for continent in CONTINENTS:
        counts[continent] = len(select_layer(mask, f"{DEFAULT_REFERENCE_ROOT}/reaches/{continent}.fgb"))
    selected = max(counts, key=counts.get)
    if counts[selected] == 0: raise ValueError("AOI does not intersect any SWORD v17b reaches")
    return selected, counts


def _archive(container: ContainerClient, submission: dict[str, Any], outcome: str,
             receipt: dict[str, Any]) -> None:
    base = (COMPLETED_PREFIX if outcome == "completed" else FAILED_PREFIX) + submission["submission_id"] + "/"
    for source in (submission["metadata_blob"], submission["aoi_blob"]):
        _copy_bytes(container, source, base + PurePosixPath(source).name)
    _upload_json(container, base + "receipt.json", receipt)
    # Archive copies and receipt exist before pending inputs are removed.
    for source in (submission["metadata_blob"], submission["aoi_blob"]):
        container.get_blob_client(source).delete_blob()


def provision_submission(*, descriptor: dict[str, str], workers: int = 8, timeout: int = 60,
                         retries: int = 5, connection_string_env: str = "AZURE_STORAGE_CONNECTION_STRING",
                         ckan_api_key: str | None = None) -> dict[str, Any]:
    container = get_container(connection_string_env)
    metadata_blob = safe_blob(descriptor["metadata_blob"], PENDING_PREFIX)
    payload = json.loads(container.get_blob_client(metadata_blob).download_blob().readall())
    payload["metadata_blob"] = metadata_blob
    submission = validate_submission(payload, metadata_blob)
    marker = PROCESSING_PREFIX + submission["submission_id"] + ".json"
    try:
        _upload_json(container, marker, {"submission_id": submission["submission_id"], "region_id": submission["region_id"], "started_utc": utc_now()}, overwrite=False)
    except ResourceExistsError:
        # max_active_runs=1 prevents concurrent claims; replacing a stale marker
        # makes manual task retries safe after an interrupted worker.
        _upload_json(container, marker, {"submission_id": submission["submission_id"], "region_id": submission["region_id"], "restarted_utc": utc_now()})
    try:
        with tempfile.TemporaryDirectory(prefix=f"swot-{submission['submission_id']}-") as temp:
            temp_path = Path(temp)
            downloaded = temp_path / PurePosixPath(submission["aoi_blob"]).name
            _download(container, submission["aoi_blob"], downloaded)
            aoi_path = _extract_aoi(downloaded, temp_path)
            continent_counts = None
            continent = submission["continent"]
            if continent == "AUTO": continent, continent_counts = detect_continent(aoi_path)
            output_root = temp_path / "output"
            historical = run_historical_aoi_pipeline(
                aoi=str(aoi_path), region_id=submission["region_id"], display_name=submission["display_name"],
                continent=continent, output_root=str(output_root), start=submission["historical_start"],
                workers=workers, timeout=timeout, retries=retries, upload=True,
                overwrite_azure=True, register_manifest=False,
                connection_string_env=connection_string_env)
            root = output_root / submission["region_id"] / "historical"
            publication = publish_region(root, submission["display_name"], submission["region_id"],
                                         submission["dataset_id"], ckan_api_key=ckan_api_key)
            receipt = {"schema_version": 1, "status": "completed", "submission_id": submission["submission_id"],
                       "region_id": submission["region_id"], "completed_utc": utc_now(), "continent": continent,
                       "continent_reach_counts": continent_counts, "historical": historical, "publication": publication}
            _archive(container, submission, "completed", receipt)
            container.get_blob_client(marker).delete_blob()
            return receipt
    except Exception as exc:
        receipt = {"schema_version": 1, "status": "failed", "submission_id": submission["submission_id"],
                   "region_id": submission["region_id"], "failed_utc": utc_now(),
                   "error_type": type(exc).__name__, "error": str(exc)[:4000]}
        _archive(container, submission, "failed", receipt)
        container.get_blob_client(marker).delete_blob()
        raise

