"""Maintain the validated continental DAWG Version 3 cache in Azure."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

import netCDF4 as nc
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContainerClient, ContentSettings

from find_dawg_product import CONTINENTS, discover_dawg_granule, download_with_earthaccess


AZURE_ACCOUNT = "ihpwinsdata"
AZURE_CONTAINER = "swot"
PREFIX = "reference/dawg/v3"
CURRENT_BLOB = "reference/dawg/current.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def load_json(container: ContainerClient, name: str) -> dict[str, Any] | None:
    try:
        return json.loads(container.get_blob_client(name).download_blob().readall())
    except ResourceNotFoundError:
        return None


def upload_json(container: ContainerClient, name: str, value: Any) -> None:
    container.get_blob_client(name).upload_blob(
        json.dumps(value, indent=2, default=str).encode("utf-8"), overwrite=True,
        content_settings=ContentSettings(content_type="application/json; charset=utf-8"),
    )


def sha256_file(path: Path, chunk_size: int = 16 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def validate_dawg(path: Path) -> dict[str, int]:
    if not path.is_file() or path.stat().st_size == 0:
        raise RuntimeError(f"DAWG download is missing or empty: {path}")
    with nc.Dataset(path, "r") as dataset:
        if "reaches" not in dataset.groups or "consensus" not in dataset.groups:
            raise RuntimeError("DAWG file is missing reaches or consensus group")
        reaches = dataset.groups["reaches"].variables
        consensus = dataset.groups["consensus"].variables
        for variable in ("reach_id",):
            if variable not in reaches:
                raise RuntimeError(f"DAWG file is missing reaches/{variable}")
        for variable in ("time_int", "consensus_q"):
            if variable not in consensus:
                raise RuntimeError(f"DAWG file is missing consensus/{variable}")
        reach_count = len(reaches["reach_id"])
        if reach_count == 0 or consensus["consensus_q"].shape[0] != reach_count:
            raise RuntimeError("DAWG reach and consensus dimensions are inconsistent")
        return {"reach_count": reach_count}


def same_release(previous: dict[str, Any] | None, discovered: dict[str, Any]) -> bool:
    return bool(
        previous
        and previous.get("granule_concept_id") == discovered.get("granule_concept_id")
        and previous.get("cmr_updated") == discovered.get("cmr_updated")
        and previous.get("granule_ur") == discovered.get("granule_ur")
    )


def earthdata_login_environment() -> None:
    # Accept both the established project names and earthaccess's standard names.
    username = runtime_secret("EARTHDATA_USERNAME") or runtime_secret("NASA_USERNAME")
    password = runtime_secret("EARTHDATA_PASSWORD") or runtime_secret("NASA_PASSWORD")
    if not username or not password:
        raise RuntimeError("EARTHDATA_USERNAME/PASSWORD (or NASA_USERNAME/PASSWORD) are required")
    os.environ["EARTHDATA_USERNAME"] = username
    os.environ["EARTHDATA_PASSWORD"] = password


def upload_source(
    container: ContainerClient, path: Path, continent: str, metadata: dict[str, Any]
) -> dict[str, Any]:
    blob = str(PurePosixPath(PREFIX, continent.lower(), path.name))
    digest = sha256_file(path)
    client = container.get_blob_client(blob)
    try:
        properties = client.get_blob_properties()
    except ResourceNotFoundError:
        properties = None
    if properties is not None:
        if properties.size != path.stat().st_size or (properties.metadata or {}).get("sha256") != digest:
            raise RuntimeError(f"Existing immutable DAWG blob differs: {blob}")
    else:
        with path.open("rb") as stream:
            client.upload_blob(
                stream, length=path.stat().st_size, overwrite=False, max_concurrency=4,
                metadata={"sha256": digest, "continent": continent.lower(), "collection_version": "3"},
                content_settings=ContentSettings(content_type="application/x-netcdf"),
            )
    metadata_blob = str(PurePosixPath(PREFIX, continent.lower(), path.stem + ".json"))
    record = {
        **metadata, "source_blob": blob,
        "source_url": f"https://{AZURE_ACCOUNT}.blob.core.windows.net/{AZURE_CONTAINER}/{blob}",
        "sha256": digest, "size_bytes": path.stat().st_size,
    }
    upload_json(container, metadata_blob, record)
    record["metadata_blob"] = metadata_blob
    return record


def update_dawg_reference(
    *, continents: list[str] | None = None, timeout: int = 60,
    connection_string_env: str = "AZURE_STORAGE_CONNECTION_STRING",
) -> dict[str, Any]:
    """Check, validate and atomically publish all requested DAWG releases."""
    selected = [item.upper() for item in (continents or sorted(CONTINENTS))]
    invalid = sorted(set(selected) - CONTINENTS)
    if invalid:
        raise ValueError(f"Invalid continents: {invalid}")
    container = get_container(connection_string_env)
    current = load_json(container, CURRENT_BLOB) or {"schema_version": 1, "continents": {}}
    previous = current.get("continents") or {}
    discovered = {continent: discover_dawg_granule(continent, timeout) for continent in selected}
    changed = [continent for continent in selected if not same_release(previous.get(continent), discovered[continent])]
    next_continents = dict(previous)
    if changed:
        earthdata_login_environment()
        with tempfile.TemporaryDirectory(prefix="swot_dawg_") as temporary:
            root = Path(temporary)
            for continent in changed:
                path = download_with_earthaccess(discovered[continent], root / continent.lower(), "environment")
                try:
                    validation = validate_dawg(path)
                    next_continents[continent] = {
                        **upload_source(container, path, continent, discovered[continent]),
                        "validation": validation, "validated_utc": utc_now(),
                    }
                finally:
                    # Continental products are multi-GB. Keep peak temporary
                    # disk usage to one granule rather than retaining all six.
                    path.unlink(missing_ok=True)
    result = {
        "schema_version": 1, "collection": "SWOT_L4_HR_DAWG_SOS_DISCHARGE_V3",
        "collection_version": "3", "dawg_sword_version": "v16",
        "updated_utc": utc_now(), "continents": next_continents,
    }
    # Atomic pointer: written only after every changed granule validates/uploads.
    if changed or not load_json(container, CURRENT_BLOB):
        upload_json(container, CURRENT_BLOB, result)
    summary = {"checked_continents": selected, "changed_continents": changed,
               "unchanged_continents": [item for item in selected if item not in changed],
               "current_blob": CURRENT_BLOB, "updated_utc": result["updated_utc"]}
    upload_json(container, "reference/dawg/logs/latest.json", summary)
    return summary
