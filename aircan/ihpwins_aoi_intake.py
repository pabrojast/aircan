"""IHP-WINS AOI intake adapter for the Azure provisioning inbox.

This module discovers GeoJSON resources in the dedicated IHP-WINS intake
dataset, validates polygon geometry, assigns a stable regional name, and writes
the two files expected by the existing Azure provisioning inbox. Destination
datasets are created later by the provisioning worker.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import unicodedata
import urllib.parse
import urllib.request
import urllib.error
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

from shapely.geometry import shape
from azure.storage.blob import ContainerClient, ContentSettings


CKAN_BASE = "https://ihp-wins.unesco.org"
INTAKE_DATASET_ID = "17bed9a4-fa7d-401d-8656-b1a5847bbb5e"
AZURE_CONTAINER = "swot"
DEFAULT_HISTORICAL_START = "2023-03-30T00:00:00Z"
DATASET_TITLE_PREFIX = "SWOT-SWORD Surface Water Observations — "
GUIDE_RESOURCE = {
    "id": "9c6f93ce-3f39-4a4e-b3cd-342c273f2522",
    "name": "SWOT-SWORD Regional Surface Water Data Guide",
    "url": f"{CKAN_BASE}/dataset/{INTAKE_DATASET_ID}/resource/9c6f93ce-3f39-4a4e-b3cd-342c273f2522/download/swot_sword_regional_data_guide.pdf",
    "format": "PDF",
}


@dataclass(frozen=True)
class IntakeResource:
    id: str
    name: str
    url: str
    format: str
    last_modified: str | None = None


@dataclass(frozen=True)
class IntakePreview:
    source_dataset_id: str
    source_resource_id: str
    source_resource_name: str
    source_url: str
    source_sha256: str
    display_name: str
    region_id: str
    destination_dataset_title: str
    destination_dataset_name: str
    aoi_blob: str
    submission_blob: str
    feature_count: int
    geometry_types: list[str]


def ckan_action(action: str, payload: dict[str, Any]) -> dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    api_key = os.getenv("CKAN_API_KEY")
    if api_key:
        headers["Authorization"] = api_key
    request = urllib.request.Request(
        f"{CKAN_BASE}/api/3/action/{action}",
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        result = json.load(response)
    if not result.get("success"):
        raise RuntimeError(f"CKAN {action} failed: {result}")
    return result["result"]


def discover_resources(dataset_id: str = INTAKE_DATASET_ID) -> list[IntakeResource]:
    package = ckan_action("package_show", {"id": dataset_id})
    resource_items = package.get("resources") or []
    # IHP-WINS can briefly return a stale package_show snapshot after its custom
    # upload workflow completes. The newest package activity contains the
    # committed resource list, so use that as a read-only fallback.
    if not resource_items and int(package.get("num_resources") or 0) == 0:
        activities = ckan_action("package_activity_list", {"id": dataset_id, "limit": 100})
        by_resource_id: dict[str, dict[str, Any]] = {}
        for activity in activities:
            activity_package = (activity.get("data") or {}).get("package") or {}
            activity_resources = activity_package.get("resources") or []
            for item in activity_resources:
                resource_id = str(item.get("id") or "")
                # Activities are newest first. Keep the newest representation
                # of each resource, but merge IDs across snapshots because the
                # IHP-WINS custom upload workflow can expose only the newest
                # resource in any single snapshot.
                if resource_id and resource_id not in by_resource_id:
                    by_resource_id[resource_id] = item
        resource_items = list(by_resource_id.values())
    resources = []
    for item in resource_items:
        resource_format = str(item.get("format") or "").strip().lower()
        path = urllib.parse.urlsplit(str(item.get("url") or "")).path.lower()
        if resource_format not in {"geojson", "json"} and not path.endswith(
            (".geojson", ".json")
        ):
            continue
        resources.append(
            IntakeResource(
                id=str(item["id"]),
                name=str(item.get("name") or "").strip(),
                url=str(item.get("url") or "").strip(),
                format=str(item.get("format") or "GeoJSON"),
                last_modified=item.get("last_modified"),
            )
        )
    return sorted(resources, key=lambda item: (item.name.casefold(), item.id))


def existing_destination_dataset_names() -> set[str]:
    result = ckan_action(
        "package_search",
        {"q": "name:swot-sword-surface-water-observations-*", "rows": 1000},
    )
    return {str(item.get("name") or "").casefold() for item in result.get("results", [])}


def download_resource(resource: IntakeResource) -> bytes:
    if not resource.url:
        raise ValueError(f"Resource {resource.id} has no download URL")
    resource_url = urllib.parse.urljoin(CKAN_BASE, resource.url)
    headers = {"User-Agent": "UNESCO-SWOT-AOI-Intake/1.0"}
    api_key = os.getenv("CKAN_API_KEY")
    request = urllib.request.Request(resource_url, headers=headers)
    if api_key:
        # Send the CKAN token to IHP-WINS, but do not copy it onto the Azure
        # download request created by CKAN's redirect.
        request.add_unredirected_header("Authorization", api_key)
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            data = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 403:
            raise RuntimeError(
                f"IHP-WINS denied download of resource {resource.id}. "
                "Set CKAN_API_KEY to a current token that can read the intake dataset."
            ) from exc
        raise
    if not data:
        raise ValueError(f"Resource {resource.id} downloaded as an empty file")
    return data


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii").lower()
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_text).strip("-")
    if not slug:
        raise ValueError("The resource name must contain at least one letter or number")
    return slug


def choose_unique_name(
    requested_name: str,
    *,
    taken_region_ids: Iterable[str] = (),
    taken_dataset_names: Iterable[str] = (),
) -> tuple[str, str, str]:
    requested_name = " ".join(str(requested_name).split())
    if not requested_name:
        raise ValueError("The GeoJSON resource must have a region name")
    regions = {str(value).casefold() for value in taken_region_ids}
    datasets = {str(value).casefold() for value in taken_dataset_names}
    number = 1
    while True:
        display_name = requested_name if number == 1 else f"{requested_name} ({number})"
        region_id = slugify(display_name)
        dataset_name = f"swot-sword-surface-water-observations-{region_id}"
        if region_id.casefold() not in regions and dataset_name.casefold() not in datasets:
            return display_name, region_id, dataset_name
        number += 1


def validate_geojson(data: bytes) -> tuple[dict[str, Any], int, list[str]]:
    try:
        payload = json.loads(data.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"The resource is not valid UTF-8 GeoJSON: {exc}") from exc
    if payload.get("type") != "FeatureCollection":
        raise ValueError("The GeoJSON root must be a FeatureCollection")
    features = payload.get("features")
    if not isinstance(features, list) or not features:
        raise ValueError("The GeoJSON must contain at least one feature")
    geometry_types: set[str] = set()
    for index, feature in enumerate(features):
        if not isinstance(feature, dict) or feature.get("type") != "Feature":
            raise ValueError(f"Feature {index} is not a valid GeoJSON Feature")
        geometry_payload = feature.get("geometry")
        if not geometry_payload:
            raise ValueError(f"Feature {index} has no geometry")
        geometry = shape(geometry_payload)
        if geometry.geom_type not in {"Polygon", "MultiPolygon"}:
            raise ValueError(
                f"Feature {index} is {geometry.geom_type}; AOIs must use Polygon or MultiPolygon"
            )
        if geometry.is_empty:
            raise ValueError(f"Feature {index} has empty geometry")
        if not geometry.is_valid:
            raise ValueError(f"Feature {index} has invalid polygon geometry")
        min_x, min_y, max_x, max_y = geometry.bounds
        if min_x < -180 or max_x > 180 or min_y < -90 or max_y > 90:
            raise ValueError(
                "GeoJSON coordinates fall outside longitude/latitude bounds; use WGS84 (EPSG:4326)"
            )
        geometry_types.add(geometry.geom_type)
    return payload, len(features), sorted(geometry_types)


def build_preview(
    resource: IntakeResource,
    data: bytes,
    *,
    dataset_id: str = INTAKE_DATASET_ID,
    taken_region_ids: Iterable[str] = (),
    taken_dataset_names: Iterable[str] = (),
    assigned_identity: dict[str, str] | None = None,
) -> tuple[IntakePreview, dict[str, Any], dict[str, Any]]:
    geojson, feature_count, geometry_types = validate_geojson(data)
    if assigned_identity:
        display_name = str(assigned_identity['display_name'])
        region_id = str(assigned_identity['region_id'])
        dataset_name = str(assigned_identity['destination_dataset_name'])
    else:
        display_name, region_id, dataset_name = choose_unique_name(
            resource.name,
            taken_region_ids=taken_region_ids,
            taken_dataset_names=taken_dataset_names,
        )
    aoi_blob = f"incoming/pending/{region_id}.geojson"
    submission_blob = f"incoming/pending/{region_id}.submission.json"
    source_sha256 = hashlib.sha256(data).hexdigest()
    preview = IntakePreview(
        source_dataset_id=dataset_id,
        source_resource_id=resource.id,
        source_resource_name=resource.name,
        source_url=resource.url,
        source_sha256=source_sha256,
        display_name=display_name,
        region_id=region_id,
        destination_dataset_title=DATASET_TITLE_PREFIX + display_name,
        destination_dataset_name=dataset_name,
        aoi_blob=aoi_blob,
        submission_blob=submission_blob,
        feature_count=feature_count,
        geometry_types=geometry_types,
    )
    submission = {
        "schema_version": 2,
        "region_id": region_id,
        "display_name": display_name,
        "aoi_blob": aoi_blob,
        "continent": "AUTO",
        "historical_start": DEFAULT_HISTORICAL_START,
        "destination_dataset_title": preview.destination_dataset_title,
        "destination_dataset_name": dataset_name,
        "source_intake": {
            "dataset_id": dataset_id,
            "resource_id": resource.id,
            "resource_name": resource.name,
            "resource_url": resource.url,
            "sha256": source_sha256,
            "last_modified": resource.last_modified,
        },
        "documentation_source": dict(GUIDE_RESOURCE),
    }
    return preview, geojson, submission


def write_preview(
    preview: IntakePreview,
    geojson: dict[str, Any],
    submission: dict[str, Any],
    output_root: Path,
) -> Path:
    destination = output_root / preview.source_resource_id
    destination.mkdir(parents=True, exist_ok=True)
    (destination / f"{preview.region_id}.geojson").write_text(
        json.dumps(geojson, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    (destination / f"{preview.region_id}.submission.json").write_text(
        json.dumps(submission, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (destination / "preview.json").write_text(
        json.dumps(asdict(preview), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return destination


def enqueue_preview(
    preview: IntakePreview,
    geojson: dict[str, Any],
    submission: dict[str, Any],
    *,
    connection_string_env: str = "AZURE_STORAGE_CONNECTION_STRING",
) -> dict[str, str]:
    """Atomically expose a validated AOI to provisioning (submission is last)."""
    connection = os.getenv(connection_string_env, "").strip()
    if not connection:
        raise RuntimeError(f"{connection_string_env} is not set")
    container = ContainerClient.from_connection_string(connection, AZURE_CONTAINER)
    if container.account_name != "ihpwinsdata":
        raise ValueError(f"Refusing unexpected Azure account {container.account_name!r}")
    aoi_bytes = json.dumps(geojson, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    submission_bytes = (json.dumps(submission, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    aoi_client = container.get_blob_client(preview.aoi_blob)
    submission_client = container.get_blob_client(preview.submission_blob)
    if aoi_client.exists() or submission_client.exists():
        raise RuntimeError(
            f"Refusing to overwrite an existing pending submission for {preview.region_id}"
        )
    aoi_client.upload_blob(
        aoi_bytes,
        overwrite=False,
        content_settings=ContentSettings(content_type="application/geo+json; charset=utf-8"),
        metadata={"source_resource_id": preview.source_resource_id, "sha256": preview.source_sha256},
    )
    try:
        submission_client.upload_blob(
            submission_bytes,
            overwrite=False,
            content_settings=ContentSettings(content_type="application/json; charset=utf-8"),
            metadata={"source_resource_id": preview.source_resource_id, "sha256": preview.source_sha256},
        )
    except Exception:
        aoi_client.delete_blob()
        raise
    return {"aoi_blob": preview.aoi_blob, "submission_blob": preview.submission_blob}


def load_intake_registry(
    *, connection_string_env: str = "AZURE_STORAGE_CONNECTION_STRING",
) -> dict[str, dict[str, Any]]:
    """Return stable source-resource assignments recorded after provisioning."""
    connection = os.getenv(connection_string_env, "").strip()
    if not connection:
        return {}
    container = ContainerClient.from_connection_string(connection, AZURE_CONTAINER)
    registry: dict[str, dict[str, Any]] = {}
    for blob in container.list_blobs(name_starts_with="incoming/registry/"):
        if not blob.name.endswith(".json"):
            continue
        item = json.loads(container.get_blob_client(blob.name).download_blob().readall())
        source_id = str(item.get("source_resource_id") or "")
        if source_id:
            registry[source_id] = item
    # Backfill assignments created before the registry feature was deployed.
    # The archived submission is authoritative and preserves the source UUID.
    for blob in container.list_blobs(name_starts_with="incoming/completed/"):
        if not blob.name.endswith(".submission.json"):
            continue
        submission = json.loads(container.get_blob_client(blob.name).download_blob().readall())
        source = submission.get("source_intake") or {}
        source_id = str(source.get("resource_id") or "")
        if source_id and source_id not in registry:
            registry[source_id] = {
                "source_dataset_id": source.get("dataset_id"),
                "source_resource_id": source_id,
                "source_sha256": source.get("sha256"),
                "display_name": submission.get("display_name"),
                "region_id": submission.get("region_id"),
                "destination_dataset_name": submission.get("destination_dataset_name"),
                "status": "completed_archive",
            }
    # Pending sources must also be reserved so a repeated poll cannot create a
    # second suffix while the first submission is waiting or running.
    for blob in container.list_blobs(name_starts_with="incoming/pending/"):
        if not blob.name.endswith(".submission.json"):
            continue
        submission = json.loads(container.get_blob_client(blob.name).download_blob().readall())
        source = submission.get("source_intake") or {}
        source_id = str(source.get("resource_id") or "")
        if source_id and source_id not in registry:
            registry[source_id] = {
                "source_dataset_id": source.get("dataset_id"),
                "source_resource_id": source_id,
                "source_sha256": source.get("sha256"),
                "display_name": submission.get("display_name"),
                "region_id": submission.get("region_id"),
                "destination_dataset_name": submission.get("destination_dataset_name"),
                "status": "pending",
            }
    return registry


def enqueue_live_dataset(
    *, dataset_id: str = INTAKE_DATASET_ID,
    output_root: Path = Path("output/intake_preview"),
) -> dict[str, Any]:
    """Poll IHP-WINS once and enqueue only new or revised source resources."""
    registry = load_intake_registry()
    taken_dataset_names = existing_destination_dataset_names()
    prefix = "swot-sword-surface-water-observations-"
    taken_region_ids = {
        name[len(prefix):] for name in taken_dataset_names if name.startswith(prefix)
    }
    queued: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    for resource in discover_resources(dataset_id):
        data = download_resource(resource)
        source_hash = hashlib.sha256(data).hexdigest()
        prior = registry.get(resource.id)
        if prior and (prior.get("source_sha256") == source_hash or prior.get("status") == "pending"):
            skipped.append({"resource_id": resource.id, "reason": "unchanged" if prior.get("source_sha256") == source_hash else "revision_waiting_for_pending_run"})
            continue
        preview, geojson, submission = build_preview(
            resource,
            data,
            dataset_id=dataset_id,
            taken_region_ids=taken_region_ids,
            taken_dataset_names=taken_dataset_names,
            assigned_identity=prior,
        )
        write_preview(preview, geojson, submission, output_root)
        enqueue_preview(preview, geojson, submission)
        queued.append(asdict(preview))
        taken_region_ids.add(preview.region_id)
        taken_dataset_names.add(preview.destination_dataset_name)
    return {"queued_count": len(queued), "skipped_count": len(skipped), "queued": queued, "skipped": skipped}


def preview_live_dataset(
    *,
    dataset_id: str = INTAKE_DATASET_ID,
    output_root: Path = Path("output/intake_preview"),
) -> list[IntakePreview]:
    previews = []
    taken_dataset_names = existing_destination_dataset_names()
    prefix = "swot-sword-surface-water-observations-"
    taken_region_ids = {
        name[len(prefix):] for name in taken_dataset_names if name.startswith(prefix)
    }
    for resource in discover_resources(dataset_id):
        data = download_resource(resource)
        preview, geojson, submission = build_preview(
            resource,
            data,
            dataset_id=dataset_id,
            taken_region_ids=taken_region_ids,
            taken_dataset_names=taken_dataset_names,
        )
        write_preview(preview, geojson, submission, output_root)
        previews.append(preview)
        taken_region_ids.add(preview.region_id)
        taken_dataset_names.add(preview.destination_dataset_name)
    return previews


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-id", default=INTAKE_DATASET_ID)
    parser.add_argument("--output-dir", type=Path, default=Path("output/intake_preview"))
    parser.add_argument("--local-geojson", type=Path)
    parser.add_argument("--resource-name")
    parser.add_argument("--enqueue", action="store_true", help="Upload validated files to Azure pending inbox")
    args = parser.parse_args()
    if args.local_geojson:
        if not args.resource_name:
            parser.error("--resource-name is required with --local-geojson")
        data = args.local_geojson.read_bytes()
        resource = IntakeResource(
            id="local-test-resource",
            name=args.resource_name,
            url=args.local_geojson.resolve().as_uri(),
            format="GeoJSON",
        )
        preview, geojson, submission = build_preview(resource, data, dataset_id=args.dataset_id)
        destination = write_preview(preview, geojson, submission, args.output_dir)
        print(json.dumps({**asdict(preview), "output_directory": str(destination)}, indent=2))
        return 0
    resources = discover_resources(args.dataset_id)
    previews = []
    taken_dataset_names = existing_destination_dataset_names()
    prefix = "swot-sword-surface-water-observations-"
    taken_region_ids = {
        name[len(prefix):] for name in taken_dataset_names if name.startswith(prefix)
    }
    registry = load_intake_registry() if args.enqueue else {}
    for resource in resources:
        data = download_resource(resource)
        source_hash = hashlib.sha256(data).hexdigest()
        prior = registry.get(resource.id)
        if prior and prior.get("source_sha256") == source_hash:
            continue
        preview, geojson, submission = build_preview(
            resource,
            data,
            dataset_id=args.dataset_id,
            taken_region_ids=taken_region_ids,
            taken_dataset_names=taken_dataset_names,
            assigned_identity=prior,
        )
        write_preview(preview, geojson, submission, args.output_dir)
        if args.enqueue:
            enqueue_preview(preview, geojson, submission)
        previews.append(preview)
        taken_region_ids.add(preview.region_id)
        taken_dataset_names.add(preview.destination_dataset_name)
    print(json.dumps([asdict(item) for item in previews], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
