"""Standalone Airflow worker for AOI-to-SWOT regional provisioning.

This deployment module intentionally contains AOI selection, node filtering,
historical ingestion, CKAN/Terria publication, and inbox orchestration so the
Airflow deployment needs only this file and its thin DAG wrapper.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import random
import re
import tempfile
import threading
import time
import zipfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable
from urllib.parse import quote_plus

import geopandas as gpd
import pandas as pd
import requests
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import ContainerClient, ContentSettings
from pandas.errors import EmptyDataError


# ---- embedded pipeline implementation ----

REQUIRED_FILTER_FIELDS = ('time_str', 'wse', 'ice_clim_f', 'node_q', 'node_q_b', 'xovr_cal_q')

OUTPUT_COLUMNS = ['node_id', 'reach_id', 'time_utc', 'lat', 'lon', 'river_name', 'wse', 'wse_u', 'wse_r_u', 'wse_units', 'wse_u_units', 'wse_r_u_units', 'width', 'width_u', 'width_units', 'width_u_units', 'node_q', 'node_q_b', 'ice_clim_f', 'xovr_cal_q', 'cycle_id', 'pass_id', 'crid', 'sword_version', 'collection_shortname', 'collection_version', 'granuleUR']

REJECT_NODE_Q_BITS = {13: 'far_range_suspect', 14: 'near_range_suspect', 19: 'geolocation_qual_degraded', 23: 'wse_outlier'}

FILL_ABS_THRESHOLD = 10000000000.0

def clean_identifier(value: Any) -> str:
    text = str(value).strip()
    if text.endswith('.0'):
        text = text[:-2]
    return text

def _numeric(frame: pd.DataFrame, field: str) -> pd.Series:
    return pd.to_numeric(frame[field], errors='coerce')

def apply_quality_filter(frame: pd.DataFrame) -> tuple[pd.DataFrame, Counter[str]]:
    """Apply required node filters and return accepted rows plus audit counts.

    Rejection counters overlap by design: a row with two problems is counted in
    both categories.  This makes each rule independently auditable.
    """
    if frame.empty:
        return (frame.copy(), Counter())
    if 'xovr_cal_q' not in frame.columns and 'xover_cal_q' in frame.columns:
        frame = frame.rename(columns={'xover_cal_q': 'xovr_cal_q'})
    missing = [field for field in REQUIRED_FILTER_FIELDS if field not in frame.columns]
    if missing:
        raise ValueError(f'Hydrocron response is missing required filter fields: {missing}')
    ice = _numeric(frame, 'ice_clim_f')
    node_q = _numeric(frame, 'node_q')
    crossover = _numeric(frame, 'xovr_cal_q')
    wse = _numeric(frame, 'wse')
    node_q_b_numeric = _numeric(frame, 'node_q_b')
    parsed_time = pd.to_datetime(frame['time_str'], format='mixed', errors='coerce', utc=True)
    masks: dict[str, pd.Series] = {'ice': ice.eq(0), 'node_q': node_q.lt(3), 'xovr_cal_q': crossover.lt(2), 'wse': wse.notna() & wse.abs().lt(FILL_ABS_THRESHOLD), 'time': parsed_time.notna(), 'node_q_b_valid': node_q_b_numeric.notna() & node_q_b_numeric.ge(0) & node_q_b_numeric.map(lambda value: float(value).is_integer() if pd.notna(value) else False)}
    bit_values = node_q_b_numeric.fillna(-1).astype('int64')
    for bit in REJECT_NODE_Q_BITS:
        masks[f'bit_{bit}'] = masks['node_q_b_valid'] & (bit_values & 1 << bit == 0)
    accepted = pd.Series(True, index=frame.index)
    for mask in masks.values():
        accepted &= mask
    counts: Counter[str] = Counter()
    counts['reject_ice'] = int((~masks['ice']).sum())
    counts['reject_node_q'] = int((~masks['node_q']).sum())
    counts['reject_xovr_cal_q'] = int((~masks['xovr_cal_q']).sum())
    counts['reject_wse'] = int((~masks['wse']).sum())
    counts['reject_time'] = int((~masks['time']).sum())
    for bit in REJECT_NODE_Q_BITS:
        counts[f'reject_bit_{bit}'] = int((~masks[f'bit_{bit}']).sum())
    output = frame.loc[accepted].copy()
    output['time_utc'] = parsed_time.loc[output.index]
    output['time_utc'] = output['time_utc'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    for field in ('node_id', 'reach_id'):
        if field in output.columns:
            output[field] = output[field].map(clean_identifier)
    for column in OUTPUT_COLUMNS:
        if column not in output.columns:
            output[column] = pd.NA
    output = output[OUTPUT_COLUMNS].drop_duplicates(subset=['node_id', 'time_utc', 'cycle_id', 'pass_id'], keep='last')
    output = output.sort_values('time_utc').reset_index(drop=True)
    return (output, counts)

DEFAULT_REFERENCE_ROOT = 'https://ihpwinsdata.blob.core.windows.net/swot/reference/sword/v17b'

def select_layer(aoi_geometry, url: str) -> gpd.GeoDataFrame:
    candidates = gpd.read_file(url, bbox=tuple(aoi_geometry.bounds))
    if candidates.empty:
        return candidates
    candidates = candidates.to_crs('EPSG:4326')
    return candidates.loc[candidates.geometry.intersects(aoi_geometry)].copy()

def select_global_layer(aoi_geometry, kind: str) -> tuple[gpd.GeoDataFrame, dict[str, int]]:
    """Select an AOI from the complete partitioned SWORD reference.

    The Azure reference is global but intentionally stored as continental
    FlatGeobuf partitions so HTTP range reads remain small.  Querying every
    partition also handles AOIs that cross a continental boundary.
    """
    id_field = 'reach_id' if kind == 'reaches' else 'node_id'
    frames: list[gpd.GeoDataFrame] = []
    counts: dict[str, int] = {}
    for continent_code in CONTINENTS:
        selected = select_layer(
            aoi_geometry,
            f'{DEFAULT_REFERENCE_ROOT}/{kind}/{continent_code}.fgb',
        )
        counts[continent_code] = len(selected)
        if not selected.empty:
            selected = selected.copy()
            selected['reference_partition'] = continent_code
            frames.append(selected)
    if not frames:
        return gpd.GeoDataFrame(geometry=[], crs='EPSG:4326'), counts
    merged = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs='EPSG:4326')
    merged[id_field] = merged[id_field].map(clean_id)
    return merged.drop_duplicates(id_field).reset_index(drop=True), counts

AZURE_ACCOUNT = 'ihpwinsdata'

AZURE_HOST = f'{AZURE_ACCOUNT}.blob.core.windows.net'

AZURE_CONTAINER = 'swot'

HYDROCRON_URL = 'https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries'

COLLECTION = 'SWOT_L2_HR_RiverSP_D'

DEFAULT_START = '2023-03-30T00:00:00Z'

REACH_FIELDS = 'reach_id,time_str,cycle_id,pass_id,wse,slope,width,area_total,dschg_gm,dschg_gm_q,reach_q,reach_q_b,river_name,crid,sword_version,collection_shortname,collection_version,granuleUR'

NODE_FIELDS = 'node_id,reach_id,time_str,lat,lon,river_name,wse,wse_u,wse_r_u,width,width_u,node_q,node_q_b,ice_clim_f,xovr_cal_q,cycle_id,pass_id,crid,sword_version,collection_shortname,collection_version,granuleUR'

REACH_OUTPUT_COLUMNS = ['reach_id', 'time_utc', 'wse', 'wse_units', 'slope', 'slope_units', 'width', 'width_units', 'area_total', 'area_total_units', 'dschg_gm', 'dschg_gm_units', 'dschg_gm_q', 'reach_q', 'reach_q_b', 'consensus_q', 'consensus_q_units', 'cycle_id', 'pass_id', 'river_name', 'crid', 'sword_version', 'collection_shortname', 'collection_version', 'granuleUR']

_thread_local = threading.local()

@dataclass
class FeatureResult:
    product: str
    feature_id: str
    status: str
    input_rows: int = 0
    accepted_rows: int = 0
    output_csv: str = ''
    message: str = ''

@dataclass
class UploadResult:
    blob: str
    status: str
    size_bytes: int
    sha256: str
    message: str = ''

def utc_now() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def clean_id(value: Any) -> str:
    text = str(value).strip()
    return text[:-2] if text.endswith('.0') else text

def validate_region_id(value: str) -> str:
    region_id = value.strip().lower()
    if not re.fullmatch('[a-z0-9]+(?:-[a-z0-9]+)*', region_id):
        raise ValueError('region-id must contain lowercase letters, numbers, and hyphens')
    return region_id

def session() -> requests.Session:
    if not hasattr(_thread_local, 'session'):
        _thread_local.session = requests.Session()
    return _thread_local.session

def response_frame(text: str) -> pd.DataFrame:
    payload = (text or '').strip()
    if not payload:
        return pd.DataFrame()
    if payload.startswith('{'):
        payload = str(json.loads(payload).get('results', {}).get('csv', '') or '').strip()
    if not payload:
        return pd.DataFrame()
    try:
        return pd.read_csv(io.StringIO(payload), dtype={'node_id': 'string', 'reach_id': 'string'})
    except EmptyDataError:
        return pd.DataFrame()

def hydrocron_get(params: dict[str, str], timeout: int, retries: int) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = session().get(HYDROCRON_URL, params=params, timeout=timeout)
            if response.status_code in {429, 500, 502, 503, 504}:
                raise RuntimeError(f'transient HTTP {response.status_code}')
            return response
        except Exception as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(min(30.0, 0.8 * 2 ** attempt) + random.uniform(0, 0.5))
    raise RuntimeError(f'Hydrocron request failed after {retries} attempts: {last_error}')

def normalize_reaches(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=REACH_OUTPUT_COLUMNS)
    if 'time_str' not in frame:
        raise ValueError('Hydrocron reach response has no time_str field')
    output = frame.copy()
    parsed = pd.to_datetime(output['time_str'], format='mixed', errors='coerce', utc=True)
    output = output.loc[parsed.notna()].copy()
    output['time_utc'] = parsed.loc[output.index].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    if 'reach_id' in output:
        output['reach_id'] = output['reach_id'].map(clean_id)
    if 'consensus_q' not in output:
        output['consensus_q'] = pd.NA
    if 'consensus_q_units' not in output:
        output['consensus_q_units'] = 'm^3/s'
    for column in REACH_OUTPUT_COLUMNS:
        if column not in output:
            output[column] = pd.NA
    # Hydrocron represents missing RiverSP measurements with extreme numeric
    # sentinels (commonly -999999999999). They must be blanked before storage;
    # otherwise Terria charts them as enormous real values.
    for column in ('wse', 'slope', 'width', 'area_total', 'dschg_gm',
                   'dschg_gm_q', 'reach_q', 'reach_q_b', 'consensus_q'):
        values = pd.to_numeric(output[column], errors='coerce')
        output[column] = values.mask(values.abs().ge(FILL_ABS_THRESHOLD))
    return output[REACH_OUTPUT_COLUMNS].drop_duplicates(['reach_id', 'time_utc', 'cycle_id', 'pass_id'], keep='last').sort_values('time_utc').reset_index(drop=True)

def atomic_csv(frame: pd.DataFrame, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + '.tmp')
    frame.to_csv(temporary, index=False, lineterminator='\n')
    temporary.replace(destination)

def download_feature(product: str, feature_id: str, destination: Path, start: str, end: str, timeout: int, retries: int, overwrite_local: bool) -> FeatureResult:
    if destination.exists() and (not overwrite_local):
        try:
            rows = len(pd.read_csv(destination))
        except EmptyDataError:
            rows = 0
        return FeatureResult(product, feature_id, 'skipped_existing', rows, rows, str(destination))
    fields = REACH_FIELDS if product == 'reaches' else NODE_FIELDS
    params = {'feature': 'Reach' if product == 'reaches' else 'Node', 'feature_id': feature_id, 'start_time': start, 'end_time': end, 'output': 'csv', 'collection_name': COLLECTION, 'fields': fields}
    try:
        response = hydrocron_get(params, timeout, retries)
        if response.status_code != 200:
            message = (response.text or '').replace('\n', ' ')[:400]
            return FeatureResult(product, feature_id, f'http_{response.status_code}', message=message)
        raw = response_frame(response.text)
        normalized = normalize_reaches(raw) if product == 'reaches' else apply_quality_filter(raw)[0]
        if normalized.empty:
            return FeatureResult(product, feature_id, 'empty', len(raw), 0)
        atomic_csv(normalized, destination)
        return FeatureResult(product, feature_id, 'ok', len(raw), len(normalized), str(destination))
    except Exception as exc:
        return FeatureResult(product, feature_id, 'error', message=str(exc))

def download_product(product: str, ids: Iterable[str], output_directory: Path, start: str, end: str, workers: int, timeout: int, retries: int, overwrite_local: bool) -> list[FeatureResult]:
    ids = list(ids)
    prefix = 'reach' if product == 'reaches' else 'node'
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(download_feature, product, feature_id, output_directory / f'{prefix}_{feature_id}.csv', start, end, timeout, retries, overwrite_local): feature_id for feature_id in ids}
        results: list[FeatureResult] = []
        for (count, future) in enumerate(as_completed(futures), 1):
            results.append(future.result())
            if count % 100 == 0 or count == len(futures):
                print(f'{product}: {count}/{len(futures)} {dict(Counter((r.status for r in results)))}')
    return sorted(results, key=lambda item: item.feature_id)

def add_timeseries_properties(frame: gpd.GeoDataFrame, product: str, results: list[FeatureResult], region_id: str) -> gpd.GeoDataFrame:
    id_field = 'reach_id' if product == 'reaches' else 'node_id'
    singular = 'reach' if product == 'reaches' else 'node'
    accepted = {item.feature_id: item.accepted_rows for item in results if item.status in {'ok', 'ok_dawg', 'skipped_existing'}}
    output = frame.copy()
    output[id_field] = output[id_field].map(clean_id)
    output['observation_count'] = output[id_field].map(accepted).fillna(0).astype(int)
    output['has_data'] = output['observation_count'].gt(0)
    base = f'https://{AZURE_HOST}/{AZURE_CONTAINER}/regions/{region_id}/{product}/timeseries'
    output['url'] = output.apply(lambda row: f'{base}/{singular}_{row[id_field]}.csv' if row['has_data'] else None, axis=1)
    output['hydrocron_collection'] = COLLECTION
    output['sword_version'] = 'v17b'
    first_by_id: dict[str, str] = {}
    latest_by_id: dict[str, str] = {}
    for item in results:
        if not item.output_csv or item.accepted_rows <= 0:
            continue
        try:
            times = pd.to_datetime(pd.read_csv(item.output_csv, usecols=['time_utc'])['time_utc'], utc=True, errors='coerce').dropna()
        except (FileNotFoundError, EmptyDataError, ValueError):
            continue
        if not times.empty:
            first_by_id[item.feature_id] = times.min().strftime('%Y-%m-%dT%H:%M:%SZ')
            latest_by_id[item.feature_id] = times.max().strftime('%Y-%m-%dT%H:%M:%SZ')
    output['first_observation_utc'] = output[id_field].map(first_by_id)
    output['latest_observation_utc'] = output[id_field].map(latest_by_id)
    return output

def write_geojson(frame: gpd.GeoDataFrame, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = json.loads(frame.to_json(drop_id=True, to_wgs84=True))
    destination.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')

def content_type(path: Path) -> str:
    return {'.csv': 'text/csv; charset=utf-8', '.json': 'application/json; charset=utf-8', '.geojson': 'application/geo+json; charset=utf-8'}.get(path.suffix.lower(), 'application/octet-stream')

def checked_blob(region_id: str, blob: str) -> str:
    normalized = str(PurePosixPath(blob.replace('\\', '/')))
    allowed = f'regions/{region_id}/'
    if not normalized.startswith(allowed) or '..' in PurePosixPath(normalized).parts:
        raise ValueError(f'Refusing Azure path outside {allowed}: {normalized}')
    return normalized

def upload_file(container: ContainerClient, region_id: str, source: Path, blob: str, overwrite: bool) -> UploadResult:
    blob = checked_blob(region_id, blob)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    try:
        with source.open('rb') as stream:
            container.upload_blob(blob, stream, length=source.stat().st_size, overwrite=overwrite, content_settings=ContentSettings(content_type=content_type(source)), metadata={'sha256': digest}, max_concurrency=1)
        return UploadResult(blob, 'uploaded', source.stat().st_size, digest)
    except ResourceExistsError:
        return UploadResult(blob, 'skipped_existing', source.stat().st_size, digest)
    except Exception as exc:
        return UploadResult(blob, 'error', source.stat().st_size, digest, str(exc))

def run_historical_aoi_pipeline(*, aoi: str, region_id: str, display_name: str, continent: str='NA', output_root: str='output', start: str=DEFAULT_START, end: str | None=None, workers: int=8, timeout: int=60, retries: int=5, upload: bool=False, overwrite_local: bool=False, overwrite_azure: bool=False, connection_string_env: str='AZURE_STORAGE_CONNECTION_STRING', limit_reaches: int | None=None, limit_nodes: int | None=None, select_only: bool=False, dawg_file: str | None=None, register_manifest: bool=True) -> dict[str, Any]:
    """Run the historical pipeline; suitable for an Airflow PythonOperator."""
    region_id = validate_region_id(region_id)
    if not 1 <= workers <= 32:
        raise ValueError('workers must be between 1 and 32')
    if overwrite_azure and (not upload):
        raise ValueError('overwrite_azure requires upload=True')
    end = end or utc_now()
    run_started = utc_now()
    root = Path(output_root) / region_id / 'historical'
    root.mkdir(parents=True, exist_ok=True)
    aoi_frame = gpd.read_file(aoi)
    if aoi_frame.empty or aoi_frame.crs is None:
        raise ValueError('AOI must contain geometry and have a defined CRS')
    aoi_frame = aoi_frame.to_crs('EPSG:4326')
    mask = aoi_frame.geometry.union_all()
    reference = DEFAULT_REFERENCE_ROOT.rstrip('/')
    continent = continent.upper()
    if continent in {'AUTO', 'GLOBAL'}:
        reaches, reach_partition_counts = select_global_layer(mask, 'reaches')
        nodes, node_partition_counts = select_global_layer(mask, 'nodes')
        continent = 'GLOBAL'
    else:
        reaches = select_layer(mask, f'{reference}/reaches/{continent}.fgb')
        nodes = select_layer(mask, f'{reference}/nodes/{continent}.fgb')
        reach_partition_counts = {continent: len(reaches)}
        node_partition_counts = {continent: len(nodes)}
    if reaches.empty:
        raise ValueError('AOI does not intersect any SWORD v17b reaches')
    reaches['reach_id'] = reaches['reach_id'].map(clean_id)
    nodes['node_id'] = nodes['node_id'].map(clean_id)
    reaches = reaches.drop_duplicates('reach_id')
    nodes = nodes.drop_duplicates('node_id')
    reach_ids = reaches['reach_id'].tolist()[:limit_reaches]
    node_ids = nodes['node_id'].tolist()[:limit_nodes]
    reaches = reaches[reaches['reach_id'].isin(reach_ids)].copy()
    nodes = nodes[nodes['node_id'].isin(node_ids)].copy()
    source_dir = root / 'source'
    source_dir.mkdir(parents=True, exist_ok=True)
    aoi_path = source_dir / 'aoi.geojson'
    write_geojson(aoi_frame, aoi_path)
    write_geojson(reaches, source_dir / 'selected_reaches.geojson')
    write_geojson(nodes, source_dir / 'selected_nodes.geojson')
    if select_only:
        summary = {'region_id': region_id, 'reach_count': len(reaches), 'node_count': len(nodes), 'select_only': True}
        (root / 'run_summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
        return summary
    reach_results = download_product('reaches', reach_ids, root / 'reaches' / 'timeseries', start, end, workers, timeout, retries, overwrite_local)
    node_results = download_product('nodes', node_ids, root / 'nodes' / 'timeseries', start, end, workers, timeout, retries, overwrite_local)
    dawg_summary = None
    if dawg_file:
        from dawg_discharge import merge_dawg_consensus
        merged = merge_dawg_consensus(Path(dawg_file), reach_ids, root / 'reaches' / 'timeseries')
        dawg_summary = asdict(merged)
        by_id = {item.feature_id: item for item in reach_results}
        for reach_id in merged.matched_reach_ids:
            csv_path = root / 'reaches' / 'timeseries' / f'reach_{reach_id}.csv'
            if not csv_path.exists():
                continue
            rows = len(pd.read_csv(csv_path))
            item = by_id[reach_id]
            item.accepted_rows = rows
            item.output_csv = str(csv_path)
            if item.status not in {'ok', 'skipped_existing'}:
                item.status = 'ok_dawg'
    result_rows = reach_results + node_results
    logs_dir = root / 'logs'
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / 'historical_download.csv'
    pd.DataFrame((asdict(item) for item in result_rows)).to_csv(log_path, index=False)
    reach_layer = add_timeseries_properties(reaches, 'reaches', reach_results, region_id)
    node_layer = add_timeseries_properties(nodes, 'nodes', node_results, region_id)
    reach_geojson = root / 'reaches' / 'reaches.geojson'
    node_geojson = root / 'nodes' / 'nodes.geojson'
    write_geojson(reach_layer, reach_geojson)
    write_geojson(node_layer, node_geojson)
    transient_statuses = {'error', 'http_429', 'http_500', 'http_502', 'http_503', 'http_504'}
    unresolved = [item for item in result_rows if item.status in transient_statuses]
    if unresolved:
        sample = ', '.join(f'{item.product}:{item.feature_id}:{item.status}' for item in unresolved[:10])
        raise RuntimeError(f'{len(unresolved)} transient Hydrocron requests remain unresolved: {sample}')
    summary: dict[str, Any] = {'schema_version': 2, 'region_id': region_id, 'display_name': display_name, 'run_started_utc': run_started, 'run_finished_utc': utc_now(), 'window': {'start': start, 'end': end}, 'hydrocron_collection': COLLECTION, 'sword_version': 'v17b', 'reference_scope': 'global_partitioned', 'reference_partition_counts': {'reaches': reach_partition_counts, 'nodes': node_partition_counts}, 'reaches': {'selected': len(reaches), 'with_data': int(reach_layer.has_data.sum()), 'observations': sum((item.accepted_rows for item in reach_results)), 'statuses': dict(Counter((item.status for item in reach_results)))}, 'nodes': {'selected': len(nodes), 'with_data': int(node_layer.has_data.sum()), 'observations': sum((item.accepted_rows for item in node_results)), 'statuses': dict(Counter((item.status for item in node_results)))}, 'dawg': dawg_summary, 'node_quality_filter': {'ice_clim_f': '== 0', 'node_q': '< 3', 'xovr_cal_q': '< 2', 'wse': 'finite non-fill', 'node_q_b_bits_unset_zero_based': [13, 14, 19, 23]}, 'azure': {'account': AZURE_ACCOUNT, 'container': AZURE_CONTAINER, 'prefix': f'regions/{region_id}/', 'uploaded': False}}
    summary_path = root / 'run_summary.json'
    summary_path.write_text(json.dumps(summary, indent=2), encoding='utf-8')
    manifest = {'schema_version': 1, 'region_id': region_id, 'display_name': display_name, 'status': 'historical_built', 'aoi_blob': f'regions/{region_id}/source/aoi.geojson', 'products': {'reaches': {'geometry_blob': f'regions/{region_id}/reaches/reaches.geojson', 'timeseries_prefix': f'regions/{region_id}/reaches/timeseries/', 'filename': 'reach_{reach_id}.csv'}, 'nodes': {'geometry_blob': f'regions/{region_id}/nodes/nodes.geojson', 'timeseries_prefix': f'regions/{region_id}/nodes/timeseries/', 'filename': 'node_{node_id}.csv'}}, 'historical_summary': summary}
    manifest_path = root / 'manifest.json'
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    plans: list[tuple[Path, str]] = [(aoi_path, f'regions/{region_id}/source/aoi.geojson'), (source_dir / 'selected_reaches.geojson', f'regions/{region_id}/source/selected_reaches.geojson'), (source_dir / 'selected_nodes.geojson', f'regions/{region_id}/source/selected_nodes.geojson'), (reach_geojson, f'regions/{region_id}/reaches/reaches.geojson'), (node_geojson, f'regions/{region_id}/nodes/nodes.geojson'), (log_path, f'regions/{region_id}/logs/historical_download.csv'), (summary_path, f'regions/{region_id}/logs/historical_summary.json')]
    if register_manifest:
        plans.append((manifest_path, f'regions/{region_id}/manifest.json'))
    for item in result_rows:
        if item.status in {'ok', 'ok_dawg', 'skipped_existing'} and item.accepted_rows > 0:
            singular = 'reach' if item.product == 'reaches' else 'node'
            plans.append((Path(item.output_csv), f'regions/{region_id}/{item.product}/timeseries/{singular}_{item.feature_id}.csv'))
    if upload:
        connection_string = runtime_secret(connection_string_env)
        if not connection_string:
            raise ValueError(f'Airflow Variable or environment variable {connection_string_env!r} is empty')
        container = ContainerClient.from_connection_string(connection_string, AZURE_CONTAINER)
        if container.account_name != AZURE_ACCOUNT:
            raise ValueError(f'Refusing unexpected Azure account {container.account_name!r}')
        container.get_container_properties()
        upload_results: list[UploadResult] = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(upload_file, container, region_id, path, blob, overwrite_azure) for (path, blob) in plans]
            for future in as_completed(futures):
                upload_results.append(future.result())
        upload_log = logs_dir / 'historical_upload.csv'
        pd.DataFrame((asdict(item) for item in sorted(upload_results, key=lambda x: x.blob))).to_csv(upload_log, index=False)
        final_log = upload_file(container, region_id, upload_log, f'regions/{region_id}/logs/historical_upload.csv', overwrite_azure)
        upload_results.append(final_log)
        counts = Counter((item.status for item in upload_results))
        summary['azure'].update({'uploaded': True, 'planned_blobs': len(plans) + 1, 'statuses': dict(counts)})
        summary['run_finished_utc'] = utc_now()
        summary_path.write_text(json.dumps(summary, indent=2), encoding='utf-8')
        upload_file(container, region_id, summary_path, f'regions/{region_id}/logs/historical_summary.json', True)
        if counts.get('error'):
            raise RuntimeError(f"Azure upload failures: {counts['error']}")
    return summary

CKAN = 'https://ihp-wins.unesco.org'

TERRIA = f'{CKAN}/terria/#start='

DATASET = 'swot-sword-regional-observations'

AZURE_ACCOUNT = 'ihpwinsdata'

AZURE_CONTAINER = 'swot'

def safe_region(value: str) -> str:
    value = value.strip().lower()
    if not re.fullmatch('[a-z0-9]+(?:-[a-z0-9]+)*', value):
        raise ValueError('region_id must contain lowercase letters, numbers, and hyphens')
    return value

def bounds(payload: dict[str, Any]) -> dict[str, float]:
    points: list[list[float]] = []

    def walk(value: Any) -> None:
        if isinstance(value, list) and len(value) >= 2 and all((isinstance(x, (int, float)) for x in value[:2])):
            points.append(value[:2])
        elif isinstance(value, list):
            for item in value:
                walk(item)
    for feature in payload.get('features', []):
        walk((feature.get('geometry') or {}).get('coordinates'))
    if not points:
        raise ValueError('Cannot publish an empty GeoJSON layer')
    return {'west': min((x for (x, _) in points)), 'south': min((y for (_, y) in points)), 'east': max((x for (x, _) in points)), 'north': max((y for (_, y) in points))}

def camera(value: dict[str, float]) -> dict[str, float]:
    dx = max((value['east'] - value['west']) * 0.08, 0.05)
    dy = max((value['north'] - value['south']) * 0.08, 0.05)
    return {'west': value['west'] - dx, 'south': value['south'] - dy, 'east': value['east'] + dx, 'north': value['north'] + dy}

def chart_html(kind: str) -> str:
    if kind == 'nodes':
        return "<div><strong>SWOT River Node</strong><p>Teal nodes contain Version D observations passing the project quality filter; gray nodes currently have none.</p></div>{{#has_data}}<chart id='{{node_id}}-variables' title='Node Variables - {{node_id}}' src='{{url}}' x-column='time_utc' y-columns='wse,width' chart-type='lineAndPoint' stroke='rgba(0,0,0,0)' can-download='false'></chart>{{/has_data}}<table><tr><th>Node ID</th><td>{{node_id}}</td></tr><tr><th>Reach ID</th><td>{{reach_id}}</td></tr><tr><th>Accepted observations</th><td>{{observation_count}}</td></tr>{{#has_data}}<tr><th>Full CSV</th><td><a href='{{url}}' target='_blank'>Open CSV</a></td></tr>{{/has_data}}</table>"
    return "<div><strong>SWOT River Reach</strong><p>Blue reaches contain valid DAWG consensus discharge; brown reaches retain SWOT observations without discharge.</p></div>{{#has_data}}<chart id='{{reach_id}}-variables' title='Reach Variables - {{reach_id}}' src='{{chart_url}}' x-column='time_utc' y-columns='wse,slope_cm_per_km,width,consensus_q' column-titles='slope_cm_per_km:Slope (cm/km)' chart-type='lineAndPoint' stroke='rgba(0,0,0,0)' can-download='false'></chart>{{/has_data}}<table><tr><th>Reach ID</th><td>{{reach_id}}</td></tr><tr><th>Observations</th><td>{{observation_count}}</td></tr><tr><th>Discharge observations</th><td>{{discharge_count}}</td></tr>{{#has_data}}<tr><th>Full CSV</th><td><a href='{{url}}' target='_blank'>Open CSV</a></td></tr>{{/has_data}}</table>"

def terria_config(region_id: str, display_name: str, kind: str, url: str, layer_bounds: dict[str, float]) -> dict[str, Any]:
    singular = 'node' if kind == 'nodes' else 'reach'
    (layer_id, group_id) = (f'{region_id}-sword-{kind}-version-d', f'//{region_id}-sword-{kind}-version-d')
    style = {'marker-size': '7', 'stroke-width': 0} if kind == 'nodes' else {'stroke-width': 5}
    (prop, color_key) = ('has_data', 'marker-color') if kind == 'nodes' else ('has_discharge', 'stroke')

    def styled(color: str) -> dict[str, Any]:
        result = {color_key: color}
        result.update({'marker-size': '7'} if kind == 'nodes' else {'stroke-width': 5})
        return result
    cam = camera(layer_bounds)
    return {'version': '8.0.0', 'initSources': [{'stratum': 'user', 'models': {'/': {'members': [group_id], 'type': 'group'}, '__User-Added_Data__': {'members': [], 'knownContainerUniqueIds': ['/'], 'type': 'group'}, group_id: {'isOpen': True, 'members': [layer_id], 'name': f'{display_name} SWOT-SWORD {kind.title()}', 'knownContainerUniqueIds': ['/'], 'type': 'group'}, layer_id: {'type': 'geojson', 'name': f'{display_name} SWOT-SWORD {kind.title()} (click to chart)', 'url': url, 'cacheDuration': '5m', 'show': True, 'isOpenInWorkbench': False, 'knownContainerUniqueIds': [group_id], 'featureInfoTemplate': {'name': f'{singular.title()} {{{{{singular}_id}}}}', 'template': chart_html(kind)}, 'style': style, 'perPropertyStyles': [{'properties': {prop: True}, 'style': styled('#159D95' if kind == 'nodes' else '#1e6091')}, {'properties': {prop: False}, 'style': styled('#7B8490' if kind == 'nodes' else '#a67c52')}]}}, 'workbench': [layer_id], 'timeline': [], 'initialCamera': cam, 'homeCamera': cam, 'viewerMode': '2d', 'settings': {'baseMapId': 'basemap-openstreetmap', 'baseMaximumScreenSpaceError': 2, 'useNativeResolution': False, 'alwaysShowTimeline': False}, 'stories': []}]}

def api(session: requests.Session, action: str, data: dict[str, Any], files=None,
        timeout: int=900, attempts: int=6) -> Any:
    """Call CKAN with bounded backoff for proxy throttling and outages."""
    response = None
    for attempt in range(attempts):
        # requests consumes upload streams. Rewind them before a retry so a
        # throttled upload is never resent as an empty file.
        for value in (files or {}).values():
            if isinstance(value, tuple) and len(value) > 1 and hasattr(value[1], 'seek'):
                value[1].seek(0)
        response = session.post(f'{CKAN}/api/3/action/{action}', data=data,
                                files=files, timeout=timeout)
        if response.status_code not in {429, 500, 502, 503, 504} or attempt + 1 == attempts:
            break
        retry_after = str(response.headers.get('Retry-After') or '').strip()
        delay = float(retry_after) if retry_after.replace('.', '', 1).isdigit() else min(120.0, 10.0 * 2 ** attempt)
        response.close()
        time.sleep(delay + random.uniform(0, 1))
    assert response is not None
    response.raise_for_status()
    payload = response.json()
    if not payload.get('success'):
        raise RuntimeError(json.dumps(payload, ensure_ascii=False))
    return payload['result']

def api_json(session: requests.Session, action: str, payload: dict[str, Any], timeout: int=180) -> Any:
    response = session.post(f'{CKAN}/api/3/action/{action}', json=payload, timeout=timeout)
    response.raise_for_status()
    body = response.json()
    if not body.get('success'):
        raise RuntimeError(json.dumps(body, ensure_ascii=False))
    return body['result']

def publish_resource(session: requests.Session, dataset: str, name: str, description: str, path: Path, resource_id: str | None=None) -> dict[str, Any]:
    package = api(session, 'package_show', {'id': dataset})
    (resources, selected) = (package.get('resources', []), None)
    if resource_id:
        selected = next((item for item in resources if item.get('id') == resource_id), None)
        if not selected:
            raise ValueError(f'Configured CKAN resource {resource_id} is not in dataset {dataset}')
    else:
        matches = [item for item in resources if item.get('name', '').casefold() == name.casefold()]
        if len(matches) > 1:
            raise RuntimeError(f"Duplicate resources named {name}: {[x['id'] for x in matches]}")
        selected = matches[0] if matches else None
    data = {'name': name, 'description': description, 'format': 'GeoJSON'}
    data['id' if selected else 'package_id'] = selected['id'] if selected else package['id']
    with path.open('rb') as handle:
        return api(session, 'resource_update' if selected else 'resource_create', data, {'upload': (path.name, handle, 'application/geo+json')})

def publish_url_resource(session: requests.Session, dataset: str, name: str,
                         description: str, url: str,
                         resource_id: str | None=None) -> dict[str, Any]:
    """Create or update a CKAN resource that points at a stable Azure object."""
    package = api(session, 'package_show', {'id': dataset})
    resources = package.get('resources', [])
    selected = next((item for item in resources if item.get('id') == resource_id), None) if resource_id else None
    if resource_id and not selected:
        raise ValueError(f'Configured CKAN resource {resource_id} is not in dataset {dataset}')
    if not selected:
        matches = [item for item in resources if str(item.get('name') or '').casefold() == name.casefold()]
        if len(matches) > 1:
            raise RuntimeError(f"Duplicate resources named {name}: {[item['id'] for item in matches]}")
        selected = matches[0] if matches else None
    data = {'name': name, 'description': description, 'format': 'GeoJSON', 'url': url, 'url_type': ''}
    data['id' if selected else 'package_id'] = selected['id'] if selected else package['id']
    return api(session, 'resource_update' if selected else 'resource_create', data)

def publish_view(session: requests.Session, resource_id: str, title: str, description: str, viewer_url: str) -> dict[str, Any]:
    views = api(session, 'resource_view_list', {'id': resource_id})
    existing = next((view for view in views if view.get('view_type') == 'terria_view'), None)
    data = {'resource_id': resource_id, 'view_type': 'terria_view', 'title': title, 'description': description, 'terria_instance_url': viewer_url, 'custom_config': 'NA', 'filterable': 'true', 'style': 'NA', 'custom_config_option': 'automatic', 'custom_config_url': '', 'style_custom_input': '', 'style_option': 'none'}
    if existing:
        data['id'] = existing['id']
    return api(session, 'resource_view_update' if existing else 'resource_view_create', data)

def publish_documentation(session: requests.Session, dataset: str, source: dict[str, Any]) -> dict[str, Any]:
    package = api(session, 'package_show', {'id': dataset})
    name = str(source.get('name') or 'SWOT-SWORD Regional Surface Water Data Guide')
    existing = next((item for item in package.get('resources', [])
                     if str(item.get('name') or '').casefold() == name.casefold()), None)
    if existing:
        return existing
    first = session.get(str(source['url']), allow_redirects=False, timeout=120)
    if first.status_code in {301, 302, 303, 307, 308}:
        location = first.headers.get('Location')
        if not location:
            raise RuntimeError('Documentation download redirect had no Location header')
        downloaded = requests.get(location, timeout=120)
    else:
        downloaded = first
    downloaded.raise_for_status()
    data = {
        'package_id': dataset,
        'name': name,
        'description': 'Universal guide to the regional SWOT-SWORD reach and node products, schemas, processing, quality filtering, viewer use, and limitations.',
        'format': 'PDF',
    }
    return api(session, 'resource_create', data, {
        'upload': ('swot_sword_regional_data_guide.pdf', io.BytesIO(downloaded.content), 'application/pdf')
    })

def improve_dataset_metadata(session: requests.Session, dataset: str, root: Path,
                             layer_bounds: dict[str, float]) -> dict[str, Any]:
    manifest_path = root / 'manifest.json'
    if manifest_path.exists():
        summary = json.loads(manifest_path.read_text(encoding='utf-8'))['historical_summary']
    elif (root / 'run_summary.json').exists():
        summary = json.loads((root / 'run_summary.json').read_text(encoding='utf-8'))
    else:
        summary = json.loads((root / 'logs' / 'historical_summary.json').read_text(encoding='utf-8'))
    observed_starts: list[pd.Timestamp] = []
    for kind in ('reaches', 'nodes'):
        path = root / kind / f'{kind}.geojson'
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding='utf-8'))
        for feature in payload.get('features', []):
            value = (feature.get('properties') or {}).get('first_observation_utc')
            parsed = pd.to_datetime(value, utc=True, errors='coerce')
            if pd.notna(parsed):
                observed_starts.append(parsed)
    start = min(observed_starts).strftime('%Y-%m-%d') if observed_starts else str(summary['window']['start'])[:10]
    bbox = {
        'type': 'Polygon',
        'coordinates': [[
            [layer_bounds['west'], layer_bounds['south']],
            [layer_bounds['east'], layer_bounds['south']],
            [layer_bounds['east'], layer_bounds['north']],
            [layer_bounds['west'], layer_bounds['north']],
            [layer_bounds['west'], layer_bounds['south']],
        ]],
    }
    dawg_note = (' Reach-scale DAWG consensus discharge is included where available.'
                 if summary.get('dawg') else '')
    provenance = (
        'Automatically generated from a researcher-supplied area of interest. '
        'The AOI selects intersecting SWORD v17b reaches and nodes. Historical '
        'SWOT RiverSP Version D observations are retrieved through Hydrocron, '
        'quality-filtered, normalized, and stored as per-feature CSV files.' + dawg_note
    )
    purpose = (
        'Provide an experimental, reproducible regional view of SWOT river '
        'observations for scientific exploration, screening, and research.'
    )
    lineage = [
        'https://podaac.jpl.nasa.gov/dataset/SWOT_L2_HR_RiverSP_D',
        'https://podaac.github.io/hydrocron/',
        'https://zenodo.org/records/15299138',
    ]
    if summary.get('dawg'):
        lineage.append('https://podaac.jpl.nasa.gov/dataset/SWOT_L4_DAWG_SOS_DISCHARGE_V3')
    return api_json(session, 'package_patch', {
        'id': dataset,
        'tags': [{'name': value} for value in
                 ('SWOT', 'SWORD', 'surface water', 'river', 'time series', 'experimental')],
        'theme': ['http://inspire.ec.europa.eu/theme/hy'],
        'theme_eu': ['http://publications.europa.eu/resource/authority/data-theme/ENVI'],
        'spatial': json.dumps(bbox, separators=(',', ':')),
        'reference_system': 'http://www.opengis.net/def/crs/EPSG/0/4326',
        'representation_type': 'http://inspire.ec.europa.eu/metadata-codelist/SpatialRepresentationType/vector',
        'temporal_start': start,
        'frequency': 'http://publications.europa.eu/resource/authority/frequency/DAILY',
        'provenance': {'en': provenance, 'es': '', 'fr': ''},
        'purpose': {'en': purpose, 'es': '', 'fr': ''},
        'lineage_source': lineage,
        'access_level': 'public',
        'access_rights': 'http://inspire.ec.europa.eu/metadata-codelist/LimitationsOnPublicAccess/noLimitations',
        'publisher_name': 'UNESCO Intergovernmental Hydrological Programme',
        'publisher_type': 'http://purl.org/adms/publishertype/SupraNationalAuthority',
        'version_notes': {
            'en': 'Initial automatically provisioned experimental regional publication.',
            'es': '',
            'fr': '',
        },
    })

def ensure_destination_dataset(session: requests.Session, submission: dict[str, Any]) -> dict[str, Any]:
    """Return the requested regional dataset, creating it once when absent."""
    requested_name = str(submission.get('destination_dataset_name') or '').strip()
    requested_title = str(submission.get('destination_dataset_title') or '').strip()
    if not requested_name or not requested_title:
        # Backward compatibility for the original manually configured inbox.
        return api(session, 'package_show', {'id': submission['dataset_id']})
    description = (
        f'Experimental SWOT RiverSP Version D time-series observations for '
        f'{submission["display_name"]}, organized using SWORD v17b reaches and nodes. '
        'This automatically generated research dataset is intended for exploration '
        'and is not validated for operational or safety-critical decisions.'
    )
    try:
        existing = api(session, 'package_show', {'id': requested_name})
    except requests.HTTPError as exc:
        if exc.response is None or exc.response.status_code != 404:
            raise
    else:
        return api(session, 'package_patch', {
            'id': existing['id'],
            'title': requested_title,
            'title_translated': json.dumps({'en': requested_title, 'es': '', 'fr': ''}),
            'notes': description,
            'notes_translated': json.dumps({'en': description, 'es': '', 'fr': ''}),
            'version': 'Experimental',
        })
    data = {
        'name': requested_name,
        'identifier': requested_name,
        'title': requested_title,
        'title_translated': json.dumps({'en': requested_title, 'es': '', 'fr': ''}),
        'owner_org': 'e3256cf0-328d-40fe-80f1-f2bcf0a466aa',
        'contact_name': 'Trevor Wilkerson',
        'contact_email': 'admin@saltosllc.com',
        'publisher_name': 'UNESCO Intergovernmental Hydrological Programme',
        'notes': description,
        'notes_translated': json.dumps({'en': description, 'es': '', 'fr': ''}),
        'license_id': 'cc-by-sa',
        'version': 'Experimental',
        'dataset_scope': 'spatial_dataset',
        'dcat_type': 'http://inspire.ec.europa.eu/metadata-codelist/ResourceType/dataset',
        'language': 'http://publications.europa.eu/resource/authority/language/ENG',
        'topic': 'http://inspire.ec.europa.eu/metadata-codelist/TopicCategory/inlandWaters',
        'access_level': 'public',
    }
    return api(session, 'package_create', data)

def add_discharge(payload: dict[str, Any], csv_dir: Path, chart_dir: Path, region_id: str) -> None:
    chart_dir.mkdir(parents=True, exist_ok=True)
    for feature in payload['features']:
        (props, rows) = (feature['properties'], [])
        (reach_id, count) = (str(props['reach_id']), 0)
        path = csv_dir / f'reach_{reach_id}.csv'
        if path.exists():
            with path.open(encoding='utf-8-sig', newline='') as handle:
                rows = list(csv.DictReader(handle))
            count = sum((str(row.get('consensus_q', '')).strip().lower() not in {'', 'nan', 'none', 'null'} for row in rows))
            for row in rows:
                try:
                    row['slope_cm_per_km'] = format(float(row.get('slope', '')) * 100000.0, '.10g')
                except (TypeError, ValueError):
                    row['slope_cm_per_km'] = ''
            chart_path = chart_dir / f'reach_{reach_id}.csv'
            with chart_path.open('w', encoding='utf-8', newline='') as handle:
                fields = ['time_utc', 'wse', 'slope_cm_per_km', 'width', 'consensus_q']
                writer = csv.DictWriter(handle, fieldnames=fields, lineterminator='\n', extrasaction='ignore')
                writer.writeheader()
                writer.writerows(rows)
            props['chart_url'] = f'https://{AZURE_ACCOUNT}.blob.core.windows.net/{AZURE_CONTAINER}/regions/{region_id}/reaches/chart-timeseries/reach_{reach_id}.csv'
        (props['discharge_count'], props['has_discharge']) = (count, count > 0)

def _load_existing_manifest(container: ContainerClient, blob: str) -> dict[str, Any]:
    try:
        return json.loads(container.get_blob_client(blob).download_blob().readall())
    except Exception as exc:
        if getattr(exc, 'status_code', None) == 404:
            return {}
        raise

def _upload(container: ContainerClient, blob: str, path: Path) -> None:
    media = {'.json': 'application/json; charset=utf-8', '.geojson': 'application/geo+json; charset=utf-8', '.csv': 'text/csv; charset=utf-8', '.txt': 'text/plain; charset=utf-8'}.get(path.suffix.lower(), 'application/octet-stream')
    container.get_blob_client(blob).upload_blob(path.read_bytes(), overwrite=True, content_settings=ContentSettings(content_type=media))

def publish_region(root: Path, display_name: str | None=None, region_id: str | None=None, dataset: str=DATASET, ckan_api_key: str | None=None, connection_string: str | None=None, documentation_source: dict[str, Any] | None=None) -> dict[str, Any]:
    key = (ckan_api_key or runtime_secret('IHP_WINS_CKAN_API_KEY') or runtime_secret('CKAN_API_KEY')).strip()
    connection = (connection_string or runtime_secret('AZURE_STORAGE_CONNECTION_STRING')).strip()
    if not key or not connection:
        raise RuntimeError('CKAN_API_KEY and AZURE_STORAGE_CONNECTION_STRING are required')
    manifest_path = root / 'manifest.json'
    if not manifest_path.exists():
        summary_path = root / 'logs' / 'historical_summary.json'
        if not summary_path.exists():
            raise FileNotFoundError(f'Neither {manifest_path} nor {summary_path} exists')
        summary = json.loads(summary_path.read_text(encoding='utf-8'))
        recovered_region = safe_region(region_id or str(summary['region_id']))
        recovered_name = (display_name or str(summary['display_name'])).strip()
        recovered_manifest = {
            'schema_version': 1,
            'region_id': recovered_region,
            'display_name': recovered_name,
            'status': 'historical_complete',
            'aoi_blob': f'regions/{recovered_region}/source/aoi.geojson',
            'products': {
                'reaches': {
                    'geometry_blob': f'regions/{recovered_region}/reaches/reaches.geojson',
                    'timeseries_prefix': f'regions/{recovered_region}/reaches/timeseries/',
                    'filename': 'reach_{reach_id}.csv',
                },
                'nodes': {
                    'geometry_blob': f'regions/{recovered_region}/nodes/nodes.geojson',
                    'timeseries_prefix': f'regions/{recovered_region}/nodes/timeseries/',
                    'filename': 'node_{node_id}.csv',
                },
            },
            'historical_summary': summary,
        }
        manifest_path.write_text(json.dumps(recovered_manifest, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    region_id = safe_region(region_id or str(manifest['region_id']))
    display_name = (display_name or str(manifest['display_name'])).strip()
    out = root / 'publication'
    out.mkdir(parents=True, exist_ok=True)
    container = ContainerClient.from_connection_string(connection, AZURE_CONTAINER)
    if container.account_name != AZURE_ACCOUNT:
        raise ValueError(f'Refusing unexpected Azure account {container.account_name!r}')
    manifest_blob = f'regions/{region_id}/manifest.json'
    previous_ckan = _load_existing_manifest(container, manifest_blob).get('ckan') or {}
    sources = {kind: root / kind / f'{kind}.geojson' for kind in ('reaches', 'nodes')}
    payloads = {kind: json.loads(path.read_text(encoding='utf-8')) for (kind, path) in sources.items()}
    chart_dir = out / 'reach-chart-timeseries'
    add_discharge(payloads['reaches'], root / 'reaches' / 'timeseries', chart_dir, region_id)
    prepared = {}
    for (kind, payload) in payloads.items():
        path = out / f'{region_id}_sword_{kind}_version_d.geojson'
        path.write_text(json.dumps(payload, ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
        prepared[kind] = (path, bounds(payload))
    geometry_blobs = {
        'reaches': f'regions/{region_id}/reaches/reaches.geojson',
        'nodes': f'regions/{region_id}/nodes/nodes.geojson',
    }
    for kind in ('reaches', 'nodes'):
        _upload(container, geometry_blobs[kind], prepared[kind][0])
    session = requests.Session()
    session.headers.update({'Authorization': key, 'X-CKAN-API-Key': key})
    (results, id_keys) = ({}, {'reaches': 'reach_resource_id', 'nodes': 'node_resource_id'})
    for kind in ('reaches', 'nodes'):
        name = f'{display_name} SWOT-SWORD {kind.title()} (Version D)'
        description = f'SWORD v17b river {kind} in {display_name} linked to SWOT RiverSP Version D time series.'
        azure_url = f'https://{AZURE_ACCOUNT}.blob.core.windows.net/{AZURE_CONTAINER}/{geometry_blobs[kind]}'
        resource = publish_url_resource(session, dataset, name, description, azure_url, previous_ckan.get(id_keys[kind]))
        config = terria_config(region_id, display_name, kind, azure_url, prepared[kind][1])
        viewer = TERRIA + quote_plus(json.dumps(config, ensure_ascii=False, separators=(',', ':')))
        view = publish_view(session, resource['id'], f'{display_name} SWOT-SWORD {kind.title()} Explorer', description, viewer)
        (out / f'{kind}-terria.json').write_text(json.dumps(config, indent=2), encoding='utf-8')
        (out / f'{kind}-viewer-url.txt').write_text(viewer, encoding='utf-8')
        results[kind] = {'resource_id': resource['id'], 'resource_url': resource['url'], 'view_id': view['id'], 'resource_page': f"{CKAN}/dataset/{dataset}/resource/{resource['id']}"}
    if documentation_source:
        document = publish_documentation(session, dataset, documentation_source)
        results['documentation'] = {
            'resource_id': document['id'],
            'resource_url': document['url'],
            'resource_page': f"{CKAN}/dataset/{dataset}/resource/{document['id']}",
        }
    improve_dataset_metadata(session, dataset, root, prepared['reaches'][1])
    manifest['status'] = 'published'
    for kind in ('reaches', 'nodes'):
        manifest['products'][kind]['publication_mode'] = 'azure_url'
        manifest['products'][kind]['enabled'] = True
    manifest['ckan'] = {'dataset_id': dataset, 'reach_resource_id': results['reaches']['resource_id'], 'node_resource_id': results['nodes']['resource_id']}
    if 'documentation' in results:
        manifest['ckan']['documentation_resource_id'] = results['documentation']['resource_id']
    manifest['terria'] = {kind: {'view_id': results[kind]['view_id'], 'config_blob': f'regions/{region_id}/terria/{kind}.json', 'viewer_url_blob': f'regions/{region_id}/terria/{kind}-viewer-url.txt'} for kind in ('reaches', 'nodes')}
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    uploads = {f'regions/{region_id}/reaches/reaches.geojson': prepared['reaches'][0], f'regions/{region_id}/nodes/nodes.geojson': prepared['nodes'][0]}
    for kind in ('reaches', 'nodes'):
        uploads[f'regions/{region_id}/terria/{kind}.json'] = out / f'{kind}-terria.json'
        uploads[f'regions/{region_id}/terria/{kind}-viewer-url.txt'] = out / f'{kind}-viewer-url.txt'
    for path in chart_dir.glob('*.csv'):
        uploads[f'regions/{region_id}/reaches/chart-timeseries/{path.name}'] = path
    for (blob, path) in uploads.items():
        _upload(container, blob, path)
    _upload(container, manifest_blob, manifest_path)
    (out / 'publication-summary.json').write_text(json.dumps(results, indent=2), encoding='utf-8')
    return results

AZURE_ACCOUNT = 'ihpwinsdata'

AZURE_CONTAINER = 'swot'

PENDING_PREFIX = 'incoming/pending/'

PROCESSING_PREFIX = 'incoming/processing/'

COMPLETED_PREFIX = 'incoming/completed/'

FAILED_PREFIX = 'incoming/failed/'

CONTINENTS = ('AF', 'AS', 'EU', 'NA', 'OC', 'SA')

SUPPORTED_SUFFIXES = {'.geojson', '.json', '.gpkg', '.kml', '.zip'}

def utc_now() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def runtime_secret(name: str) -> str:
    """Read an Airflow Variable in tasks, with an environment fallback."""
    try:
        from airflow.models import Variable
        value = str(Variable.get(name, default_var='')).strip()
    except Exception:
        value = os.environ.get(name, '').strip()
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    return value

def get_container(connection_string_env: str='AZURE_STORAGE_CONNECTION_STRING') -> ContainerClient:
    connection = runtime_secret(connection_string_env)
    if not connection:
        raise ValueError(f'Airflow Variable or environment variable {connection_string_env!r} is empty')
    container = ContainerClient.from_connection_string(connection, AZURE_CONTAINER)
    if container.account_name != AZURE_ACCOUNT:
        raise ValueError(f'Refusing unexpected Azure account {container.account_name!r}')
    container.get_container_properties()
    return container

def safe_blob(name: str, required_prefix: str | None=None) -> str:
    normalized = str(PurePosixPath(name.replace('\\', '/')))
    if normalized.startswith('/') or '..' in PurePosixPath(normalized).parts:
        raise ValueError(f'Unsafe blob path: {name}')
    if required_prefix and (not normalized.startswith(required_prefix)):
        raise ValueError(f'Blob must be below {required_prefix}: {name}')
    return normalized

def submission_id(metadata_blob: str) -> str:
    name = PurePosixPath(metadata_blob).name
    if not name.endswith('.submission.json'):
        raise ValueError('Submission metadata must end in .submission.json')
    value = name[:-len('.submission.json')]
    if not re.fullmatch('[a-z0-9]+(?:-[a-z0-9]+)*', value):
        raise ValueError('Submission filename must use lowercase letters, numbers, and hyphens')
    return value

def validate_submission(payload: dict[str, Any], metadata_blob: str) -> dict[str, Any]:
    required = [key for key in ('region_id', 'display_name', 'aoi_blob') if not str(payload.get(key, '')).strip()]
    if required:
        raise ValueError(f"Submission is missing: {', '.join(required)}")
    result = dict(payload)
    result['submission_id'] = submission_id(metadata_blob)
    result['region_id'] = validate_region_id(str(payload['region_id']))
    result['display_name'] = str(payload['display_name']).strip()
    result['aoi_blob'] = safe_blob(str(payload['aoi_blob']), PENDING_PREFIX)
    suffix = Path(result['aoi_blob']).suffix.lower()
    if suffix not in SUPPORTED_SUFFIXES:
        raise ValueError(f'Unsupported AOI type {suffix}; use GeoJSON, GeoPackage, KML, or zipped shapefile')
    continent = str(payload.get('continent') or 'AUTO').strip().upper()
    if continent != 'AUTO' and continent not in CONTINENTS:
        raise ValueError(f'Unknown continent {continent}')
    result['continent'] = continent
    result['dataset_id'] = str(payload.get('dataset_id') or DATASET).strip()
    result['historical_start'] = str(payload.get('historical_start') or DEFAULT_START).strip()
    return result

def discover_submissions(connection_string_env: str='AZURE_STORAGE_CONNECTION_STRING', submission_filter: str | None=None) -> list[dict[str, str]]:
    container = get_container(connection_string_env)
    allowed = {item.strip() for item in (submission_filter or '').split(',') if item.strip()}
    found = []
    pending = list(container.list_blobs(name_starts_with=PENDING_PREFIX))
    for blob in pending:
        if not blob.name.endswith('.submission.json'):
            continue
        sid = submission_id(blob.name)
        if allowed and sid not in allowed:
            continue
        found.append({'submission_id': sid, 'metadata_blob': blob.name})
    found = sorted(found, key=lambda item: item['submission_id'])
    # Airflow task logs are not consistently available in this deployment.
    # Persist a safe discovery receipt so empty dynamic maps are diagnosable.
    audit = {
        'schema_version': 1,
        'checked_utc': utc_now(),
        'account': container.account_name,
        'container': AZURE_CONTAINER,
        'pending_prefix': PENDING_PREFIX,
        'pending_blobs': [blob.name for blob in pending],
        'submission_filter': sorted(allowed),
        'matched_submissions': found,
    }
    container.get_blob_client('incoming/logs/discovery_latest.json').upload_blob(
        json.dumps(audit, ensure_ascii=False, indent=2).encode(), overwrite=True,
        content_settings=ContentSettings(content_type='application/json; charset=utf-8'))
    return found

def _download(container: ContainerClient, blob: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open('wb') as handle:
        container.get_blob_client(blob).download_blob().readinto(handle)

def _upload_json(container: ContainerClient, blob: str, payload: dict[str, Any], overwrite: bool=True) -> None:
    container.get_blob_client(blob).upload_blob(json.dumps(payload, ensure_ascii=False, indent=2).encode(), overwrite=overwrite, content_settings=ContentSettings(content_type='application/json; charset=utf-8'))

def _copy_bytes(container: ContainerClient, source: str, destination: str) -> None:
    data = container.get_blob_client(source).download_blob().readall()
    container.get_blob_client(destination).upload_blob(data, overwrite=True)

def _extract_aoi(downloaded: Path, directory: Path) -> Path:
    if downloaded.suffix.lower() != '.zip':
        return downloaded
    extract_root = directory / 'unzipped'
    extract_root.mkdir()
    with zipfile.ZipFile(downloaded) as archive:
        for member in archive.infolist():
            target = (extract_root / member.filename).resolve()
            if extract_root.resolve() not in target.parents and target != extract_root.resolve():
                raise ValueError(f'Unsafe ZIP member: {member.filename}')
        archive.extractall(extract_root)
    shapefiles = list(extract_root.rglob('*.shp'))
    if len(shapefiles) != 1:
        raise ValueError(f'A zipped shapefile must contain exactly one .shp; found {len(shapefiles)}')
    stem = shapefiles[0].with_suffix('')
    missing = [suffix for suffix in ('.shx', '.dbf', '.prj') if not stem.with_suffix(suffix).exists()]
    if missing:
        raise ValueError(f"Zipped shapefile is missing required sidecars: {', '.join(missing)}")
    return shapefiles[0]

def detect_continent(aoi_path: Path) -> tuple[str, dict[str, int]]:
    frame = gpd.read_file(aoi_path)
    if frame.empty or frame.crs is None:
        raise ValueError('AOI must contain geometry and have a defined CRS')
    frame = frame.to_crs('EPSG:4326')
    mask = frame.geometry.union_all()
    counts = {}
    for continent in CONTINENTS:
        counts[continent] = len(select_layer(mask, f'{DEFAULT_REFERENCE_ROOT}/reaches/{continent}.fgb'))
    selected = max(counts, key=counts.get)
    if counts[selected] == 0:
        raise ValueError('AOI does not intersect any SWORD v17b reaches')
    return (selected, counts)

def _archive(container: ContainerClient, submission: dict[str, Any], outcome: str, receipt: dict[str, Any]) -> None:
    base = (COMPLETED_PREFIX if outcome == 'completed' else FAILED_PREFIX) + submission['submission_id'] + '/'
    for source in (submission['metadata_blob'], submission['aoi_blob']):
        _copy_bytes(container, source, base + PurePosixPath(source).name)
    _upload_json(container, base + 'receipt.json', receipt)
    for source in (submission['metadata_blob'], submission['aoi_blob']):
        container.get_blob_client(source).delete_blob()

def provision_submission(*, descriptor: dict[str, str], workers: int=8, timeout: int=60, retries: int=5, connection_string_env: str='AZURE_STORAGE_CONNECTION_STRING', ckan_api_key: str | None=None) -> dict[str, Any]:
    container = get_container(connection_string_env)
    metadata_blob = safe_blob(descriptor['metadata_blob'], PENDING_PREFIX)
    payload = json.loads(container.get_blob_client(metadata_blob).download_blob().readall())
    payload['metadata_blob'] = metadata_blob
    submission = validate_submission(payload, metadata_blob)
    marker = PROCESSING_PREFIX + submission['submission_id'] + '.json'
    try:
        _upload_json(container, marker, {'submission_id': submission['submission_id'], 'region_id': submission['region_id'], 'started_utc': utc_now()}, overwrite=False)
    except ResourceExistsError as exc:
        raise RuntimeError(f'Submission {submission["submission_id"]} is already being processed') from exc
    try:
        with tempfile.TemporaryDirectory(prefix=f"swot-{submission['submission_id']}-") as temp:
            temp_path = Path(temp)
            downloaded = temp_path / PurePosixPath(submission['aoi_blob']).name
            _download(container, submission['aoi_blob'], downloaded)
            aoi_path = _extract_aoi(downloaded, temp_path)
            continent_counts = None
            continent = 'GLOBAL' if submission['continent'] == 'AUTO' else submission['continent']
            output_root = temp_path / 'output'
            historical = run_historical_aoi_pipeline(aoi=str(aoi_path), region_id=submission['region_id'], display_name=submission['display_name'], continent=continent, output_root=str(output_root), start=submission['historical_start'], workers=workers, timeout=timeout, retries=retries, upload=True, overwrite_azure=True, register_manifest=False, connection_string_env=connection_string_env)
            root = output_root / submission['region_id'] / 'historical'
            key = (ckan_api_key or runtime_secret('IHP_WINS_CKAN_API_KEY') or runtime_secret('CKAN_API_KEY')).strip()
            if not key:
                raise RuntimeError('CKAN_API_KEY is required to create and publish the destination dataset')
            ckan_session = requests.Session()
            ckan_session.headers.update({'Authorization': key, 'X-CKAN-API-Key': key})
            destination = ensure_destination_dataset(ckan_session, submission)
            publication = publish_region(root, submission['display_name'], submission['region_id'], destination['id'], ckan_api_key=key, documentation_source=submission.get('documentation_source'))
            publication['dataset_id'] = destination['id']
            publication['dataset_name'] = destination['name']
            receipt = {'schema_version': 1, 'status': 'completed', 'submission_id': submission['submission_id'], 'region_id': submission['region_id'], 'completed_utc': utc_now(), 'continent': continent, 'continent_reach_counts': continent_counts, 'historical': historical, 'publication': publication}
            source_intake = submission.get('source_intake') or {}
            if source_intake.get('resource_id'):
                _upload_json(container, f"incoming/registry/{source_intake['resource_id']}.json", {
                    'schema_version': 1,
                    'source_dataset_id': source_intake.get('dataset_id'),
                    'source_resource_id': source_intake['resource_id'],
                    'source_sha256': source_intake.get('sha256'),
                    'source_last_modified': source_intake.get('last_modified'),
                    'display_name': submission['display_name'],
                    'region_id': submission['region_id'],
                    'destination_dataset_name': destination['name'],
                    'destination_dataset_id': destination['id'],
                    'completed_utc': receipt['completed_utc'],
                })
            _archive(container, submission, 'completed', receipt)
            container.get_blob_client(marker).delete_blob()
            return receipt
    except Exception as exc:
        receipt = {'schema_version': 1, 'status': 'failed', 'submission_id': submission['submission_id'], 'region_id': submission['region_id'], 'failed_utc': utc_now(), 'error_type': type(exc).__name__, 'error': str(exc)[:4000]}
        # Preserve the pending AOI and metadata so an Airflow retry or a later
        # scheduled run can resume.  The failure receipt is diagnostic history,
        # not a destructive dead-letter move.
        failed_base = FAILED_PREFIX + submission['submission_id'] + '/'
        _upload_json(container, failed_base + f'receipt-{datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}.json', receipt)
        container.get_blob_client(marker).delete_blob()
        raise
