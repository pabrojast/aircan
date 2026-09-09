"""Read selected DAWG rows from the validated Azure cache, only on revision change."""
from __future__ import annotations

import hashlib
import shutil
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

# SWORD uses HydroBASINS codes; Siberia shares AS, Arctic/Greenland share NA.
CONTINENT_BY_DIGIT = dict(zip('123456789', ('AF', 'EU', 'AS', 'AS', 'OC', 'SA', 'NA', 'NA', 'NA')))


def extract_rows(path, reach_ids):
    # netCDF access is deliberately serial, before HTTP worker threads start.
    import netCDF4
    rows = {}
    with netCDF4.Dataset(path) as dataset:
        ids = dataset.groups['reaches'].variables['reach_id'][:]
        index = {str(int(value)): i for i, value in enumerate(ids)}
        group = dataset.groups['consensus'].variables
        for rid in reach_ids:
            if rid not in index:
                continue
            i = index[rid]
            times = np.ma.asarray(group['time_int'][i], dtype=float).filled(np.nan)
            values = np.ma.asarray(group['consensus_q'][i], dtype=float).filled(np.nan)
            valid = np.isfinite(times) & (times >= 0) & np.isfinite(values) & (values > -1e9)
            dates = pd.to_datetime(times[valid].astype('int64'), unit='s', origin='2000-01-01', utc=True)
            rows[rid] = pd.DataFrame({'time_utc': dates.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                      'consensus_q': values[valid]})
    return rows


def refresh_rows(container, pointer, previous_revisions, reach_ids):
    """Never contact NASA here; consume only immutable validated Azure objects.

    One continental file occupies disk at a time. Nothing is checkpointed by
    this function: the caller commits returned revisions after publication.
    """
    groups = {}
    for rid in reach_ids:
        continent = CONTINENT_BY_DIGIT.get(rid[:1])
        if not continent:
            continue
        groups.setdefault(continent, []).append(rid)
    rows, revisions, missing = {}, dict(previous_revisions), []
    for continent, ids in sorted(groups.items()):
        metadata = (pointer or {}).get('continents', {}).get(continent)
        if not metadata:
            missing.append(continent)
            continue
        checksum = str(metadata.get('sha256') or '')
        source = str(metadata.get('source_blob') or '')
        if (len(checksum) != 64 or not metadata.get('validated_utc')
                or not source.startswith('reference/dawg/') or '..' in source.split('/')):
            raise ValueError(f'Invalid validated DAWG pointer for {continent}')
        if previous_revisions.get(continent) == checksum:
            continue
        with tempfile.TemporaryDirectory(prefix='swot-dawg-') as directory:
            size = container.get_blob_client(source).get_blob_properties().size
            if shutil.disk_usage(directory).free < size + 256 * 1024**2:
                raise RuntimeError(f'Insufficient temporary disk for {continent} DAWG ({size} bytes)')
            path = Path(directory) / 'source.nc'
            digest = hashlib.sha256()
            with path.open('wb') as output:
                for chunk in container.get_blob_client(source).download_blob().chunks():
                    digest.update(chunk)
                    output.write(chunk)
            if digest.hexdigest() != checksum:
                raise ValueError(f'DAWG checksum mismatch for {continent}')
            rows.update(extract_rows(path, ids))
        revisions[continent] = checksum
    return rows, revisions, missing
