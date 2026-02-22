"""
This script will be automated to run at a set interval to append new Hydrocron RiverSP rows
into existing per-reach CSVs.

Key behavior:
- For each reach, start_time = (last time in existing CSV) - overlap
- end_time = now UTC (or end of provided --day)

Statuses:
- appended   (CSV changed)
- no_change  (CSV unchanged)
"""

import os, json, time, random, argparse
from io import StringIO
from datetime import datetime, timezone, date, timedelta

import pandas as pd
import geopandas as gpd
import requests


# ---- CONFIG ----
GPKG_PATH = r"./dnipro_sword_reaches_clip.gpkg"
LAYER_NAME = "dnipro_reaches"
REACH_ID_FIELD = "reach_id"

OUT_DIR = r"./hydrocron_timeseries_by_reach"

HYDROCRON_URL = "https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries"
COLLECTION_NAME = "SWOT_L2_HR_RiverSP_2.0"
FIELDS = "reach_id,time_str,cycle_id,pass_id,wse,slope,width,dschg_gm,dschg_gm_q,reach_q"

TIMEOUT_S = 180
SLEEP_BETWEEN_REQUESTS_S = (0.2, 0.6)
MAX_RETRIES = 5

# overlap so we don't miss late/edge timestamps; also helps de-dupe
OVERLAP_HOURS = 48

# if a reach CSV is empty/nonexistent, only backfill this far by default
DEFAULT_BACKFILL_DAYS_IF_EMPTY = 60


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
        return pd.read_csv(StringIO(csv_text)) if csv_text else pd.DataFrame()
    return pd.read_csv(StringIO(text))


def request_with_retries(url: str, params: dict) -> requests.Response:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT_S)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code}")
            return r
        except Exception as e:
            last_err = e
            backoff = min(30, (2 ** (attempt - 1)) * 0.7) + random.uniform(0, 0.5)
            time.sleep(backoff)
    raise RuntimeError(f"Failed after {MAX_RETRIES} retries. Last error: {last_err}")


def safe_reach_filename(reach_id: str) -> str:
    s = str(reach_id).strip()
    cleaned = "".join(ch for ch in s if ch.isalnum() or ch in ("-", "_"))
    return cleaned or "unknown_reach"


def load_reach_ids() -> list[str]:
    gdf = gpd.read_file(GPKG_PATH, layer=LAYER_NAME)
    if REACH_ID_FIELD not in gdf.columns:
        raise ValueError(f"Field '{REACH_ID_FIELD}' not found in layer '{LAYER_NAME}'")
    s = gdf[REACH_ID_FIELD].dropna().astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    return s.loc[s != ""].unique().tolist()


def parse_day(day_str: str | None) -> date | None:
    """
    If provided, day_str is YYYY-MM-DD and used to set end_time to end-of-day UTC.
    If None, we use end_time = now UTC.
    """
    if day_str is None:
        return None
    return date.fromisoformat(day_str)


def read_existing_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def get_last_time_dt(existing: pd.DataFrame) -> datetime | None:
    """
    Return latest timestamp in existing CSV as tz-aware UTC datetime.
    Robust to empty files / missing time_str / odd formats.
    """
    if existing is None or len(existing) == 0 or "time_str" not in existing.columns:
        return None

    s = existing["time_str"].astype(str).str.strip()
    s = s[(s != "") & (s.str.lower() != "nan")]

    if len(s) == 0:
        return None

    ts = pd.to_datetime(s, format="%Y-%m-%dT%H:%M:%SZ", errors="coerce", utc=True)

    if ts.isna().any():
        ts2 = pd.to_datetime(s[ts.isna()], errors="coerce", utc=True)
        ts.loc[ts.isna()] = ts2

    if ts.notna().any():
        return ts.max().to_pydatetime()

    return None


def normalize_new_df(df: pd.DataFrame, rid: str) -> pd.DataFrame:
    """
    Make sure new rows are comparable/dedupable:
    - ensure reach_id exists
    - normalize time_str to ISO Z
    - drop bad time rows
    """
    if df is None or len(df) == 0:
        return pd.DataFrame()

    df = df.copy()

    if "reach_id" not in df.columns:
        df["reach_id"] = rid

    if "time_str" in df.columns:
        t = pd.to_datetime(df["time_str"], errors="coerce", utc=True)
        df = df.loc[t.notna()].copy()
        # Keep alignment safe even if rows were dropped
        df["time_str"] = t.loc[t.notna()].dt.strftime("%Y-%m-%dT%H:%M:%SZ").values

    return df


def append_dedup(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    if existing is None or len(existing) == 0:
        out = new.copy()
    elif new is None or len(new) == 0:
        out = existing.copy()
    else:
        out = pd.concat([existing, new], ignore_index=True, sort=False)

    if "time_str" in out.columns:
        out["time_str"] = out["time_str"].astype(str)
        out = out.drop_duplicates(subset=["time_str"], keep="last")
        out = out.sort_values("time_str")

    return out


def main(day_override: str | None = None):
    # end_time: now UTC by default; or end-of-day UTC if --day is given
    day = parse_day(day_override)
    if day is None:
        end_dt = datetime.now(timezone.utc)
    else:
        end_dt = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc)

    reach_ids = load_reach_ids()
    os.makedirs(OUT_DIR, exist_ok=True)

    print("Update (append since last timestamp → end time)")
    print("Reaches:", len(reach_ids))
    print("End time:", end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"))

    log = []
    appended_count = 0
    no_change_count = 0
    error_count = 0

    for i, rid in enumerate(reach_ids, 1):
        out_csv = os.path.join(OUT_DIR, f"reach_{safe_reach_filename(rid)}.csv")
        existing = read_existing_csv(out_csv)

        last_dt = get_last_time_dt(existing)

        if last_dt is not None:
            start_dt = last_dt - timedelta(hours=OVERLAP_HOURS)
        else:
            start_dt = end_dt - timedelta(days=DEFAULT_BACKFILL_DAYS_IF_EMPTY)

        if start_dt >= end_dt:
            start_dt = end_dt - timedelta(days=1)

        params = {
            "feature": "Reach",
            "feature_id": rid,
            "start_time": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_time": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "output": "csv",
            "collection_name": COLLECTION_NAME,
            "fields": FIELDS,
        }

        try:
            time.sleep(random.uniform(*SLEEP_BETWEEN_REQUESTS_S))
            r = request_with_retries(HYDROCRON_URL, params)
            new_df = hydrocron_response_to_df(r.text)
            new_df = normalize_new_df(new_df, rid)

            # --- Unified "no change" block ---
            # no rows came back OR nothing in the response is new vs existing time_str values
            if new_df is None or len(new_df) == 0:
                status = "no_change"
                no_change_count += 1
                log.append({"reach_id": rid, "status": status, "start": params["start_time"], "end": params["end_time"],
                            "new_rows": 0, "total_rows": len(existing)})
                print(f"[{i}/{len(reach_ids)}] {rid} no change (0 rows returned)")
                continue

            if existing is not None and len(existing) > 0 and "time_str" in existing.columns and "time_str" in new_df.columns:
                existing_times = set(existing["time_str"].astype(str).tolist())
                incoming_times = set(new_df["time_str"].astype(str).tolist())
                truly_new = incoming_times - existing_times
                if len(truly_new) == 0:
                    status = "no_change"
                    no_change_count += 1
                    log.append({"reach_id": rid, "status": status, "start": params["start_time"], "end": params["end_time"],
                                "new_rows": 0, "total_rows": len(existing)})
                    print(f"[{i}/{len(reach_ids)}] {rid} no change (all timestamps already present)")
                    continue

            # Otherwise, append + dedup + write
            out_df = append_dedup(existing, new_df)
            out_df.to_csv(out_csv, index=False)

            appended_count += 1
            log.append({"reach_id": rid, "status": "appended", "start": params["start_time"], "end": params["end_time"],
                        "new_rows": len(new_df), "total_rows": len(out_df)})
            print(f"[{i}/{len(reach_ids)}] {rid} appended new={len(new_df)} total={len(out_df)}")

        except Exception as e:
            error_count += 1
            log.append({"reach_id": rid, "status": f"error: {e}", "start": params["start_time"], "end": params["end_time"],
                        "new_rows": None, "total_rows": None})
            print(f"[{i}/{len(reach_ids)}] {rid} ERROR: {e}")

    # --- Final summary ---
    print("\nSUMMARY")
    print(f"  appended : {appended_count}")
    print(f"  no change: {no_change_count}")
    print(f"  errors   : {error_count}")
    print("Done.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--day", default=None, help="Optional end day (UTC) YYYY-MM-DD. If omitted, end_time = now UTC.")
    args = p.parse_known_args()[0]  # Jupyter-safe
    main(day_override=args.day)
