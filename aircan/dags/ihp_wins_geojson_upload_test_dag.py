"""Create a tiny GeoJSON resource in IHP-WINS."""

from __future__ import annotations

import io
import hashlib
import json
from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.models import Variable


@dag(
    dag_id="ihp_wins_geojson_upload_test",
    schedule_interval=None,
    start_date=datetime(2026, 9, 9),
    catchup=False,
    tags=["ihp-wins", "ckan", "test"],
)
def ihp_wins_geojson_upload_test():
    @task
    def upload_geojson() -> dict:
        stored_key = str(Variable.get("CKAN_API_KEY"))
        api_key = stored_key.strip()
        if api_key.startswith('"') and api_key.endswith('"'):
            api_key = api_key[1:-1]

        key_details = {
            "stored_length": len(stored_key),
            "sent_length": len(api_key),
            "sha256_prefix": hashlib.sha256(api_key.encode()).hexdigest()[:16],
            "whitespace_removed": stored_key != stored_key.strip(),
            "quotes_removed": stored_key.strip() != api_key,
        }
        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "Airflow upload test"},
                    "geometry": {
                        "type": "Point",
                        "coordinates": [-105.0, 40.0],
                    },
                }
            ],
        }
        content = json.dumps(geojson).encode("utf-8")

        try:
            response = requests.post(
                "https://ihp-wins.unesco.org/api/3/action/resource_create",
                headers={"Authorization": api_key, "X-CKAN-API-Key": api_key},
                data={
                    "package_id": "swot-sword-regional-observations",
                    "name": "Airflow GeoJSON upload test",
                    "format": "GeoJSON",
                },
                files={
                    "upload": (
                        "airflow_upload_test.geojson",
                        io.BytesIO(content),
                        "application/geo+json",
                    )
                },
                timeout=60,
            )
            return {
                "key": key_details,
                "status_code": response.status_code,
                "response": response.text,
            }
        except requests.RequestException as error:
            return {
                "key": key_details,
                "connection_error": f"{type(error).__name__}: {error}",
            }

    upload_geojson()


dag = ihp_wins_geojson_upload_test()
