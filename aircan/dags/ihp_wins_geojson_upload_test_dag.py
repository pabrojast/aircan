"""Create a tiny GeoJSON resource in IHP-WINS."""

from __future__ import annotations

import io
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
        api_key = Variable.get("CKAN_API_KEY")
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

        response = requests.post(
            "https://ihp-wins.unesco.org/api/3/action/resource_create",
            headers={"Authorization": api_key},
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
        response.raise_for_status()
        return response.json()

    upload_geojson()


dag = ihp_wins_geojson_upload_test()
