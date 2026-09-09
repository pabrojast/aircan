"""Test Airflow access to the Dnipro IHP-WINS resource."""

import hashlib
from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.models import Variable


@dag(
    dag_id="ihp_wins_resource_access_test",
    schedule_interval=None,
    start_date=datetime(2026, 9, 9),
    catchup=False,
    tags=["ihp-wins", "ckan", "test"],
)
def ihp_wins_resource_access_test():
    @task
    def show_resource() -> dict:
        api_key = str(Variable.get("CKAN_API_KEY")).strip()
        fingerprint = hashlib.sha256(api_key.encode()).hexdigest()[:16]

        try:
            response = requests.post(
                "https://ihp-wins.unesco.org/api/3/action/resource_show",
                headers={"Authorization": api_key, "X-CKAN-API-Key": api_key},
                data={"id": "95e713b5-1edb-43c8-9254-515fed7f6f27"},
                timeout=60,
            )
            return {
                "key_length": len(api_key),
                "key_sha256_prefix": fingerprint,
                "status_code": response.status_code,
                "response": response.text,
            }
        except requests.RequestException as error:
            return {
                "key_length": len(api_key),
                "key_sha256_prefix": fingerprint,
                "connection_error": f"{type(error).__name__}: {error}",
            }

    show_resource()


dag = ihp_wins_resource_access_test()
