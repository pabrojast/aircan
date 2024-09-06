from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pytz
import requests
import pandas as pd
import json
from collections import defaultdict

# API URLs
STATIONS_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_dubrfobs"
STATION_INFO_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_obsinfo"
HOURLY_PRECIPITATION_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_hrdata"
DAILY_PRECIPITATION_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_dtdata"

# Default arguments for the DAGs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to get stations list
def get_stations(**context):
    params = {"output": "json"}
    response = requests.get(STATIONS_URL, params=params)
    if response.status_code == 200:
        stations = response.json()
        context['ti'].xcom_push(key='stations', value=stations)  # Push stations to XCom
    else:
        print("Error fetching stations:", response.status_code)
        return None

# Save stations by group
def save_stations_by_group(**context):
    stations = context['ti'].xcom_pull(key='stations', task_ids='get_stations_task')
    stations_by_mngorg = defaultdict(list)
    for station in stations['list']:
        stations_by_mngorg[station['mngorg']].append(station)
        
    for mngorg, stations_group in stations_by_mngorg.items():
        filename = f"/path/to/save/{mngorg}_stations.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(stations_group, f, ensure_ascii=False, indent=4)

# Process hourly precipitation data
def process_hourly_precipitation(**context):
    stations = context['ti'].xcom_pull(key='stations', task_ids='get_stations_task')
    all_precipitation_data = pd.DataFrame()
    start_date, end_date = get_korean_time_range()

    for station in stations['list']:
        station_code = station['obscd']
        station_name = station['obsnm']
        hourly_data = get_hourly_precipitation(station_code, start_date, end_date)

        if hourly_data != 'ERROR' and 'list' in hourly_data:
            station_data = pd.DataFrame(hourly_data['list'])
            all_precipitation_data = pd.concat([all_precipitation_data, station_data], ignore_index=True)

    all_precipitation_data.to_csv("/path/to/save/hourly_precipitation_data.csv", index=False)

# Process daily precipitation data
def process_daily_precipitation(**context):
    stations = context['ti'].xcom_pull(key='stations', task_ids='get_stations_task')
    all_precipitation_data = pd.DataFrame()
    start_date, end_date = get_korean_time_range(hours_back=2160)

    for station in stations['list']:
        station_code = station['obscd']
        station_name = station['obsnm']
        daily_data = get_daily_precipitation(station_code, start_date, end_date)

        if daily_data != 'ERROR' and 'list' in daily_data:
            station_data = pd.DataFrame(daily_data['list'])
            all_precipitation_data = pd.concat([all_precipitation_data, station_data], ignore_index=True)

    all_precipitation_data.to_csv("/path/to/save/daily_precipitation_data.csv", index=False)

# Helper functions for precipitation data
def get_hourly_precipitation(station_code, start_date, end_date):
    params = {"obscd": station_code, "startdt": start_date, "enddt": end_date, "output": "json"}
    response = requests.get(HOURLY_PRECIPITATION_URL, params=params)
    return response.json() if response.status_code == 200 else 'ERROR'

def get_daily_precipitation(station_code, start_date, end_date):
    params = {"obscd": station_code, "startdt": start_date, "enddt": end_date, "output": "json"}
    response = requests.get(DAILY_PRECIPITATION_URL, params=params)
    return response.json() if response.status_code == 200 else 'ERROR'

def get_korean_time_range(hours_back=72):
    korean_timezone = pytz.timezone('Asia/Seoul')
    now_korea = datetime.now(korean_timezone)
    past_korea = now_korea - timedelta(hours=hours_back)
    return past_korea.strftime('%Y%m%d'), now_korea.strftime('%Y%m%d')

# Define the hourly DAG
hourly_dag = DAG(
    'hourly_precipitation_data',
    default_args=default_args,
    description='A DAG to fetch hourly precipitation data',
    schedule_interval='@hourly',
)

# Define the daily DAG
daily_dag = DAG(
    'daily_precipitation_data',
    default_args=default_args,
    description='A DAG to fetch daily precipitation data',
    schedule_interval='@daily',
)

# Get stations task
get_stations_task = PythonOperator(
    task_id='get_stations_task',
    python_callable=get_stations,
    provide_context=True,
    dag=hourly_dag,
)

# Save stations task (only in daily DAG)
save_stations_task = PythonOperator(
    task_id='save_stations_by_group',
    python_callable=save_stations_by_group,
    provide_context=True,
    dag=daily_dag,
)

# Hourly data task
process_hourly_data_task = PythonOperator(
    task_id='process_hourly_precipitation',
    python_callable=process_hourly_precipitation,
    provide_context=True,
    dag=hourly_dag,
)

# Daily data task
process_daily_data_task = PythonOperator(
    task_id='process_daily_precipitation',
    python_callable=process_daily_precipitation,
    provide_context=True,
    dag=daily_dag,
)

# Set dependencies
get_stations_task >> process_hourly_data_task
get_stations_task >> save_stations_task >> process_daily_data_task
