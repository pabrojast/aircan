from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
import pandas as pd
from time import sleep
import re
from datetime import datetime, timedelta
import pytz
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
def get_stations(url = STATIONS_URL):
    params = {"output": "json"}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching stations:", response.status_code)
        return None
# Write each group of stations into a separate JSON file
def save_stations_by_group(stations):
    stations_by_mngorg = defaultdict(list)
    for station in stations['list']:
        stations_by_mngorg[station['mngorg']].append(station)
        
    output_files = {}
    
    for mngorg, stations_group in stations_by_mngorg.items():
        # Construct the filename using the managing organization name
        filename = f"{mngorg}_stations.json"
        
        # Write the station data to the file
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(stations_group, f, ensure_ascii=False, indent=4)
        
        # Store the filename in the output dictionary
        output_files[mngorg] = filename
    
    print("Station groups saved in the following files:", output_files)
    return output_files

# Function to get station info
def get_station_info(station_code):
    params = {"obscd": station_code, "output": "json"}
    response = requests.get(STATION_INFO_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching station info for {station_code}:", response.status_code)
        return None

# Function to get hourly precipitation data
def get_hourly_precipitation(station_code, start_date, end_date):
    params = {"obscd": station_code, "startdt": start_date, "enddt": end_date, "output": "json"}
    response = requests.get(HOURLY_PRECIPITATION_URL, params=params)
    return response.json() if response.status_code == 200 else 'ERROR'

# Function to get daily precipitation data
def get_daily_precipitation(station_code, start_date, end_date):
    params = {"obscd": station_code, "startdt": start_date, "enddt": end_date, "output": "json"}
    response = requests.get(DAILY_PRECIPITATION_URL, params=params)
    return response.json() if response.status_code == 200 else 'ERROR'

# Function to transform lat/lon from DMS to decimal
def dms_to_dd(dms_str):
    matches = re.match(r"(\d+)-(\d+)-(\d+)", dms_str)
    if matches:
        degrees, minutes, seconds = map(int, matches.groups())
        return degrees + minutes / 60 + seconds / 3600
    return float(dms_str)

# Function to convert coordinates
def convert_coordinates(coord):
    lat_dd = dms_to_dd(coord['lat'])
    lon_dd = dms_to_dd(coord['lon'])
    return {'lat': lat_dd, 'lon': lon_dd}

# Function to get the time range in Korean time
def get_korean_time_range(hours_back=72):
    korean_timezone = pytz.timezone('Asia/Seoul')
    now_korea = datetime.now(korean_timezone)
    past_korea = now_korea - timedelta(hours=hours_back)
    return past_korea.strftime('%Y%m%d'), now_korea.strftime('%Y%m%d')

# Function to attempt parsing date and time
def try_parse_h(date):
    try:
        return pd.to_datetime(date, format='%Y%m%d%H', errors='coerce').strftime('%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        return None

# Retry function for resilience
def retry(func, max_attempts=3, sleep_time=5):
    attempts = 0
    while attempts < max_attempts:
        try:
            return func()
        except Exception as e:
            print(f"Error: {e}. Retrying...")
            sleep(sleep_time)
            attempts += 1
    raise Exception(f"Failed after {max_attempts} attempts.")

# Function to process and save hourly precipitation data
def process_hourly_precipitation():
    stations = get_stations()
    all_precipitation_data = pd.DataFrame()
    start_date, end_date = get_korean_time_range()

    for station in stations['list']:
        station_code = station['obscd']
        station_name = station['obsnm']
        
        def get_station_data():
            return get_station_info(station_code)

        station_info = retry(get_station_data)
        
        if station_info['result'].get('code') == 'success' and '해당 데이터가 없습니다.' not in station_info['result'].get('msg'):
            coords = {'lat': station_info['list'][0]['lat'], 'lon': station_info['list'][0]['lon']}
            converted_coords = convert_coordinates(coords)
            station_lat = converted_coords['lat']
            station_lon = converted_coords['lon']
            
            print(f"Downloading hourly data for station: {station_name} ({station_code})")
            
            def get_data():
                return get_hourly_precipitation(station_code, start_date, end_date)

            hourly_data = retry(get_data)

            if hourly_data != 'ERROR' and 'list' in hourly_data:
                station_data = pd.DataFrame(hourly_data['list'])
                station_data['station_code'] = station_code
                station_data['station_name'] = station_name
                station_data['latitude'] = station_lat
                station_data['longitude'] = station_lon
                all_precipitation_data = pd.concat([all_precipitation_data, station_data], ignore_index=True)

    all_precipitation_data['time'] = all_precipitation_data['ymdh'].apply(try_parse_h)
    all_precipitation_data.to_csv("hourly_precipitation_data.csv", index=False)
    print("Hourly precipitation data saved.")

# Function to process and save daily precipitation data
def process_daily_precipitation():
    stations = get_stations()
    all_precipitation_data = pd.DataFrame()
    start_date, end_date = get_korean_time_range(hours_back=2160)

    for station in stations['list']:
        station_code = station['obscd']
        station_name = station['obsnm']

        def get_station_data():
            return get_station_info(station_code)

        station_info = retry(get_station_data)

        if station_info['result'].get('code') == 'success' and '해당 데이터가 없습니다.' not in station_info['result'].get('msg'):
            coords = {'lat': station_info['list'][0]['lat'], 'lon': station_info['list'][0]['lon']}
            converted_coords = convert_coordinates(coords)
            station_lat = converted_coords['lat']
            station_lon = converted_coords['lon']

            print(f"Downloading daily data for station: {station_name} ({station_code})")

            def get_data():
                return get_daily_precipitation(station_code, start_date, end_date)

            daily_data = retry(get_data)

            if daily_data != 'ERROR' and 'list' in daily_data:
                station_data = pd.DataFrame(daily_data['list'])
                station_data['station_code'] = station_code
                station_data['station_name'] = station_name
                station_data['latitude'] = station_lat
                station_data['longitude'] = station_lon
                all_precipitation_data = pd.concat([all_precipitation_data, station_data], ignore_index=True)

    all_precipitation_data['time'] = pd.to_datetime(all_precipitation_data['ymd'], format='%Y%m%d', errors='coerce')
    all_precipitation_data.to_csv("daily_precipitation_data.csv", index=False)
    print("Daily precipitation data saved.")

# Define the hourly DAG
hourly_dag = DAG(
    'hourly_precipitation_data',
    default_args=default_args,
    description='A DAG to fetch hourly precipitation data',
    schedule_interval=None,
)

# Define the daily DAG
daily_dag = DAG(
    'daily_precipitation_data',
    default_args=default_args,
    description='A DAG to fetch daily precipitation data',
    schedule_interval=None,
)

# Hourly tasks
fetch_hourly_data = PythonOperator(
    task_id='process_hourly_precipitation',
    python_callable=process_hourly_precipitation,
    dag=hourly_dag,
)

# Daily tasks
fetch_daily_data = PythonOperator(
    task_id='process_daily_precipitation',
    python_callable=process_daily_precipitation,
    dag=daily_dag,
)
