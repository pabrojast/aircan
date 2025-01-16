import pandas as pd
import requests
import json
import time
import random
import requests
import json
import pandas as pd
from time import sleep
import re
from datetime import datetime, timedelta
import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# PRECIPITATION API URLs
STATIONS_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_dubrfobs"
STATION_INFO_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_obsinfo"
HOURLY_PRECIPITATION_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_hrdata"
DAILY_PRECIPITATION_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_dtdata"
MONTHLY_PRECIPITATION_URL =  "http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_mndata"

#list of stations
def get_stations(urlstation = STATIONS_URL):
    params = {"output": "json"}
    response = requests.get(urlstation, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching stations:", response.status_code)
        return None
#korean time   
def get_korean_time_range(hours_back=72):
    korean_timezone = pytz.timezone('Asia/Seoul')
    now_korea = datetime.now(korean_timezone)
    past_korea = now_korea - timedelta(hours=hours_back)
    return past_korea.strftime('%Y%m%d'), now_korea.strftime('%Y%m%d')

#metadata
def get_station_info(station_code, stationInfoUrl = STATION_INFO_URL):
    params = {"obscd": station_code, "output": "json"}
    response = requests.get(stationInfoUrl, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching station info for {station_code}:", response.status_code)
        return None
#data 
def get_daily_precipitation(station_code, start_date, end_date):
    params = {"obscd": station_code, "startdt": start_date, "enddt": end_date, "output": "json"}
    response = requests.get(DAILY_PRECIPITATION_URL, params=params)
    return response.json() if response.status_code == 200 else 'ERROR'

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

# Function to transform lat/lon from DMS to decimal
def dms_to_dd(dms_str):
    if not dms_str or dms_str.strip() == "":
        return None  # Return None if the input is empty or invalid

    # Check if the input is already a decimal number
    try:
        decimal_value = float(dms_str)
        return decimal_value  # If it's a valid decimal, return it as is
    except ValueError:
        pass  # Continue if it's not a valid decimal

    # If not decimal, attempt to convert from DMS format
    matches = re.match(r"(\d+)-(\d+)-(\d+)", dms_str.strip())
    if matches:
        degrees, minutes, seconds = matches.groups()

        # Ensure degrees, minutes, and seconds are valid numbers before converting
        try:
            degrees = int(degrees)
            minutes = int(minutes)
            seconds = int(seconds)
            return degrees + minutes / 60 + seconds / 3600
        except ValueError:
            return None  # If conversion fails, return None
    
    return None  # Return None if the DMS format is incorrect or empty

# Function to convert coordinates
def convert_coordinates(coord):
    lat = coord['lat']
    lon = coord['lon']
    if lat is not None:
        lat_dd = dms_to_dd(coord['lat'])
    else:
        lat_dd = None
    if lon is not None:
        lon_dd = dms_to_dd(coord['lon'])
    else:
        lon_dd = None    
    return {'lat': lat_dd, 'lon': lon_dd}


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
                station_data['id'] = station_code
                station_data['station_name'] = station_name
                station_data['latitude'] = station_lat
                station_data['longitude'] = station_lon
                station_data['obsnmeng'] = station_info['list'][0].get('obsnmeng')
                station_data['bbsnnm'] = station_info['list'][0].get('bbsnnm')
                station_data['obsknd'] = station_info['list'][0].get('obsknd')
                station_data['addr'] = station_info['list'][0].get('addr')
                station_data['shgt'] = station_info['list'][0].get('shgt')
                station_data['hrdtstart'] = station_info['list'][0].get('hrdtstart')
                station_data['dydtend'] = station_info['list'][0].get('dydtend')
                station_data['mngorg'] = station_info['list'][0].get('mngorg')
                station_data['sbsncd'] = station_info['list'][0].get('sbsncd')
                station_data['opendt'] = station_info['list'][0].get('opendt')
                station_data['shgt'] = station_info['list'][0].get('shgt')
                station_data['hrdtend'] = station_info['list'][0].get('hrdtend')
                station_data['dydtstart'] = station_info['list'][0].get('dydtstart')
                all_precipitation_data = pd.concat([all_precipitation_data, station_data], ignore_index=True)

    all_precipitation_data['time'] = pd.to_datetime(all_precipitation_data['ymd'], format='%Y%m%d', errors='coerce')
    # Just in Case, check column tipe
    # all_precipitation_data['rf'] = all_precipitation_data['rf'].astype(str)
    # Change "-" to ""
    all_precipitation_data.loc[all_precipitation_data['rf'] == "-", 'rf'] = ""
    all_precipitation_data.to_csv("daily_precipitation_data.csv", index=False)

    print("Daily precipitation data saved.")
    print("Uploading data to IHP-WINS")
    upload_ckan(name='Testing Daily Precipitation', description='Test Data', url='https://data.dev-wins.com/api/action/resource_patch', pathtofile="daily_precipitation_data.csv", resource_id="7bc71960-6a9f-4189-94f1-6db211a912d6", package_id="702b0105-d9f7-4747-ba42-8fa2f36ae086")
    print("Upload done")
    
#process_daily_precipitation()


APIdev = Variable.get("APIDEV")

def upload_ckan(name='Daily Water Level Data', description='Test Data', url='https://data.dev-wins.com/api/action/resource_patch', pathtofile="daily_wl_data.csv", resource_id="XXXXXX", package_id="YYYYYY"):
    files = {'upload': open(pathtofile, 'rb')}
    headers = {"API-Key": APIdev}
    data_dict = {
        'id': resource_id,
        'package_id': package_id,
        'format': 'CSV',
        'name': name,
        'description': description
    }
    # POST request
    response = requests.post(url, headers=headers, data=data_dict, files=files)
    
    # Logging
    print("Status Code", response.status_code)
    print("JSON Response", response.json())
 

# Default arguments for the DAGs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the hourly DAG
hourly_dag = DAG(
    'Example Test Precipitation',
    default_args=default_args,
    description='A DAG to fetch hourly precipitation data as Example',
    schedule_interval="0 */3 * * *",
    catchup=False,
)

fetch_daily_data = PythonOperator(
    task_id='Example Test Precipitation',
    python_callable=process_daily_precipitation,
    dag=hourly_dag,
)