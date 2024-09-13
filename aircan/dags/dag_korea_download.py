from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import requests
import json
import pandas as pd
from time import sleep
import re
from datetime import datetime, timedelta
import pytz
from collections import defaultdict

# API URLs
# PRECIPITATION API URLs
STATIONS_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_dubrfobs"
STATION_INFO_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_obsinfo"
HOURLY_PRECIPITATION_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_hrdata"
DAILY_PRECIPITATION_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_dtdata"
MONTHLY_PRECIPITATION_URL =  "http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_mndata"

# WATER LEVEL URL
WL_STATIONS_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/wl_dubwlobs"
WL_STATIONS_INFO_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/wl_obsinfo"
HOURLY_WL_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/wl_hrdata"
DAILY_WL_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/wl_dtdata"

# METEOROLOGICAL DATA URL
METEO_STATIONS_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/we_dwtwtobs"
METEO_STATION_INFO_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/we_obsinfo"
HOURLY_METEO_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/we_hrdata"
DAILY_METEO_URL = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/we_dtdata"

# FLOW RATE
FLOW_RATE_STATION_INFO = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/flw_dubobsif"
DAILY_FLOW_RATE = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/flw_dtdata"

# SUSPENDED SEDIMENT LOAD 
SSL_STATION_INFO = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/wkw_ardata"
SSL_STATION_DATA = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/wkw_qsvsrrslst"

#CKAN API
APIdev = Variable.get("APIDEV")

# Default arguments for the DAGs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Function to get stations list
def get_stations(urlstation = STATIONS_URL):
    params = {"output": "json"}
    response = requests.get(urlstation, params=params)
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
def get_station_info(station_code, stationInfoUrl = STATION_INFO_URL):
    params = {"obscd": station_code, "output": "json"}
    response = requests.get(stationInfoUrl, params=params)
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

# Function to get monthly precipitation data
def get_monthly_precipitation(station_code, start_date, end_date):
    params = {"obscd": station_code, "startdt": start_date, "enddt": end_date, "output": "json"}
    response = requests.get(MONTHLY_PRECIPITATION_URL, params=params)
    return response.json() if response.status_code == 200 else 'ERROR'

# Function to get hourly water level data
def get_hourly_water_level(station_code, start_date, end_date):
    params = {"obscd": station_code, "startdt": start_date, "enddt": end_date, "output": "json"}
    response = requests.get(HOURLY_WL_URL, params=params)
    return response.json() if response.status_code == 200 else 'ERROR'

# Function to get daily water level data
def get_daily_water_level(station_code, start_date, end_date):
    params = {"obscd": station_code, "startdt": start_date, "enddt": end_date, "output": "json"}
    response = requests.get(DAILY_WL_URL, params=params)
    return response.json() if response.status_code == 200 else 'ERROR'

# Function to get hourly meteorological data
def get_hourly_meteorological_data(station_code, start_date, end_date):
    params = {"obscd": station_code, "startdt": start_date, "enddt": end_date, "output": "json"}
    response = requests.get(HOURLY_METEO_URL, params=params)
    return response.json() if response.status_code == 200 else 'ERROR'

# Function to get daily meteorological data
def get_daily_meteorological_data(station_code, start_date, end_date):
    params = {"obscd": station_code, "startdt": start_date, "enddt": end_date, "output": "json"}
    response = requests.get(DAILY_METEO_URL, params=params)
    return response.json() if response.status_code == 200 else 'ERROR'

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

# Resource Patch (CKAN Upload)
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

    all_precipitation_data['time'] = all_precipitation_data['ymdh'].apply(try_parse_h)
    all_precipitation_data.loc[all_precipitation_data['rf'] == "-", 'rf'] = ""
    all_precipitation_data.to_csv("hourly_precipitation_data.csv", index=False)
    
    print("Hourly precipitation data saved.")
    print("Uploading data to IHP-WINS...")
    upload_ckan(name = 'Hourly Precipitation Data from the Last 3 Days', description = 'This is only for testing', url = 'https://data.dev-wins.com/api/action/resource_patch',  pathtofile = "hourly_precipitation_data.csv",  resource_id = "46c3c577-c847-47b1-a833-f56b24b0aac7", package_id = "9897691a-c6d4-416c-8d16-02e0e7db1a2f")
    print("Upload done")

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
    upload_ckan(name = 'Daily Precipitation Data for the Last 3 Months', description = 'This is only for testing', url = 'https://data.dev-wins.com/api/action/resource_patch',  pathtofile = "daily_precipitation_data.csv",  resource_id = "5a157f90-7a9a-4eee-9152-d5a084598a1c", package_id = "9897691a-c6d4-416c-8d16-02e0e7db1a2f")
    print("Upload done")

# Function to process and save daily precipitation data
def process_monthly_precipitation():
    stations = get_stations()
    all_precipitation_data = pd.DataFrame()
    start_date, end_date = get_korean_time_range(hours_back=26280)
    start_date = start_date[:4]
    end_date = end_date[:4]

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

            print(f"Downloading monthly data for station: {station_name} ({station_code})")

            def get_data():
                return get_monthly_precipitation(station_code, start_date, end_date)

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

    all_precipitation_data['time'] = pd.to_datetime(all_precipitation_data['ym'], format='%Y%m', errors='coerce')
    # Just in Case, check column tipe
    # all_precipitation_data['rf'] = all_precipitation_data['rf'].astype(str)
    # Change "-" to ""
    all_precipitation_data.loc[all_precipitation_data['dtrf'] == "-", 'dtrf'] = ""
    all_precipitation_data.to_csv("monthly_precipitation_data.csv", index=False)

    print("Monthly precipitation data saved.")
    print("Uploading data to IHP-WINS")
    upload_ckan(name = 'Monthly Precipitation Data for the Last 3 Years', description = 'This is only for testing', url = 'https://data.dev-wins.com/api/action/resource_patch',  pathtofile = "monthly_precipitation_data.csv",  resource_id = "dabc9b1b-d31a-4c62-a15b-746dff1f20d8", package_id = "9897691a-c6d4-416c-8d16-02e0e7db1a2f")
    print("Upload done")
    
# Function to process and save hourly water level data
def process_hourly_water_level():
    stations = get_stations(WL_STATIONS_URL)
    all_wl_data = pd.DataFrame()
    start_date, end_date = get_korean_time_range()

    for station in stations['list']:
        station_code = station['obscd']
        station_name = station['obsnm']
        
        def get_station_data():
            return get_station_info(station_code, stationInfoUrl=WL_STATIONS_INFO_URL)

        station_info = retry(get_station_data)

        if station_info['result'].get('code') == 'success' and '해당 데이터가 없습니다.' not in station_info['result'].get('msg'):
            coords = {'lat': station_info['list'][0]['lat'], 'lon': station_info['list'][0]['lon']}
            converted_coords = convert_coordinates(coords)
            station_lat = converted_coords['lat']
            station_lon = converted_coords['lon']

            print(f"Downloading hourly water level data for station: {station_name} ({station_code})")

            def get_data():
                return get_hourly_water_level(station_code, start_date, end_date)

            hourly_data = retry(get_data)

            if hourly_data != 'ERROR' and 'list' in hourly_data:
                station_data = pd.DataFrame(hourly_data['list'])
                station_data['id'] = station_code
                station_data['station_name'] = station_name
                station_data['latitude'] = station_lat
                station_data['longitude'] = station_lon
                station_data['bbsnnm'] = station_info['list'][0].get('bbsnnm')
                station_data['clsyn'] = station_info['list'][0].get('clsyn')
                station_data['obsknd'] = station_info['list'][0].get('obsknd')
                station_data['sbsncd'] = station_info['list'][0].get('sbsncd')
                station_data['mngorg'] = station_info['list'][0].get('mngorg')
                all_wl_data = pd.concat([all_wl_data, station_data], ignore_index=True)

    all_wl_data['time'] = all_wl_data['ymdh'].apply(try_parse_h)
    all_wl_data.loc[all_wl_data['wl'] == "-", 'wl'] = ""
    all_wl_data.to_csv("hourly_wl_data.csv", index=False)

    print("Hourly water level data saved.")
    print("Uploading data to IHP-WINS...")
    upload_ckan(name='Hourly Water Level Data for the Last 3 Days', description='Testing', pathtofile="hourly_wl_data.csv", resource_id = "c7753a8a-b9f6-44ae-9437-ee5c442555cd", package_id = "9897691a-c6d4-416c-8d16-02e0e7db1a2f")
    print("Upload done")

# Function to process and save daily water level data
def process_daily_water_level():
    stations = get_stations(WL_STATIONS_URL)
    all_wl_data = pd.DataFrame()
    start_date, end_date = get_korean_time_range(hours_back=2160)

    for station in stations['list']:
        station_code = station['obscd']
        station_name = station['obsnm']

        def get_station_data():
            return get_station_info(station_code, stationInfoUrl=WL_STATIONS_INFO_URL)

        station_info = retry(get_station_data)

        if station_info['result'].get('code') == 'success' and '해당 데이터가 없습니다.' not in station_info['result'].get('msg'):
            coords = {'lat': station_info['list'][0]['lat'], 'lon': station_info['list'][0]['lon']}
            converted_coords = convert_coordinates(coords)
            station_lat = converted_coords['lat']
            station_lon = converted_coords['lon']


            print(f"Downloading daily water level data for station: {station_name} ({station_code})")

            def get_data():
                return get_daily_water_level(station_code, start_date, end_date)

            daily_data = retry(get_data)

            if daily_data != 'ERROR' and 'list' in daily_data:
                station_data = pd.DataFrame(daily_data['list'])
                station_data['id'] = station_code
                station_data['station_name'] = station_name
                station_data['latitude'] = station_lat
                station_data['longitude'] = station_lon
                station_data['obsnmeng'] = station_info['list'][0].get('obsnmeng')
                station_data['wlobscd'] = station_info['list'][0].get('wlobscd')
                station_data['mggvcd'] = station_info['list'][0].get('mggvcd')
                station_data['bbsncd'] = station_info['list'][0].get('bbsncd')
                station_data['sbsncd'] = station_info['list'][0].get('sbsncd')
                station_data['obsopndt'] = station_info['list'][0].get('obsopndt')
                station_data['obskdcd'] = station_info['list'][0].get('obskdcd')
                station_data['rivnm'] = station_info['list'][0].get('rivnm')
                station_data['bsnara'] = station_info['list'][0].get('bsnara')
                station_data['rvwdt'] = station_info['list'][0].get('rvwdt')                
                station_data['bedslp'] = station_info['list'][0].get('bedslp')
                station_data['rvmjctdis'] = station_info['list'][0].get('rvmjctdis')
                station_data['wsrdis'] = station_info['list'][0].get('wsrdis')
                station_data['tmx'] = station_info['list'][0].get('tmx')
                station_data['tmy'] = station_info['list'][0].get('tmy')                
                station_data['gdt'] = station_info['list'][0].get('gdt')                
                station_data['wltel'] = station_info['list'][0].get('wltel')
                station_data['tdeyn'] = station_info['list'][0].get('tdeyn')
                station_data['mxgrd'] = station_info['list'][0].get('mxgrd')
                station_data['sistartobsdh'] = station_info['list'][0].get('sistartobsdh')
                station_data['siendobsdh'] = station_info['list'][0].get('siendobsdh')
                station_data['olstartobsdh'] = station_info['list'][0].get('olstartobsdh')
                station_data['olendobsdh'] = station_info['list'][0].get('olendobsdh')
                all_wl_data = pd.concat([all_wl_data, station_data], ignore_index=True)

    all_wl_data['time'] = pd.to_datetime(all_wl_data['ymd'], format='%Y%m%d', errors='coerce')
    all_wl_data.loc[all_wl_data['wl'] == "-", 'wl'] = ""
    all_wl_data.to_csv("daily_wl_data.csv", index=False)

    print("Daily water level data saved.")
    print("Uploading data to IHP-WINS...")
    upload_ckan(name='Daily Water Level Data for the Last 3 Months', description='This is for testing', pathtofile="daily_wl_data.csv", resource_id="c8482cd0-c70c-4d28-9e14-76d83a8e6544", package_id="9897691a-c6d4-416c-8d16-02e0e7db1a2f")
    print("Upload done")


# Function to process and save hourly meteorological data
def process_hourly_meteorological_data():
    stations = get_stations(METEO_STATIONS_URL)
    all_meteo_data = pd.DataFrame()
    start_date, end_date = get_korean_time_range()

    for station in stations['list']:
        station_code = station['obscd']
        station_name = station['obsnm']

        def get_station_data():
            return get_station_info(station_code, stationInfoUrl=METEO_STATION_INFO_URL)

        station_info = retry(get_station_data)

        if station_info['result'].get('code') == 'success' and '해당 데이터가 없습니다.' not in station_info['result'].get('msg'):
            coords = {'lat': station_info['list'][0]['lat'], 'lon': station_info['list'][0]['lon']}
            converted_coords = convert_coordinates(coords)
            station_lat = converted_coords['lat']
            station_lon = converted_coords['lon']

            print(f"Downloading hourly meteorological data for station: {station_name} ({station_code})")

            def get_data():
                return get_hourly_meteorological_data(station_code, start_date, end_date)

            hourly_data = retry(get_data)

            if hourly_data != 'ERROR' and 'list' in hourly_data:
                station_data = pd.DataFrame(hourly_data['list'])
                station_data['id'] = station_code
                station_data['station_name'] = station_name
                station_data['latitude'] = station_lat
                station_data['longitude'] = station_lon
                station_data['bbsnnm'] = station['bbsnnm']
                station_data['clsyn'] = station['clsyn']
                station_data['obsknd'] = station['obsknd']
                station_data['sbsncd'] = station['sbsncd']
                station_data['mngorg'] = station['mngorg']
                station_data['addr'] = station['addr']
                station_data['wtobscd'] = station_info['list'][0].get('wtobscd')
                station_data['obskdcd'] = station_info['list'][0].get('obskdcd')
                station_data['mggvcd'] = station_info['list'][0].get('mggvcd')
                station_data['opndt'] = station_info['list'][0].get('opndt')
                station_data['tmx'] = station_info['list'][0].get('tmx')
                station_data['tmy'] = station_info['list'][0].get('tmy')
                station_data['bbsncd'] = station_info['list'][0].get('bbsncd')
                station_data['obselm'] = station_info['list'][0].get('obselm')
                station_data['thrmlhi'] = station_info['list'][0].get('thrmlhi')
                station_data['prselm'] = station_info['list'][0].get('prselm')
                station_data['wvmlhi'] = station_info['list'][0].get('wvmlhi')
                station_data['hytmlhi'] = station_info['list'][0].get('hytmlhi')
                station_data['nj'] = station_info['list'][0].get('nj')
                all_meteo_data = pd.concat([all_meteo_data, station_data], ignore_index=True)

    all_meteo_data['time'] = all_meteo_data['ymdh'].apply(try_parse_h)
    all_meteo_data.loc[all_meteo_data['ta'] == "null", 'ta'] = ""
    all_meteo_data.loc[all_meteo_data['hm'] == "null", 'hm'] = ""
    all_meteo_data.loc[all_meteo_data['td'] == "null", 'td'] = ""
    all_meteo_data.loc[all_meteo_data['ps'] == "null", 'ps'] = ""
    all_meteo_data.loc[all_meteo_data['ws'] == "null", 'ws'] = ""
    all_meteo_data.loc[all_meteo_data['wd'] == "null", 'wd'] = ""
    all_meteo_data.loc[all_meteo_data['sihr1'] == "null", 'sihr1'] = ""
    all_meteo_data.loc[all_meteo_data['catot'] == "null", 'catot'] = ""
    all_meteo_data.loc[all_meteo_data['sdtot'] == "null", 'sdtot'] = ""
    all_meteo_data.loc[all_meteo_data['sshr1'] == "null", 'sshr1'] = ""


    all_meteo_data.to_csv("hourly_meteo_data.csv", index=False)

    print("Hourly meteorological data saved.")
    print("Uploading data to IHP-WINS...")
    upload_ckan(name='Hourly Meteorological Data for the Last 3 Days', description='Testing', pathtofile="hourly_meteo_data.csv", resource_id="04b31ddd-5681-49bd-a065-f8c387433382", package_id="9897691a-c6d4-416c-8d16-02e0e7db1a2f")
    print("Upload done")

# Function to process and save daily meteorological data
def process_daily_meteorological_data():
    stations = get_stations(METEO_STATIONS_URL)
    all_meteo_data = pd.DataFrame()
    start_date, end_date = get_korean_time_range(hours_back=2160)

    for station in stations['list']:
        station_code = station['obscd']
        station_name = station['obsnm']

        def get_station_data():
            return get_station_info(station_code, stationInfoUrl=METEO_STATION_INFO_URL)

        station_info = retry(get_station_data)

        if station_info['result'].get('code') == 'success' and '해당 데이터가 없습니다.' not in station_info['result'].get('msg'):
            coords = {'lat': station_info['list'][0]['lat'], 'lon': station_info['list'][0]['lon']}
            converted_coords = convert_coordinates(coords)
            station_lat = converted_coords['lat']
            station_lon = converted_coords['lon']

            print(f"Downloading daily meteorological data for station: {station_name} ({station_code})")

            def get_data():
                return get_daily_meteorological_data(station_code, start_date, end_date)

            daily_data = retry(get_data)

            if daily_data != 'ERROR' and 'list' in daily_data:
                station_data = pd.DataFrame(daily_data['list'])
                station_data['id'] = station_code
                station_data['station_name'] = station_name
                station_data['latitude'] = station_lat
                station_data['longitude'] = station_lon
                station_data['bbsnnm'] = station['bbsnnm']
                station_data['clsyn'] = station['clsyn']
                station_data['obsknd'] = station['obsknd']
                station_data['sbsncd'] = station['sbsncd']
                station_data['mngorg'] = station['mngorg']
                station_data['addr'] = station['addr']
                station_data['wtobscd'] = station_info['list'][0].get('wtobscd')
                station_data['obskdcd'] = station_info['list'][0].get('obskdcd')
                station_data['mggvcd'] = station_info['list'][0].get('mggvcd')
                station_data['opndt'] = station_info['list'][0].get('opndt')
                station_data['tmx'] = station_info['list'][0].get('tmx')
                station_data['tmy'] = station_info['list'][0].get('tmy')
                station_data['bbsncd'] = station_info['list'][0].get('bbsncd')
                station_data['obselm'] = station_info['list'][0].get('obselm')
                station_data['thrmlhi'] = station_info['list'][0].get('thrmlhi')
                station_data['prselm'] = station_info['list'][0].get('prselm')
                station_data['wvmlhi'] = station_info['list'][0].get('wvmlhi')
                station_data['hytmlhi'] = station_info['list'][0].get('hytmlhi')
                station_data['nj'] = station_info['list'][0].get('nj')                
                all_meteo_data = pd.concat([all_meteo_data, station_data], ignore_index=True)

    all_meteo_data['time'] = pd.to_datetime(all_meteo_data['ymd'], format='%Y%m%d', errors='coerce')
    all_meteo_data.loc[all_meteo_data['taavg'] == "null", 'taavg'] = ""
    all_meteo_data.loc[all_meteo_data['tamin'] == "null", 'tamin'] = ""
    all_meteo_data.loc[all_meteo_data['tamax'] == "null", 'tamax'] = ""
    all_meteo_data.loc[all_meteo_data['wsavg'] == "null", 'wsavg'] = ""
    all_meteo_data.loc[all_meteo_data['wsmax'] == "null", 'wsmax'] = ""
    all_meteo_data.loc[all_meteo_data['wdmax'] == "null", 'wdmax'] = ""
    all_meteo_data.loc[all_meteo_data['hmavg'] == "null", 'hmavg'] = ""
    all_meteo_data.loc[all_meteo_data['hmmin'] == "null", 'hmmin'] = ""
    all_meteo_data.loc[all_meteo_data['evs'] == "null", 'evs'] = ""
    all_meteo_data.loc[all_meteo_data['evl'] == "null", 'evl'] = ""
    all_meteo_data.loc[all_meteo_data['catotavg'] == "null", 'catotavg'] = ""
    all_meteo_data.loc[all_meteo_data['psavg'] == "null", 'psavg'] = ""
    all_meteo_data.loc[all_meteo_data['psmax'] == "null", 'psmax'] = ""
    all_meteo_data.loc[all_meteo_data['psmin'] == "null", 'psmin'] = ""
    all_meteo_data.loc[all_meteo_data['sdmax'] == "null", 'sdmax'] = ""
    all_meteo_data.loc[all_meteo_data['tdavg'] == "null", 'tdavg'] = ""
    all_meteo_data.loc[all_meteo_data['siavg'] == "null", 'siavg'] = ""
    all_meteo_data.loc[all_meteo_data['ssavg'] == "null", 'ssavg'] = ""


    all_meteo_data.to_csv("daily_meteo_data.csv", index=False)

    print("Daily meteorological data saved.")
    print("Uploading data to IHP-WINS...")
    upload_ckan(name='Daily Meteorological Data for the last 3 Months', description='Testing', pathtofile="daily_meteo_data.csv", resource_id="0378e7c0-1f70-434b-9ae0-e06d65476bfa", package_id="9897691a-c6d4-416c-8d16-02e0e7db1a2f")
    print("Upload done")
    
# Function to get daily flow rate data
def get_daily_flow_rate(station_code, year):
    params = {"obscd": station_code, "year": year, "output": "json"}
    response = requests.get(DAILY_FLOW_RATE, params=params)
    return response.json() if response.status_code == 200 else 'ERROR'

# Function to get suspended sediment load data
def get_suspended_sediment_load(station_code, start_year, end_year):
    params = {"obscd": station_code, "startyear": start_year, "endyear": end_year, "output": "json"}
    response = requests.get(SSL_STATION_DATA, params=params)
    return response.json() if response.status_code == 200 else 'ERROR'

# Example functions to process flow rate and suspended sediment load
def process_flow_rate():
    stations = get_stations(FLOW_RATE_STATION_INFO)
    all_flow_data = pd.DataFrame()

    for station in stations['list']:
        station_code = station['obscd']
        station_name = station['obsnm']
        current_year = datetime.now().year
        
        def get_data():
            return get_daily_flow_rate(station_code, current_year)

        flow_data = retry(get_data)

        if flow_data != 'ERROR' and 'list' in flow_data:
            station_data = pd.DataFrame(flow_data['list'])
            station_data['id'] = station_code
            station_data['station_name'] = station_name
            station_data['bbsnnm'] = station.get('bbsnnm')
            station_data['sbsncd'] = station.get('sbsncd')
            station_data['mngorg'] = station.get('mngorg')
            station_data['minyear'] = station.get('minyear')
            station_data['maxyear'] = station.get('maxyear')
            all_flow_data = pd.concat([all_flow_data, station_data], ignore_index=True)
            
    all_flow_data.loc[all_flow_data['fw'] == "-", 'fw'] = ""
    all_flow_data.to_csv("daily_flow_rate_data.csv", index=False)
    print("Daily flow rate data saved.")
    print("Uploading data to IHP-WINS...")
    upload_ckan(name='Daily Flow Rate for Current Year', description='Testing', pathtofile="daily_flow_rate_data.csv", resource_id="d93b4838-ecd4-416e-94b4-cdbb0b1b9d1b", package_id="9897691a-c6d4-416c-8d16-02e0e7db1a2f")
    print("Upload done")

def process_suspended_sediment_load():
    stations = get_stations(SSL_STATION_INFO)
    all_ssl_data = pd.DataFrame()
    start_year = datetime.now().year - 3  
    end_year = datetime.now().year

    for station in stations['list']:
        station_code = station['wlcd']
        station_name = station['obsnm']

        def get_data():
            return get_suspended_sediment_load(station_code, start_year, end_year)

        ssl_data = retry(get_data)

        if ssl_data != 'ERROR' and 'list' in ssl_data:
            station_data = pd.DataFrame(ssl_data['list'])
            station_data['id'] = station_code
            station_data['station_name'] = station_name
            all_ssl_data = pd.concat([all_ssl_data, station_data], ignore_index=True)

    all_ssl_data.to_csv("suspended_sediment_load_data.csv", index=False)
    print("Suspended sediment load data saved.")
    print("Uploading data to IHP-WINS...")
    upload_ckan(name='Suspended Sediment Load for Last 3 Years', description='Testing', pathtofile="suspended_sediment_load_data.csv", resource_id="8f87fd40-8676-4f98-8f4d-fffeed77c1ec", package_id="9897691a-c6d4-416c-8d16-02e0e7db1a2f")
    print("Upload done")
    

# Define the hourly DAG
hourly_dag = DAG(
    'kr_hourly_precipitation_data',
    default_args=default_args,
    description='A DAG to fetch hourly precipitation data',
    schedule_interval="0 */3 * * *",
)
hourly_dag_wl = DAG(
    'kr_hourly_water_level_data',
    default_args=default_args,
    description='A DAG to fetch hourly wl data',
    schedule_interval="20 */3 * * *",
)
hourly_dag_meteo= DAG(
    'kr_hourly_meteo_data',
    default_args=default_args,
    description='A DAG to fetch hourly meteo data',
    schedule_interval="50 */3 * * *",
)
# Define the daily DAG
daily_dag = DAG(
    'kr_daily_precipitation_data',
    default_args=default_args,
    description='A DAG to fetch daily precipitation data',
    schedule_interval="0 3 * * *",
)
# Define the daily DAG
daily_dag_dwl = DAG(
    'kr_process_daily_water_level',
    default_args=default_args,
    description='A DAG to fetch daily water level',
    schedule_interval="0 2 * * *",
)
# Define the daily DAG
daily_dag_dmd = DAG(
    'kr_process_daily_meteorological_data',
    default_args=default_args,
    description='A DAG to fetch daily meteo data',
    schedule_interval="0 1 * * *",
)
daily_dag_dfr = DAG(
    'kr_process_flow_rate',
    default_args=default_args,
    description='A DAG to fetch daily flow rate data',
    schedule_interval="0 5 * * *",
)
daily_dag_dssl = DAG(
    'kr_process_suspended_sediment_load',
    default_args=default_args,
    description='A DAG to fetch daily sediment load',
    schedule_interval="0 6 * * *",
)
daily_dag_mmp = DAG(
    'kr_process_monthly_precipitation',
    default_args=default_args,
    description='A DAG to fetch monthly precipitation',
    schedule_interval="0 7 * * *",
)


# Hourly tasks
fetch_hourly_data = PythonOperator(
    task_id='process_hourly_precipitation',
    python_callable=process_hourly_precipitation,
    dag=hourly_dag,
)
# Hourly tasks
fetch_hourly_wl = PythonOperator(
    task_id='process_hourly_water_level',
    python_callable=process_hourly_water_level,
    dag=hourly_dag_wl,
)
# Hourly tasks
fetch_hourly_meteo = PythonOperator(
    task_id='process_hourly_meteorological_data',
    python_callable=process_hourly_meteorological_data,
    dag=hourly_dag_meteo,
)
# Daily tasks
fetch_daily_data = PythonOperator(
    task_id='process_daily_precipitation',
    python_callable=process_daily_precipitation,
    dag=daily_dag,
)
# Daily tasks
fetch_daily_wl = PythonOperator(
    task_id='process_daily_water_level',
    python_callable=process_daily_water_level,
    dag=daily_dag_dwl,
)
fetch_daily_meteo = PythonOperator(
    task_id='process_daily_meteorological_data',
    python_callable=process_daily_meteorological_data,
    dag=daily_dag_dmd,
)
fetch_daily_fr = PythonOperator(
    task_id='process_flow_rate',
    python_callable=process_flow_rate,
    dag=daily_dag_dfr,
)
fetch_daily_ssl = PythonOperator(
    task_id='process_suspended_sediment_load',
    python_callable=process_suspended_sediment_load,
    dag=daily_dag_dssl,
)
fetch_daily_mmp = PythonOperator(
    task_id='process_monthly_precipitation',
    python_callable=process_monthly_precipitation,
    dag=daily_dag_mmp,
)


