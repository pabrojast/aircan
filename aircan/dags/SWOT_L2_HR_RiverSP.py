from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import subprocess
import sys
import os
import requests
import json
from datetime import datetime, timedelta
from time import sleep
###config
APIdev = Variable.get("APIDEV")

def setup_netrc():
    # Obtenemos las credenciales de Airflow Variables (o de os.environ)
    username = Variable.get("NASA_USERNAME")
    password = Variable.get("NASA_PASSWORD")
    
    netrc_path = os.path.expanduser("~/.netrc")  # /home/airflow/.netrc en contenedores de Airflow
    with open(netrc_path, "w") as f:
        f.write(f"""machine urs.earthdata.nasa.gov
    login {username}
    password {password}
""")
    # Ajustar permisos a 600 (lectura/escritura sólo para el dueño)
    os.chmod(netrc_path, 0o600)
    print(f".netrc creado en {netrc_path}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
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




def check_file_exists_in_api(name):
    api_url = "https://ihp-wins.unesco.org/api/3/action/package_show"
    
    # Define los IDs de ambos datasets
    package_ids = [
        "swot-level-2-river-single-pass-vector-node-data-product-for-ukraine",
        "swot-level-2-river-single-pass-vector-reach-data-product-for-ukraine"
    ]
    
    try:
        # Verificar en ambos datasets
        for package_id in package_ids:
            params = {"id": package_id}
            response = requests.get(api_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data["success"]:
                resources = data["result"]["resources"]
                name_lower = name.lower()  # Convert input filename to lowercase
                for resource in resources:
                    resource_url = resource["url"].lower()  # Convert URL to lowercase
                    if resource_url.endswith(name_lower):
                        print(f"Archivo {name} encontrado en dataset {package_id}")
                        last_modified = datetime.strptime(resource["last_modified"], "%Y-%m-%dT%H:%M:%S.%f")
                        return True, last_modified
        
        return False, None
        
    except requests.exceptions.RequestException as e:
        print(f"Error al consultar la API: {str(e)}")
        return False, None

def download_swot_data():    
    #creamos (o reescribimos) el archivo .netrc para que podaac-data-subscriber tenga credenciales
    setup_netrc()
    # Primero verificar si el comando existe
    try:
        subprocess.run(['podaac-data-subscriber', '--version'], 
                      check=True, 
                      capture_output=True, 
                      text=True)
    except subprocess.CalledProcessError:
        print("Error: podaac-data-subscriber no está instalado o no es accesible")
        sys.exit(1)
    except FileNotFoundError:
        print("Error: podaac-data-subscriber no se encuentra en el PATH")
        sys.exit(1)

    end_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    
    command = [
        'podaac-data-subscriber',
        '-c', 'SWOT_L2_HR_RiverSP_2.0',
        '-d', './data',
        '-m', '1200',
        '-e', '.zip',
        '-b', '22,44,40,52'
    ]
    #'--start-date', '2024-12-01T00:00:00Z'
    #'--end-date', end_date

    try:
        # Ejecutar con shell=True si es necesario en tu entorno
        result = subprocess.run(command, 
                              check=True, 
                              capture_output=True, 
                              text=True,
                              shell=False)  # Cambiar a True si es necesario
        print("Comando ejecutado:")
        print(" ".join(command))
        print("\nSalida:")
        print(result.stdout)
        upload_ckan()
        main()
        return True
    except subprocess.CalledProcessError as e:
        print("Error durante la descarga:")
        print(f"Código de salida: {e.returncode}")
        print(f"Error: {e.stderr}")
        return False

def get_file_type(filename):
    """Determine if the file is a Node or Reach type"""
    if '_Node_' in filename:
        return 'node'
    elif '_Reach_' in filename:
        return 'reach'
    return None

def upload_ckan(
    node_package_id="swot-level-2-river-single-pass-vector-node-data-product-for-ukraine",
    reach_package_id="swot-level-2-river-single-pass-vector-reach-data-product-for-ukraine",
    url='https://ihp-wins.unesco.org/api/3/action/resource_create'
):
    """
    Upload SWOT data files from the data directory to CKAN platform,
    separating Node and Reach files into different datasets
    """
    data_dir = "./data"
    
    if not os.path.exists(data_dir):
        print(f"El directorio {data_dir} no existe")
        return
    
    for filename in os.listdir(data_dir):
        file_path = os.path.join(data_dir, filename)
        
        # Determine file type
        file_type = get_file_type(filename)
        if not file_type:
            print(f"Archivo {filename} no reconocido como Node o Reach, saltando...")
            continue
            
        # Select appropriate package_id based on file type
        package_id = node_package_id if file_type == 'node' else reach_package_id
        
        exists, last_modified = check_file_exists_in_api(filename)
        if exists:
            print(f"El archivo ya existe en la API (última modificación: {last_modified})")
            print("Cancelando la subida...")
            continue

        if os.path.isfile(file_path):
            try:
                files = {'upload': open(file_path, 'rb')}
                headers = {"Authorization": APIdev}
                
                data_dict = {
                    'package_id': package_id,
                    'name': filename,
                    'description': f'SWOT {file_type.upper()} data file: {filename}',
                    'format': 'shp'  
                }
                
                # POST request
                response = requests.post(url, headers=headers, data=data_dict, files=files)
                
                # Logging
                print(f"Subiendo archivo {file_type}: {filename}")
                print("Status Code:", response.status_code)
                print("JSON Response:", response.json())
                
            except Exception as e:
                print(f"Error al subir {filename}: {str(e)}")
            finally:
                files['upload'].close()


def get_resource_views(resource_id, api_url, api_key):
    """Get list of views for a specific resource"""
    headers = {
        'Authorization': api_key
    }
    
    view_list_url = f"{api_url}/api/3/action/resource_view_list"
    params = {'id': resource_id}
    
    try:
        response = requests.post(view_list_url, headers=headers, json=params)
        response.raise_for_status()
        return response.json()['result']
    except requests.exceptions.RequestException as e:
        print(f"Error getting views for resource {resource_id}: {e}")
        return []

def update_resource_view(view_id, view_data, api_url, api_key):
    """Update a specific resource view"""
    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }
    
    update_url = f"{api_url}/api/3/action/resource_view_update"
    
    # Ensure view_id is included in the update data
    view_data['id'] = view_id
    
    try:
        response = requests.post(update_url, headers=headers, json=view_data)
        response.raise_for_status()
        return response.json()['result']
    except requests.exceptions.RequestException as e:
        print(f"Error updating view {view_id}: {e}")
        return None

def process_package_resources(package_data, api_url, api_key):
    """Process all resources in a package"""
    resources = package_data.get('result', {}).get('resources', [])
    
    for resource in resources:
        resource_id = resource['id']
        print(f"\nProcessing resource: {resource_id}")
        
        # Get existing views
        views = get_resource_views(resource_id, api_url, api_key)
        
        for view in views:
            view_id = view['id']
            print(f"Found view: {view_id}")
            
            # Here you can add logic to determine what updates are needed
            # For example:
            updated_view_data = {
                'id': view_id,
                'title': view.get('title', ''),
                'description': view.get('description', ''),
                # Add other fields you want to update
            }
            
            # Update the view
            result = update_resource_view(view_id, updated_view_data, api_url, api_key)
            if result:
                print(f"Successfully updated view {view_id}")
            else:
                print(f"Failed to update view {view_id}")

def main():
    API_URLPS = "https://ihp-wins.unesco.org/api/3/action/package_show"
    API_URL = "https://ihp-wins.unesco.org/"
    API_KEY = APIdev  
    
    package_ids = "swot-level-2-river-single-pass-vector-reach-data-product-for-ukraine"
    
    params = {"id": package_ids}
    response = requests.get(API_URLPS, params=params)
    response.raise_for_status()
    data = response.json()

            
    if data["success"]:
        resources = data["result"]["resources"]
        # Your package data is already provided in the JSON
        for resource in resources:
            views = get_resource_views(resource["id"], API_URL, API_KEY)
            print(resource["id"])
            for view in views:
                if view["title"] == "Terria Viewer":
                    view['custom_config'] = "https://ihp-wins.unesco.org/terria/#start=%7B%22version%22%3A%228.0.0%22%2C%22initSources%22%3A%5B%7B%22stratum%22%3A%22user%22%2C%22models%22%3A%7B%22%2F%22%3A%7B%22members%22%3A%5B%22SWOT_L2_HR_RiverSP_Reach_025_363_EU_20241215T223722_20241215T224651_PIC2_01.zip%22%5D%2C%22type%22%3A%22group%22%7D%2C%22SWOT_L2_HR_RiverSP_Reach_025_363_EU_20241215T223722_20241215T224651_PIC2_01.zip%22%3A%7B%22name%22%3A%22SWOT_L2_HR_RiverSP_Reach_025_363_EU_20241215T223722_20241215T224651_PIC2_01.zip%22%2C%22isOpenInWorkbench%22%3Atrue%2C%22currentTime%22%3A%222024-12-15T22%3A38%3A20.000000000Z%22%2C%22startTime%22%3A%222024-12-15T22%3A37%3A22.000000000Z%22%2C%22stopTime%22%3A%222024-12-15T22%3A46%3A30.000000000Z%22%2C%22multiplier%22%3A2.7676767676767677%2C%22isPaused%22%3Atrue%2C%22showInChartPanel%22%3Afalse%2C%22opacity%22%3A0.8%2C%22columns%22%3A%5B%7B%22name%22%3A%22wse%22%2C%22title%22%3A%22Water+Surface+Elevation+%22%2C%22units%22%3A%22%5Bmeters%5D%22%7D%5D%2C%22styles%22%3A%5B%7B%22id%22%3A%22wse%22%2C%22color%22%3A%7B%22mapType%22%3A%22continuous%22%2C%22minimumValue%22%3A0%2C%22maximumValue%22%3A2100%2C%22binColors%22%3A%5B%5D%2C%22enumColors%22%3A%5B%5D%2C%22colorPalette%22%3A%22Warm%22%2C%22legendTicks%22%3A20%2C%22legend%22%3A%7B%22title%22%3A%22Water+Surface+Elevation%22%7D%7D%2C%22outline%22%3A%7B%22null%22%3A%7B%22width%22%3A4%7D%7D%2C%22time%22%3A%7B%22timeColumn%22%3A%22time_str%22%2C%22idColumns%22%3A%5B%22reach_id%22%5D%2C%22spreadStartTime%22%3Atrue%2C%22spreadFinishTime%22%3Atrue%7D%2C%22hidden%22%3Afalse%7D%5D%2C%22defaultStyle%22%3A%7B%22time%22%3A%7B%22timeColumn%22%3Anull%7D%7D%2C%22activeStyle%22%3A%22wse%22%2C%22showDisableTimeOption%22%3Atrue%2C%22url%22%3A%22https%3A%2F%2Fhel1.your-objectstorage.com%2Fihp%2Fresources%2Fad0d133a-69a2-46a1-90b1-ee18b8a07f07%2Fswot_l2_hr_riversp_reach_025_363_eu_20241215t223722_20241215t224651_pic2_01.zip%22%2C%22cacheDuration%22%3A%225m%22%2C%22knownContainerUniqueIds%22%3A%5B%22%2F%22%5D%2C%22type%22%3A%22shp%22%7D%7D%2C%22workbench%22%3A%5B%22SWOT_L2_HR_RiverSP_Reach_025_363_EU_20241215T223722_20241215T224651_PIC2_01.zip%22%5D%2C%22timeline%22%3A%5B%5D%2C%22initialCamera%22%3A%7B%22west%22%3A-7.210575368062457%2C%22south%22%3A40.79309814167728%2C%22east%22%3A23.566954983233757%2C%22north%22%3A53.25830559526699%2C%22position%22%3A%7B%22x%22%3A5433759.068950988%2C%22y%22%3A780905.3937606366%2C%22z%22%3A6034107.421245508%7D%2C%22direction%22%3A%7B%22x%22%3A-0.6639601265147111%2C%22y%22%3A-0.0954201387028785%2C%22z%22%3A-0.7416548708991888%7D%2C%22up%22%3A%7B%22x%22%3A-0.7341125763137992%2C%22y%22%3A-0.10550200389759194%2C%22z%22%3A0.6707816727307843%7D%7D%2C%22homeCamera%22%3A%7B%22west%22%3A22.155219%2C%22south%22%3A44.395641%2C%22east%22%3A40.135255%2C%22north%22%3A52.363276%7D%2C%22viewerMode%22%3A%223d%22%2C%22showSplitter%22%3Afalse%2C%22splitPosition%22%3A0.5%2C%22settings%22%3A%7B%22baseMaximumScreenSpaceError%22%3A2%2C%22useNativeResolution%22%3Afalse%2C%22alwaysShowTimeline%22%3Afalse%2C%22baseMapId%22%3A%22basemap-natural-earth-II%22%2C%22terrainSplitDirection%22%3A0%2C%22depthTestAgainstTerrainEnabled%22%3Afalse%7D%2C%22stories%22%3A%5B%5D%7D%5D%7D"
                    update = update_resource_view(view["id"], view, API_URL, API_KEY)
                    print(update)
            #print(views)
        #print(resources)


    # Define los IDs de ambos datasets
    package_ids = "swot-level-2-river-single-pass-vector-node-data-product-for-ukraine"
    
    params = {"id": package_ids}
    response = requests.get(API_URLPS, params=params)
    response.raise_for_status()
    data = response.json()

            
    if data["success"]:
        resources = data["result"]["resources"]
        # Your package data is already provided in the JSON
        for resource in resources:
            views = get_resource_views(resource["id"], API_URL, API_KEY)
            print(resource["id"])
            for view in views:
                if view["title"] == "Terria Viewer":
                    view['custom_config'] = "https://ihp-wins.unesco.org/terria/#start=%7B%22version%22%3A%228.0.0%22%2C%22initSources%22%3A%5B%7B%22stratum%22%3A%22user%22%2C%22models%22%3A%7B%22%2F%22%3A%7B%22members%22%3A%5B%22SWOT_L2_HR_RiverSP_Node_024_555_EU_20241201T222613_20241201T223604_PIC2_01.zip%22%5D%2C%22type%22%3A%22group%22%7D%2C%22SWOT_L2_HR_RiverSP_Node_024_555_EU_20241201T222613_20241201T223604_PIC2_01.zip%22%3A%7B%22name%22%3A%22SWOT_L2_HR_RiverSP_Node_024_555_EU_20241201T222613_20241201T223604_PIC2_01.zip%22%2C%22isOpenInWorkbench%22%3Atrue%2C%22currentTime%22%3A%222024-12-01T22%3A35%3A26.931351733Z%22%2C%22startTime%22%3A%222024-12-01T22%3A28%3A45.000000000Z%22%2C%22stopTime%22%3A%222024-12-01T22%3A35%3A46.000000000Z%22%2C%22multiplier%22%3A2.567073170731707%2C%22isPaused%22%3Atrue%2C%22opacity%22%3A0.8%2C%22columns%22%3A%5B%7B%22name%22%3A%22wse%22%2C%22title%22%3A%22WSE+-+Water+Surface+Elevation%22%2C%22units%22%3A%22meters%22%7D%5D%2C%22styles%22%3A%5B%7B%22id%22%3A%22wse%22%2C%22color%22%3A%7B%22mapType%22%3A%22continuous%22%2C%22minimumValue%22%3A0%2C%22maximumValue%22%3A2100%2C%22colorPalette%22%3A%22Reds%22%2C%22legendTicks%22%3A20%2C%22legend%22%3A%7B%22title%22%3A%22Water+Surface+Elevation+%5Bmeters%5D%22%7D%7D%2C%22pointSize%22%3A%7B%22pointSizeColumn%22%3A%22%22%7D%2C%22time%22%3A%7B%22timeColumn%22%3A%22time_str%22%7D%2C%22hidden%22%3Afalse%7D%5D%2C%22activeStyle%22%3A%22wse%22%2C%22url%22%3A%22https%3A%2F%2Fhel1.your-objectstorage.com%2Fihp%2Fresources%2F2c29aa65-a170-43a5-85c0-a9eb2ffa91ce%2Fswot_l2_hr_riversp_node_024_555_eu_20241201t222613_20241201t223604_pic2_01.zip%22%2C%22cacheDuration%22%3A%225m%22%2C%22knownContainerUniqueIds%22%3A%5B%22%2F%22%5D%2C%22type%22%3A%22shp%22%7D%7D%2C%22workbench%22%3A%5B%22SWOT_L2_HR_RiverSP_Node_024_555_EU_20241201T222613_20241201T223604_PIC2_01.zip%22%5D%2C%22timeline%22%3A%5B%22SWOT_L2_HR_RiverSP_Node_024_555_EU_20241201T222613_20241201T223604_PIC2_01.zip%22%5D%2C%22initialCamera%22%3A%7B%22west%22%3A35.6233709376047%2C%22south%22%3A52.098470391108535%2C%22east%22%3A54.78654033477024%2C%22north%22%3A57.71367072457998%2C%22position%22%3A%7B%22x%22%3A2965888.7204693444%2C%22y%22%3A2987183.847472996%2C%22z%22%3A6029362.091251377%7D%2C%22direction%22%3A%7B%22x%22%3A-0.4018027296219559%2C%22y%22%3A-0.40468767945121303%2C%22z%22%3A-0.8214514280033468%7D%2C%22up%22%3A%7B%22x%22%3A-0.578772361590359%2C%22y%22%3A-0.5829279561213858%2C%22z%22%3A0.5702784858569209%7D%7D%2C%22homeCamera%22%3A%7B%22west%22%3A22.155219%2C%22south%22%3A44.395641%2C%22east%22%3A40.135255%2C%22north%22%3A52.363276%7D%2C%22viewerMode%22%3A%223d%22%2C%22showSplitter%22%3Afalse%2C%22splitPosition%22%3A0.4999%2C%22settings%22%3A%7B%22baseMaximumScreenSpaceError%22%3A2%2C%22useNativeResolution%22%3Afalse%2C%22alwaysShowTimeline%22%3Afalse%2C%22baseMapId%22%3A%22basemap-natural-earth-II%22%2C%22terrainSplitDirection%22%3A0%2C%22depthTestAgainstTerrainEnabled%22%3Afalse%7D%2C%22stories%22%3A%5B%5D%7D%5D%7D"
                    update = update_resource_view(view["id"], view, API_URL, API_KEY)
                    print(update)
    



# Define the hourly DAG
hourly_dag = DAG(
    'Nasa_hourly_data',
    default_args=default_args,
    description='A DAG to fetch hourly nasa SWOT_L2_HR_RiverSP_2.0',
    schedule_interval="0 */10 * * *",
    catchup=False,
)
# Hourly tasks
fetch_hourly_meteo = PythonOperator(
    task_id='download_swot_data',
    python_callable=download_swot_data,
    dag=hourly_dag,
)
