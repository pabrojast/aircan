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
    os.chmod(netrc_path, 600)
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
    api_url = "https://data.dev-wins.com/api/3/action/package_show"
    
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
        '--start-date', '2024-12-01T00:00:00Z',
        '--end-date', end_date,
        '-e', '.zip',
        '-b', '22,44,40,52'
    ]
    
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
    url='https://data.dev-wins.com/api/3/action/resource_create'
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
                headers = {"API-Key": APIdev}
                
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
