import requests
import json
from collections import defaultdict
import xml.etree.ElementTree as ET
import urllib.parse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

# ------------------------------
# Configurations
# ------------------------------

# Base URLs and site configurations
site_url = "https://ihp-wins.unesco.org/"
base_url = "https://ihp-wins.unesco.org/api/3/action/"
formatos_permitidos = ['KML', 'tif', 'tiff', 'geotiff', 'csv', 'wms', 'wmts', 'shape', 'shp']

# Retry configuration
retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"]  # Updated for newer versions of urllib3
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)

# CKAN API Configuration
ckan_api_url = f"{site_url}api/3/action/resource_patch"
APIdev = Variable.get("APIDEV")
resource_id = "d38e6e75-dcc3-4e41-924e-edf41997be15"
package_id = "8aadbf95-c6ed-4366-9ad7-deafd033ae15"

# ------------------------------
# Functions
# ------------------------------

def get_api_data(url):
    """
    Performs a GET request to the given URL with retry logic.
    """
    try:
        response = http.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error in request to {url}: {response.status_code}")
            return None
    except Exception as e:
        print(f"Exception during request to {url}: {e}")
        return None

def process_sld_styles(style_url, resource_format):
    """
    Retrieves and processes the SLD styles from the given URL.
    """
    try:
        response = http.get(style_url)
        if response.status_code == 200:
            sld_xml = response.text
            root = ET.fromstring(sld_xml)
            # Define namespaces
            namespaces = {
                'sld': 'http://www.opengis.net/sld',
                'se': 'http://www.opengis.net/se',
                'ogc': 'http://www.opengis.net/ogc'
            }

            if resource_format in ['tif', 'tiff', 'geotiff', 'cog']:
                # Process raster styles for COG files
                colors = []
                legend_items = []
                color_map_entries = root.findall('.//sld:ColorMapEntry', namespaces)
                for entry in color_map_entries:
                    quantity = entry.get('quantity')
                    color = entry.get('color')
                    label = entry.get('label', '')

                    # Convert color to RGB string
                    if color.startswith('#'):
                        hex_color = color.lstrip('#')
                        rgb = tuple(int(hex_color[i:i + 2], 16) for i in (0, 2, 4))
                        rgb_string = f"rgb({rgb[0]}, {rgb[1]}, {rgb[2]})"
                    else:
                        rgb_string = color

                    colors.append([float(quantity), rgb_string])
                    legend_items.append({
                        "title": label.replace('+', ' '),
                        "color": color
                    })

                return {
                    'colors': colors,
                    'legend_items': legend_items
                }

            elif resource_format in ['shp', 'shape']:
                # Process vector styles for SHP files
                styles = {
                    'legend_items': [],
                    'enum_colors': [],
                    'property_name': None  # To store the property used for styling
                }

                rules = root.findall('.//se:Rule', namespaces)
                for rule in rules:
                    name = rule.find('se:Name', namespaces)
                    title = rule.find('.//se:Title', namespaces)
                    fill = rule.find('.//se:Fill/se:SvgParameter[@name="fill"]', namespaces)
                    property_name_element = rule.find('.//ogc:PropertyName', namespaces)
                    property_value_element = rule.find('.//ogc:Literal', namespaces)

                    if fill is not None and (name is not None or title is not None):
                        color = fill.text
                        label = (title.text if title is not None else name.text) if name is not None else "No label"
                        label = label.replace('+', ' ')

                        # Add to legend
                        styles['legend_items'].append({
                            "title": label,
                            "color": color
                        })

                        # Add to styles
                        if property_name_element is not None and property_value_element is not None:
                            property_value = property_value_element.text
                            property_name = property_name_element.text
                            styles['property_name'] = property_name  # Store property name for later use

                            # Convert color to RGBA string
                            if color.startswith('#'):
                                hex_color = color.lstrip('#')
                                rgb = tuple(int(hex_color[i:i + 2], 16) for i in (0, 2, 4))
                                rgba_string = f"rgba({rgb[0]}, {rgb[1]}, {rgb[2]},1)"
                            else:
                                rgba_string = color

                            styles['enum_colors'].append({
                                "value": property_value.replace('+', ' '),
                                "color": rgba_string
                            })
                return styles
            else:
                return None
        else:
            print(f"Error fetching SLD styles from {style_url}: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error processing SLD: {e}")
        return None

def format_dataset_item(resource, package_id, notes):
    """
    Formats a dataset item, including styles and legends if available.
    """
    resource_id = resource['id']
    resource_format = resource['format'].lower()
    resource_name = resource['name']
    resource_url = resource['url']
    resource_description = resource.get('description', '')

    # Adjust the type if necessary
    if resource_format in ["tif", "tiff", "geotiff"]:
        resource_format = "cog"

    # Create the base element
    elemento = {
        "name": resource_name,
        "type": resource_format,
        "id": resource_id,
        "url": resource_url,
        'description': (notes or '') + '<br/><br/><strong>Dataset URL: </strong> <a href="' + site_url + 'dataset/' + package_id + '">' + site_url + 'dataset/' + package_id + '</a>'
    }

    # Obtain Terria view and styles
    view_url = f"{base_url}resource_view_list"
    view_params = {"id": resource_id}
    try:
        response = http.post(view_url, json=view_params)
    except Exception as e:
        print(f"Exception during request to {view_url}: {e}")
        return elemento

    custom_config = None
    style_config = None

    if response.status_code == 200:
        view_data = response.json()
        for view in view_data.get('result', []):
            if view.get('view_type') == 'terria_view':
                custom_config = view.get('custom_config')
                style_url = view.get('style')
                # Only process styles for specific formats
                if style_url and style_url != 'NA':
                    style_config = process_sld_styles(style_url, resource_format)
                break
    else:
        print(f"Error obtaining views for resource {resource_id}: {response.status_code}")
        print(response.text)

    # Apply styles from SLD if available
    if style_config:
        if resource_format in ['cog']:
            # For raster data (COG files)
            elemento['renderOptions'] = {
                "single": {
                    "colors": style_config['colors'],
                    "useRealValue": True
                }
            }
            elemento['opacity'] = 0.8
            # Corrected legends structure
            elemento['legends'] = [
                {
                    "title": "Legend",
                    "items": style_config['legend_items']
                }
            ]
        elif resource_format in ['shp', 'shape']:
            # For vector data (SHP files)
            if style_config['enum_colors']:
                property_name = style_config['property_name'] or "default_style"
                elemento['styles'] = [{
                    "id": property_name,
                    "color": {
                        "enumColors": style_config['enum_colors'],
                        "colorPalette": "HighContrast"
                    }
                }]
                elemento['activeStyle'] = property_name
                elemento['opacity'] = 0.8
                # Corrected legends structure
                elemento['legends'] = [
                    {
                        "title": "Legend",
                        "items": style_config['legend_items']
                    }
                ]
    # If there are no styles from SLD but there is custom_config, process custom_config
    elif custom_config and custom_config not in ['NA', '']:
        try:
            # Extract the 'start' parameter from the URL
            parsed_url = urllib.parse.urlparse(custom_config)
            fragment = parsed_url.fragment
            start_param = fragment.split('=', 1)[1]
            decoded_param = urllib.parse.unquote_plus(start_param)

            # Parse the JSON
            custom_data = json.loads(decoded_param)

            # Process '+' in legend titles and values
            for init_source in custom_data.get('initSources', []):
                if 'models' in init_source:
                    for model_key, model_value in init_source['models'].items():
                        if 'legends' in model_value:
                            # Corrected legends structure
                            legends = model_value['legends']
                            for legend in legends:
                                if 'items' in legend:
                                    for item in legend['items']:
                                        if 'title' in item:
                                            item['title'] = item['title'].replace('+', ' ')
                            elemento['legends'] = legends
                        if 'styles' in model_value:
                            elemento['styles'] = model_value['styles']
                            if 'activeStyle' in model_value:
                                elemento['activeStyle'] = model_value['activeStyle']

        except Exception as e:
            print(f"Error processing custom config for {resource_id}: {e}")

    return elemento

def convert_sets_to_lists(obj):
    """
    Converts any sets in the object to lists for JSON serialization.
    """
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, dict):
        return {k: convert_sets_to_lists(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_sets_to_lists(v) for v in obj]
    return obj

def upload_ckan(name='IHP-WINS.json', description='Generated catalog', url=ckan_api_url, pathtofile="IHP-WINS.json", resource_id=resource_id, package_id=package_id):
    """
    Uploads a file to CKAN, replacing the specified resource.
    """
    files = {'upload': open(pathtofile, 'rb')}
    headers = {"Authorization": APIdev}
    data_dict = {
        'id': resource_id,
        'package_id': package_id,
        'format': 'JSON',
        'name': name,
        'description': description
    }
    # POST request
    response = requests.post(url, headers=headers, data=data_dict, files=files)
    
    # Logging
    print("Status Code", response.status_code)
    print("JSON Response", response.json())

def generate_and_upload_catalog():
    # 1. Obtain package IDs from the organization
    package_list_url = f"{base_url}package_list"
    package_list_data = get_api_data(package_list_url)

    if package_list_data and 'result' in package_list_data:
        package_ids = package_list_data['result']
    else:
        print("Failed to obtain package IDs.")
        return  # exit the function

    # Create data structures for grouping
    datasets_by_org = defaultdict(lambda: defaultdict(list))

    # Main loop to process resources
    for package_id in package_ids:
        package_url = f"{base_url}package_show?id={package_id}"
        package_data = get_api_data(package_url)

        if not package_data or 'result' not in package_data:
            continue

        org = package_data['result'].get('organization')
        if not org:
            continue
        org_name = org.get('title', 'No Organization')
        dataset_title = package_data['result']['title']
        notes = package_data['result'].get('notes', '')
        resources = package_data['result'].get('resources', [])

        # Filter and process resources
        for resource in resources:
            resource_format = resource.get('format', '').lower()
            if resource_format in [f.lower() for f in formatos_permitidos]:
                formatted_item = format_dataset_item(resource, package_id, notes)
                datasets_by_org[org_name][dataset_title].append(formatted_item)

    # Create the final catalog structure
    catalog = {
        "catalog": []
    }

    # Build the hierarchy
    for org_name, datasets in datasets_by_org.items():
        org_group = {
            "name": org_name,
            "type": "group",
            "members": []
        }

        for dataset_title, items in datasets.items():
            dataset_group = {
                "name": dataset_title,
                "type": "group",
                "members": items
            }
            org_group["members"].append(dataset_group)

        catalog["catalog"].append(org_group)

    # Ensure sets are converted to lists before serialization
    catalog = convert_sets_to_lists(catalog)

    # Write the final configuration file
    output_file = "IHP-WINS"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)

    print(f"Catalog with styles saved to: {output_file}")

    # Upload the generated catalog to CKAN
    print("Uploading catalog to CKAN...")
    upload_ckan()
    print("Upload done.")

# ------------------------------
# Airflow DAG Configuration
# ------------------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'update_catalog_grouped_json',
    default_args=default_args,
    description='Generate and upload IHP-WINS.json every hour',
    schedule_interval='0 * * * *',  # every hour at minute 0
    catchup=False,
)

generate_and_upload_task = PythonOperator(
    task_id='generate_and_upload_catalog',
    python_callable=generate_and_upload_catalog,
    dag=dag,
)
