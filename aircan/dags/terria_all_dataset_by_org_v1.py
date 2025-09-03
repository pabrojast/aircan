import requests
import json
from collections import defaultdict, Counter
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

# Timeout configuration (add timeouts to prevent tasks from hanging)
TIMEOUT_SECONDS = 120

# Retry configuration
retry_strategy = Retry(
    total=2,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)

# CKAN API Configuration
ckan_api_url = "https://ihp-wins.unesco.org/api/3/action/"
APIdev = Variable.get("APIDEV")

# ------------------------------
# Functions
# ------------------------------

def get_api_data(url):
    """
    Performs a GET request to the given URL with retry logic and timeout.
    """
    try:
        response = http.get(url, timeout=TIMEOUT_SECONDS)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error in request to {url}: {response.status_code}")
            return None
    except requests.exceptions.Timeout:
        print(f"Request to {url} timed out after {TIMEOUT_SECONDS} seconds")
        return None
    except Exception as e:
        print(f"Exception during request to {url}: {e}")
        return None

def process_sld_styles(style_url, resource_format):
    """
    Retrieves and processes the SLD styles from the given URL.
    """
    try:
        response = http.get(style_url, timeout=TIMEOUT_SECONDS)
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
                try:
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
                            "title": label,
                            "color": color
                        })
                except Exception as e:
                    print(f"Error parsing SLD XML: {e}")
                    colors = []
                    legend_items = []

                return {
                    'colors': colors,
                    'legend_items': legend_items
                }

            elif resource_format in ['shp', 'shape']:
                # Process vector styles for SHP files using improved categorical approach
                styles = {
                    'legend_items': [],
                    'enum_colors': [],
                    'property_name': None  # To store the property used for styling
                }

                valid_property_name = None  # To store a valid property name

                try:
                    # Enhanced SLD processing similar to the updated plugin
                    rules = root.findall('.//se:Rule', namespaces)
                    if not rules:
                        # Try alternative namespace patterns
                        rules = root.findall('.//sld:Rule', namespaces)
                    if not rules:
                        # Try without namespaces
                        rules = root.findall('.//Rule')
                    
                    print(f"Found {len(rules)} rules in SLD")
                    
                    for i, rule in enumerate(rules):
                        try:
                            # Check for ElseFilter first - these are fallback rules for unmatched values
                            else_filter = rule.find('.//se:ElseFilter', namespaces)
                            if else_filter is not None:
                                # ElseFilter rules don't have specific property values, skip them for styling
                                print(f"Rule {i+1}: Found ElseFilter - skipping for categorical styling")
                                continue
                            
                            # Enhanced rule processing
                            name = rule.find('se:Name', namespaces) or rule.find('sld:Name', namespaces) or rule.find('Name')
                            title = rule.find('.//se:Title', namespaces) or rule.find('.//sld:Title', namespaces) or rule.find('.//Title')
                            
                            # Enhanced color extraction - try multiple patterns
                            fill = (rule.find('.//se:Fill/se:SvgParameter[@name="fill"]', namespaces) or
                                   rule.find('.//sld:Fill/sld:CssParameter[@name="fill"]', namespaces) or
                                   rule.find('.//Fill/CssParameter[@name="fill"]') or
                                   rule.find('.//se:PolygonSymbolizer/se:Fill/se:SvgParameter[@name="fill"]', namespaces))
                            
                            # Enhanced property extraction
                            property_name_element = (rule.find('.//ogc:PropertyName', namespaces) or 
                                                   rule.find('.//PropertyName'))
                            property_value_element = (rule.find('.//ogc:Literal', namespaces) or 
                                                    rule.find('.//Literal'))

                            if fill is not None and fill.text and (name is not None or title is not None):
                                color = fill.text.strip()
                                
                                # Normalize color to hex format
                                if not color.startswith('#'):
                                    if color.startswith('0x'):
                                        color = '#' + color[2:]
                                    elif len(color) == 6 and all(c in '0123456789ABCDEFabcdef' for c in color):
                                        color = '#' + color
                                
                                # Generate label with better fallback
                                if title is not None and title.text and title.text.strip():
                                    label = title.text.strip()
                                elif name is not None and name.text and name.text.strip():
                                    label = name.text.strip()
                                else:
                                    label = f"Style {i+1}"

                                # Skip problematic "else" condition labels
                                if "is ''" in label or "else" in label.lower():
                                    print(f"Rule {i+1}: Skipping problematic label: '{label}'")
                                    continue

                                # Add to legend
                                styles['legend_items'].append({
                                    "title": label,
                                    "color": color.upper()  # Ensure uppercase hex
                                })

                                # Enhanced property processing for categorical data
                                if (property_name_element is not None and property_name_element.text and
                                    property_name_element.text.strip() and property_value_element is not None and
                                    property_value_element.text):

                                    property_value = property_value_element.text.strip()
                                    property_name = property_name_element.text.strip()
                                    
                                    if not valid_property_name:
                                        valid_property_name = property_name
                                    elif valid_property_name != property_name:
                                        print(f"Warning: Multiple property names found: {valid_property_name} vs {property_name}")

                                    # Skip problematic values
                                    if not property_value or "is ''" in property_value or "else" in property_value.lower():
                                        print(f"Rule {i+1}: Skipping problematic property value: '{property_value}'")
                                        continue

                                    # For categorical data (geological codes, etc.), store as-is
                                    # TerriaJS enum mapping can handle non-numeric values
                                    styles['enum_colors'].append({
                                        "value": property_value,
                                        "color": color.upper()  # Use hex color for TerriaJS enum mapping
                                    })
                                    
                                    print(f"Rule {i+1}: Added categorical mapping '{property_value}' -> {color.upper()}")
                                    
                        except Exception as rule_error:
                            print(f"Error processing rule {i+1}: {rule_error}")
                            continue
                            
                except Exception as e:
                    print(f"Error parsing SLD XML: {e}")
                    styles['legend_items'] = []
                    styles['enum_colors'] = []
                    
                # Update property_name with valid_property_name
                styles['property_name'] = valid_property_name
                print(f"Processed SLD: {len(styles['legend_items'])} legend items, {len(styles['enum_colors'])} enum colors, property: {valid_property_name}")
                return styles
            else:
                return None
        else:
            print(f"Error fetching SLD styles from {style_url}: {response.status_code}")
            return None
    except requests.exceptions.Timeout:
        print(f"Request to {style_url} timed out after {TIMEOUT_SECONDS} seconds")
        return None
    except Exception as e:
        print(f"Error processing SLD: {e}")
        return None

def format_dataset_item(resource, package_id, notes, org_info, view_index=0):
    """
    Formats a dataset item, including styles and legends if available.

    Args:
        resource: Resource data dictionary
        package_id: ID of the package
        notes: Additional notes
        org_info: Dictionary containing organization information
        view_index: Index of the Terria view to use (default 0)

    Returns:
        tuple: (formatted_element, total_views)
    """
    resource_id = resource['id']
    resource_format = resource['format'].lower()
    # Fix para recursos sin nombre - generar nombre por defecto (del plugin)
    resource_name = resource.get('name', '').strip()
    if not resource_name or resource_name.lower() in ['', 'none', 'null', 'undefined', 'unnamed resource']:
        # Usar el ID del recurso o un nombre genérico
        resource_name = resource.get('id', f"Resource_{hash(resource.get('url', 'sin_url')) % 10000}")
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
        'description': (notes or '') + '<br/><br/><strong>Dataset URL: </strong> <a href="' + site_url + 'dataset/' + package_id + '">' + site_url + 'dataset/' + package_id + '</a>',
        "info": [
            {
                "name": f"Organization: {org_info['display_name']}",
                "content": f"<img style=\"max-width:300px;width:100%\" alt=\"{org_info['display_name']}\" src=\"{org_info['image_display_url']}\" /><br/>{org_info['description']}<br/>"
            },
            {"name": "File Description", "content": resource_description},
            {"name": "Availability", "content": resource.get("availability", "")},
            {"name": "Last Modified", "content": resource.get("last_modified", "")},
            {"name": "Created", "content": resource.get("created", "")}
        ],
        "infoSectionOrder": [
            f"Organization: {org_info['display_name']}",
            "About Dataset",
            "File Description",
            "Availability",
            "Last Modified",
            "Created"
        ]
    }

    # Obtain Terria view and styles
    view_url = f"{base_url}resource_view_list"
    view_params = {"id": resource_id}
    try:
        response = http.post(view_url, json=view_params)
    except Exception as e:
        print(f"Exception during request to {view_url}: {e}")
        return elemento, 0

    custom_config = None
    style_config = None
    total_views = 0
    view_name = ""

    if response.status_code == 200:
        view_data = response.json()
        terria_views = [view for view in view_data.get('result', [])
                       if view.get('view_type') == 'terria_view']
        total_views = len(terria_views)

        if total_views > 0:
            if view_index >= total_views:
                view_index = 0  # Reset to first view if index is out of range

            selected_view = terria_views[view_index]

            custom_config = selected_view.get('custom_config')
            style_url = selected_view.get('style')
            view_name = selected_view.get('title', '')

            # Modify resource name if there are multiple views
            if total_views > 1:
                resource_name = f"{resource_name} - {view_name}"
                print(f"Resource name: {resource_name}")
                elemento['name'] = resource_name
                elemento['id'] = f"{resource_id}-{view_index}"

            # Process styles if available
            if style_url and style_url != 'NA':
                style_config = process_sld_styles(style_url, resource_format)
    else:
        print(f"Error obtaining views for resource {resource_id}: {response.status_code}")
        print(response.text)

    # Apply styles from SLD if available
    if style_config:
        if resource_format in ['cog', 'tif', 'tiff', 'geotiff']:
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
            # For vector data (SHP files) using improved categorical styling
            if style_config['enum_colors'] and style_config['property_name']:
                property_name = style_config['property_name']
                
                # Determine if data is numeric or categorical like in the updated plugin
                numeric_count = 0
                categorical_count = 0
                
                for enum_item in style_config['enum_colors']:
                    try:
                        float(enum_item['value'])
                        numeric_count += 1
                    except (ValueError, TypeError):
                        categorical_count += 1
                
                # Use enum mapping for categorical data (which is typical for geological codes)
                use_categorical = categorical_count > numeric_count
                
                if use_categorical:
                    # Configure for categorical data using enum mapping
                    elemento['styles'] = [{
                        "id": "sld-style",
                        "title": f"SLD Style ({property_name})",
                        "color": {
                            "mapType": "enum",
                            "colorColumn": property_name,
                            "enumColors": style_config['enum_colors'],
                            "nullColor": "#808080"
                        }
                    }]
                    elemento['activeStyle'] = "sld-style"
                    print(f"Applied categorical enum styling with {len(style_config['enum_colors'])} mappings on column '{property_name}'")
                else:
                    # Configure for numeric data using bin mapping
                    bin_maximums = []
                    bin_colors = []
                    
                    # Sort enum_colors by numeric value for proper bin mapping
                    sorted_enum_colors = sorted(style_config['enum_colors'], 
                                              key=lambda x: float(x['value']) if x['value'].replace('.','',1).replace('-','',1).isdigit() else float('inf'))
                    
                    for enum_item in sorted_enum_colors:
                        try:
                            bin_maximums.append(float(enum_item['value']))
                            bin_colors.append(enum_item['color'])
                        except (ValueError, TypeError):
                            continue
                    
                    if bin_maximums:
                        elemento['styles'] = [{
                            "id": "sld-style", 
                            "title": f"SLD Style ({property_name})",
                            "color": {
                                "mapType": "bin",
                                "colorColumn": property_name,
                                "binMaximums": bin_maximums,
                                "binColors": bin_colors,
                                "nullColor": "#808080"
                            }
                        }]
                        elemento['activeStyle'] = "sld-style"
                        print(f"Applied numeric bin styling with {len(bin_maximums)} bins on column '{property_name}'")
                
                # Common properties for both categorical and numeric
                elemento['opacity'] = 0.8
                
                # GeoJsonTraits for shapefile rendering improvements
                # These properties help with 3D positioning and rendering performance
                elemento['clampToGround'] = True  # Default per TerriaJS docs, helps with geological layers
                
                # Only add forceCesiumPrimitives if we have complex styling that might benefit
                if len(style_config['enum_colors']) > 10:
                    elemento['forceCesiumPrimitives'] = True
                
                # Add legends
                elemento['legends'] = [
                    {
                        "title": "Legend", 
                        "items": style_config['legend_items']
                    }
                ]
            else:
                # If we have legend items but no proper styling, try to create basic categorical styling
                if style_config['legend_items'] and style_config['enum_colors']:
                    print(f"Attempting to create basic categorical styling with {len(style_config['legend_items'])} legend items")
                    # Try to create a basic style with available data
                    basic_enum_colors = []
                    for item in style_config['legend_items']:
                        basic_enum_colors.append({
                            "value": item["title"],
                            "color": item["color"]
                        })
                    
                    if basic_enum_colors:
                        # Create a basic categorical style
                        elemento['styles'] = [{
                            "id": "basic-categorical-style",
                            "title": "Basic Categorical Style",
                            "color": {
                                "mapType": "enum",
                                "colorColumn": style_config['property_name'] or "UNKNOWN_COLUMN",
                                "enumColors": basic_enum_colors,
                                "nullColor": "#808080"
                            }
                        }]
                        elemento['activeStyle'] = "basic-categorical-style"
                        elemento['opacity'] = 0.8
                        elemento['clampToGround'] = True
                        print(f"Created basic categorical style with {len(basic_enum_colors)} enum colors")
                        
                        # Add legends
                        elemento['legends'] = [
                            {
                                "title": "Legend", 
                                "items": style_config['legend_items']
                            }
                        ]
                    else:
                        # Fallback to legend-only styling
                        elemento['legends'] = [
                            {
                                "title": "Legend",
                                "items": style_config['legend_items']
                            }
                        ]
                        elemento['opacity'] = 0.8
                        elemento['clampToGround'] = True
                        print(f"Applied legend-only styling with {len(style_config['legend_items'])} items")
                elif style_config['legend_items']:
                    # Only legends available
                    elemento['legends'] = [
                        {
                            "title": "Legend",
                            "items": style_config['legend_items']
                        }
                    ]
                    elemento['opacity'] = 0.8
                    elemento['clampToGround'] = True
                    print(f"Applied legend-only styling with {len(style_config['legend_items'])} items")
    
    # Process custom_config if available (can be combined with SLD styles)
    if custom_config and custom_config not in ['NA', '']:
        try:
            # Extract the 'start' or 'share' parameter from the URL
            parsed_url = urllib.parse.urlparse(custom_config)
            fragment = parsed_url.fragment
            
            if fragment.startswith('share='):
                # Case of URL with #share (gist)
                gist_id = fragment.split('=g-')[1]
                gist_url = f'https://gist.githubusercontent.com/pabrojast/{gist_id}/raw/Terriajs-usercatalog.json'
                try:
                    response = http.get(gist_url, timeout=TIMEOUT_SECONDS)
                    if response.status_code == 200:
                        decoded_param = response.text
                    else:
                        decoded_param = '{}'
                except Exception as e:
                    print(f"Error fetching gist config: {e}")
                    decoded_param = '{}'
            else:
                # Original case with #start
                start_param = fragment.split('=', 1)[1]
                decoded_param = urllib.parse.unquote(start_param)
            
            # Parse the JSON
            custom_data = json.loads(decoded_param)
            
            # Helper function to decode names (like in the plugin)
            def decode_names(obj):
                if isinstance(obj, dict):
                    new_dict = {}
                    for key, value in obj.items():
                        # Decode the key if it contains '+'
                        new_key = urllib.parse.unquote_plus(key) if '+' in key else key
                        
                        if isinstance(value, dict):
                            # If there are specific fields that contain names
                            if 'name' in value:
                                value['name'] = urllib.parse.unquote_plus(value['name'])
                            if 'title' in value:
                                value['title'] = urllib.parse.unquote_plus(value['title'])
                            if 'legend' in value and isinstance(value['legend'], dict):
                                if 'title' in value['legend']:
                                    value['legend']['title'] = urllib.parse.unquote_plus(value['legend']['title'])
                            
                            value = decode_names(value)
                        elif isinstance(value, list):
                            value = [urllib.parse.unquote_plus(item) if isinstance(item, str) else decode_names(item) for item in value]
                        elif isinstance(value, str) and '+' in value:
                            value = urllib.parse.unquote_plus(value)
                            
                        new_dict[new_key] = value
                    return new_dict
                elif isinstance(obj, list):
                    return [decode_names(item) for item in obj]
                return obj

            # Decode all names in the custom config
            custom_data = decode_names(custom_data)
            
            # Apply styles from custom config
            for init_source in custom_data.get('initSources', []):
                if 'models' in init_source:
                    for model_key, model_value in init_source['models'].items():
                        # Apply existing legends and styles from custom config
                        # These override any SLD styles that were already applied
                        if 'legends' in model_value:
                            elemento['legends'] = model_value['legends']
                        if 'styles' in model_value:
                            elemento['styles'] = model_value['styles']
                            if 'activeStyle' in model_value:
                                elemento['activeStyle'] = model_value['activeStyle']
                        if 'renderOptions' in model_value:
                            elemento['renderOptions'] = model_value['renderOptions']
                        if 'opacity' in model_value:
                            elemento['opacity'] = model_value['opacity']

        except Exception as e:
            print(f"Error processing custom config for {resource_id}: {e}")

    return elemento, total_views

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

def write_catalog_file(catalog_data, filename, entity_name=None, entity_type=None):
    """
    Writes a catalog to a JSON file with additional configuration.
    """
    # Determine what to include in the output based on filename
    if filename == 'IHP-WINS.json':
        # For 'IHP-WINS.json', only include 'catalog' and add name
        final_data = {
            "catalog": catalog_data['catalog'],
            "name": "IHP-WINS"
        }
    else:
        # Collect workbench items and models when necessary
        workbench_items = []
        container_ids = {}

        # Collect all items for the workbench
        def collect_items(members, parent_path="/"):
            for item in members:
                if item['type'] == 'group':
                    current_path = f"{parent_path}//{item['name']}"
                    container_ids[current_path] = {
                        "isOpen": True,
                        "knownContainerUniqueIds": [parent_path],
                        "type": "group"
                    }
                    collect_items(item['members'], current_path)
                else:
                    workbench_items.append(item['id'])
                    container_ids[item['id']] = {
                        "show": True,
                        "isOpenInWorkbench": True,
                        "knownContainerUniqueIds": [parent_path],
                        "type": item['type']
                    }

        # Process catalog members
        collect_items(catalog_data['catalog'][0]['members'])

        # Now determine what to include
        if filename.startswith('tag_'):
            # For tag files, include catalog, workbench, models, but without 'version' and 'initSources'
            final_data = {
                "catalog": catalog_data['catalog'],
                "workbench": workbench_items,
                "models": {
                    **container_ids,
                    "/": {"type": "group"}
                },
                "viewerMode": "3dSmooth",
                "focusWorkbenchItems": True,
                "baseMaps": {
                    "defaultBaseMapId": "basemap-positron",
                    "previewBaseMapId": "basemap-positron"
                }
            }
        elif entity_type == 'organization':
            # For organization files, include 'previewedItemId'
            final_data = {
                "catalog": catalog_data['catalog'],
                "previewedItemId": f"//{entity_name}"
            }
        else:
            # For other files, include only 'catalog'
            final_data = {
                "catalog": catalog_data['catalog']
            }

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)

def upload_ckan(file_path, entity_name=None, entity_type='organization'):
    """
    Uploads a catalog file to CKAN.

    Args:
        file_path (str): Path to the file to upload
        entity_name (str): Name of the organization or tag (optional)
        entity_type (str): Type of the entity ('organization' or 'tag')
    """
    headers = {"Authorization": APIdev}

    # Get the ID of the package
    package_show_url = f"{ckan_api_url}package_show"
    params = {"id": "terriajs-map-catalog-in-json-format"}

    try:
        response = requests.get(package_show_url, params=params, headers=headers, timeout=TIMEOUT_SECONDS)
        if response.status_code == 200:
            package_data = response.json()
            package_id = package_data['result']['id']
            resources = package_data['result'].get('resources', [])

            # Determine the final name and description based on entity type
            if entity_name:
                if entity_type == 'organization':
                    final_name = f"{entity_name}.json"
                    # Description in English for organizations
                    description = (
                        f"TerriaJS configuration file for {entity_name}'s map catalog. "
                        f"This JSON file contains the visualization settings and data layers "
                        f"specific to {entity_name}'s geospatial resources."
                    )
                elif entity_type == 'tag':
                    final_name = f"tag_{entity_name}.json"  # Added 'tag_' prefix
                    # Description in English for tags
                    description = (
                        f"TerriaJS configuration file for datasets tagged with '{entity_name}'. "
                        f"This JSON file contains the visualization settings and data layers "
                        f"for all datasets associated with the '{entity_name}' tag."
                    )
                else:
                    final_name = f"{entity_name}.json"
                    description = "TerriaJS configuration file."
            else:
                final_name = "IHP-WINS.json"
                # Description in English for the main file
                description = (
                    "Main TerriaJS configuration file for the IHP-WINS map catalog. "
                    "This JSON file contains the consolidated visualization settings "
                    "and data layers from all contributing organizations and tags."
                )

            # Look for an existing resource with the same name
            existing_resource = next((r for r in resources if r['name'] == final_name), None)

            files = {'upload': open(file_path, 'rb')}
            data_dict = {
                'package_id': package_id,
                'format': 'JSON',
                'name': final_name,
                'description': description
            }

            if existing_resource:
                # Update the existing resource
                data_dict['id'] = existing_resource['id']
                update_url = f"{ckan_api_url}resource_update"
                response = requests.post(update_url, headers=headers, data=data_dict, files=files, timeout=TIMEOUT_SECONDS)
            else:
                # Create a new resource
                create_url = f"{ckan_api_url}resource_create"
                response = requests.post(create_url, headers=headers, data=data_dict, files=files, timeout=TIMEOUT_SECONDS)

            print(f"Status Code: {response.status_code}")
            print(f"Response: {response.json()}")

        else:
            print(f"Failed to retrieve package: {response.status_code}")
    except requests.exceptions.Timeout:
        print(f"Upload to CKAN timed out after {TIMEOUT_SECONDS} seconds")
    except Exception as e:
        print(f"Error in upload_ckan: {e}")

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
    org_info_cache = {}

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
        org_id = org.get('name')
        dataset_title = package_data['result']['title']
        notes = package_data['result'].get('notes', '')
        resources = package_data['result'].get('resources', [])

        # Retrieve organization details if not already cached
        if org_name not in org_info_cache:
            org_url = f"{base_url}organization_show?id={org_id}"
            org_data = get_api_data(org_url)
            if org_data and 'result' in org_data:
                org_result = org_data['result']
                org_info = {
                    'display_name': org_result.get('display_name', org_name),
                    'description': org_result.get('description', ''),
                    'image_display_url': org_result.get('image_display_url', '')
                }
                org_info_cache[org_name] = org_info
            else:
                org_info_cache[org_name] = {'display_name': org_name, 'description': '', 'image_display_url': ''}

        org_info = org_info_cache[org_name]

        # Filter and process resources
        for resource in resources:
            resource_format = resource.get('format', '').lower()
            if resource_format in [f.lower() for f in formatos_permitidos]:
                formatted_item, total_views = format_dataset_item(resource, package_id, notes, org_info)
                datasets_by_org[org_name][dataset_title].append(formatted_item)

                if total_views > 1:
                    view_index = 1
                    while view_index < total_views:
                        formatted_item, _ = format_dataset_item(resource, package_id, notes, org_info, view_index)
                        datasets_by_org[org_name][dataset_title].append(formatted_item)
                        view_index += 1

    # Create the final catalog structure
    catalog = {
        "catalog": []
    }

    # Build the hierarchy and create individual organization files
    for org_name, datasets in datasets_by_org.items():
        org_info = org_info_cache[org_name]
        # Create structure for the individual organization file
        org_catalog = {
            "catalog": [{
                "name": org_name,
                "type": "group",
                "members": [],
                "info": [
                    {
                        "name": f"Organization: {org_info['display_name']}",
                        "content": f"<img style=\"max-width:300px;width:100%\" alt=\"{org_info['display_name']}\" src=\"{org_info['image_display_url']}\" /><br/>{org_info['description']}<br/>"
                    }
                ],
                "infoSectionOrder": [
                    f"Organization: {org_info['display_name']}"
                ]
            }]
        }

        # Create structure for the main catalog
        org_group = {
            "name": org_name,
            "type": "group",
            "members": [],
            "info": [
                {
                    "name": f"Organization: {org_info['display_name']}",
                    "content": f"<img style=\"max-width:300px;width:100%\" alt=\"{org_info['display_name']}\" src=\"{org_info['image_display_url']}\" /><br/>{org_info['description']}<br/>"
                }
            ],
            "infoSectionOrder": [
                f"Organization: {org_info['display_name']}"
            ]
        }

        for dataset_title, items in datasets.items():
            dataset_group = {
                "name": dataset_title,
                "type": "group",
                "members": items,
                "info": [
                    {
                        "name": f"About Dataset",
                        "content": notes or ''
                    }
                ],
                "infoSectionOrder": [
                    "About Dataset"
                ]
            }
            org_group["members"].append(dataset_group)
            org_catalog["catalog"][0]["members"].append(dataset_group)

        catalog["catalog"].append(org_group)

        # Save and upload individual organization file
        org_filename = f"catalog_{org_name.lower().replace(' ', '_')}.json"
        write_catalog_file(convert_sets_to_lists(org_catalog), org_filename, entity_name=org_name, entity_type='organization')
        print(f"Catalog for {org_name} saved to: {org_filename}")
        upload_ckan(org_filename, entity_name=org_name, entity_type='organization')

    # Save and upload consolidated file
    write_catalog_file(convert_sets_to_lists({"catalog": catalog["catalog"]}), "IHP-WINS.json")
    print("Consolidated catalog saved to: IHP-WINS.json")
    upload_ckan("IHP-WINS.json")

def generate_and_upload_catalog_by_tag():
    # 1. Obtain the list of tags
    tag_list_url = f"{base_url}tag_list"
    tag_list_data = get_api_data(tag_list_url)
    if tag_list_data and 'result' in tag_list_data:
        tag_names = tag_list_data['result']
    else:
        print("Failed to obtain tag list.")
        return

    # Create data structures for grouping
    datasets_by_tag = defaultdict(lambda: defaultdict(list))

    # Main loop to process tags
    for tag_name in tag_names:
        # Omit the tag 'IHP-WINS' to prevent conflict with IHP-WINS.json
        if tag_name == 'IHP-WINS':
            continue

        tag_show_url = f"{base_url}tag_show?id={urllib.parse.quote_plus(tag_name)}&include_datasets=True"
        tag_data = get_api_data(tag_show_url)
        if not tag_data or 'result' not in tag_data:
            continue

        datasets = tag_data['result'].get('packages', [])
        for dataset in datasets:
            package_id = dataset['id']
            dataset_title = dataset['title']
            notes = dataset.get('notes', '')
            resources = dataset.get('resources', [])
            org = dataset.get('organization', {})
            org_info = {
                'display_name': org.get('title', ''),
                'description': org.get('description', ''),
                'image_display_url': org.get('image_display_url', '')
            }

            # Process resources
            for resource in resources:
                resource_format = resource.get('format', '').lower()
                if resource_format in [f.lower() for f in formatos_permitidos]:
                    formatted_item, total_views = format_dataset_item(resource, package_id, notes, org_info)
                    datasets_by_tag[tag_name][dataset_title].append(formatted_item)

                    # Handle multiple views
                    if total_views > 1:
                        view_index = 1
                        while view_index < total_views:
                            formatted_item, _ = format_dataset_item(resource, package_id, notes, org_info, view_index)
                            datasets_by_tag[tag_name][dataset_title].append(formatted_item)
                            view_index += 1

    # Create the final catalog structure
    catalog = {
        "catalog": []
    }

    # Build the hierarchy and create individual tag files
    for tag_name, datasets in datasets_by_tag.items():
        # Create structure for the individual tag catalog
        tag_catalog = {
            "catalog": [{
                "name": tag_name,
                "type": "group",
                "members": []
            }]
        }

        # Create structure for the main catalog
        tag_group = {
            "name": tag_name,
            "type": "group",
            "members": []
        }

        for dataset_title, items in datasets.items():
            dataset_group = {
                "name": dataset_title,
                "type": "group",
                "members": items
            }
            tag_group["members"].append(dataset_group)
            tag_catalog["catalog"][0]["members"].append(dataset_group)

        # Add to the main catalog
        catalog["catalog"].append(tag_group)

        # Save and upload individual tag catalog
        tag_filename = f"tag_{tag_name.lower().replace(' ', '_')}.json"
        write_catalog_file(convert_sets_to_lists(tag_catalog), tag_filename)
        print(f"Catalog for tag {tag_name} saved to: {tag_filename}")
        upload_ckan(tag_filename, entity_name=tag_name, entity_type='tag')

    print("Consolidated catalog by tags saved to: Skipped")

# ------------------------------
# Airflow DAG Configuration
# ------------------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),  # Add timeout to each task
    'dagrun_timeout': timedelta(minutes=60),     # Add timeout to the entire DAG
    'max_active_runs': 1,  # Allow only 1 active run at a time
    'concurrency': 2       # Maximum number of task instances to run concurrently
}

dag = DAG(
    'update_catalog_grouped_json',
    default_args=default_args,
    description='Generate and upload IHP-WINS.json and tag files every hour',
    schedule_interval='0 * * * *',  # every hour at minute 0
    catchup=False,
)

generate_and_upload_catalog_task = PythonOperator(
    task_id='generate_and_upload_catalog',
    python_callable=generate_and_upload_catalog,
    dag=dag,
)

generate_and_upload_catalog_by_tag_task = PythonOperator(
    task_id='generate_and_upload_catalog_by_tag',
    python_callable=generate_and_upload_catalog_by_tag,
    dag=dag,
)

generate_and_upload_catalog_task >> generate_and_upload_catalog_by_tag_task
