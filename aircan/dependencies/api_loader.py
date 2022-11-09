import logging
import json
import requests
from urllib.parse import urljoin

from aircan.dependencies.postgres_loader import aircan_status_update
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from frictionless import Resource
from frictionless.plugins.remote import RemoteControl
from frictionless import describe

from aircan.dependencies.utils import (
    AirflowCKANException, 
    frictionless_to_ckan_schema,
    ckan_to_frictionless_schema,
    chunky,
    DatastoreEncoder)

from aircan import RequestError

def fetch_and_read(resource_dict, site_url, api_key):
    """
    Fetch and read source type, metadata and schema from
    ckan resource URl.
    """
    logging.info('Fetching resource data from url')
    try:
        resource = describe(path=resource_dict['path'], type="resource")
        status_dict = { 
                'res_id': resource_dict['ckan_resource_id'],
                'state': 'progress',
                'message': 'Fetching datafile from {0}.'.format(resource_dict['path']),
            }
        aircan_status_update(site_url, api_key, status_dict)
        return {'sucess': True, 'resource': resource}

    except Exception as err:
        raise AirflowCKANException(
             'Failed to fetch data file from {0}.'.format(resource_dict['path']), str(err))

def compare_schema(site_url, ckan_api_key, res_dict, schema):
    """
    compare old datastore schema with new schema to know wheather 
    it changed or not.
    retrun type: list
    reutrn value: [recreate_datastore_table_flag, old_schema]
    """

    res_id = res_dict['ckan_resource_id']
    logging.info('fetching old data dictionary {0}'.format(res_id))
    try:
        url = urljoin(site_url, '/api/3/action/datastore_search')
        response = requests.get(url,
                        params={'resource_id': res_id, 'limit': 0},
                        headers={'Authorization': ckan_api_key}
                    )
        if response.status_code == 200:
            resource_json = response.json()
            old_schema_dict = resource_json['result'].get('fields', [])
            
            # filter old and new schema and compare them if they are identical
            old_schema_columns = [fields['id'] for fields in old_schema_dict if fields['id'] != '_id']
            new_schema_columns = [field_name['name'] for field_name in schema]
            have_same_columns = set(old_schema_columns) == set(new_schema_columns)

            if have_same_columns:
                # override schema type with user defined type from data dictionary or old schema
                type_has_changed = False
                for idx, old_field in enumerate(old_schema_dict):
                    # if field name is the same and type is different then override it
                    if old_field.get('info', {}).get('type', False):
                        if ckan_to_frictionless_schema(old_field['type']) != old_field.get('info', {}).get('type', False):
                            logging.info('type has changed for field {0},'.format(old_field['id']))
                            old_schema_dict[idx]['type'] = old_field.get('info', {}).get('type', old_field['type'])
                            type_has_changed = True
                        
                # if type has changed for append enabled resource there is chance of previous data being deleted
                # so throw an error
                if res_dict['datastore_append_enabled'] and type_has_changed:
                    raise AirflowCKANException('You cannot change type of existing fields in append enabled resource.')
                elif type_has_changed:
                    #  have same columns but column type is changed so recreate table with overriding schemas
                    return [True, old_schema_dict]
                else:
                    # Both columns and types are same so no need to recreate table
                    return [False, old_schema_dict]
            else:
                return [True, None]
        else:
            return [True, None]

    except AirflowCKANException as err:
        raise err
    except Exception as err: 
        raise AirflowCKANException(
            'Failed to fetch data dictionary for {0}'.format(res_id), str(err))

def delete_datastore_table(data_resource_id, ckan_api_key, ckan_site_url):
    header = {'Authorization': ckan_api_key}
    try:
        response = requests.post(
            urljoin(ckan_site_url, '/api/3/action/datastore_delete'),
            headers=header,
            json={
                "resource_id": data_resource_id,
                'force': True
            }
        )
        if response.status_code == 200 or response.status_code == 404:
            logging.info('Table was deleted successfuly')

            status_dict = { 
                'res_id': data_resource_id,
                'state': 'progress',
                'message': 'New table detected, existing table is being deleted.'
            }
            aircan_status_update(ckan_site_url, ckan_api_key, status_dict)
            return {'success': True, 'message': 'Table deleted successfully.'}
        else:
            raise RequestError(response.json()['error'])
    except Exception as err:
        raise AirflowCKANException('Failed to clean up table.', str(err))

def create_datastore_table(data_resource_id, resource_schema, old_schema, ckan_api_key, ckan_site_url):
    logging.info('Create Datastore Table method starts')
   
    schema = []

    for f in resource_schema:
        field = {
            'id': f['name'],
            'type':  frictionless_to_ckan_schema(f['type'])
        }

        # Preserve old data dictionary if it exists for matching fields
        if old_schema:
            old_data_dictionary = [item for item in old_schema if f['name'] == item['id']][0].get('info', False)
            if old_data_dictionary:
                field['info'] = old_data_dictionary
                field['type'] = frictionless_to_ckan_schema(old_data_dictionary.get('type', field['type']))
        schema.append(field)
    
    data_dict = dict(
        # resource={'package_id': 'my-first-dataset', 'name' : 'Test1'},
        resource_id=data_resource_id,
        fields=schema)
    data_dict['records'] = None  # just create an empty table
    data_dict['force'] = True
    try:
        response = requests.post(
            urljoin(ckan_site_url, '/api/3/action/datastore_create'),
            headers={'Authorization': ckan_api_key},
            json=data_dict
        )
        if response.status_code == 200:
            logging.info('Table was created successfuly')
            status_dict = { 
                'res_id': data_resource_id,
                'state': 'progress',
                'message': 'Determined headers and types: {0}'.format(json.dumps(data_dict['fields']))
            }
            aircan_status_update(ckan_site_url, ckan_api_key, status_dict)
            return {'success': True, 'message': 'Table created Successfully.'}
        else:
            raise RequestError(response.json()['error'])
    except Exception as err:
        raise AirflowCKANException('Failed to create table in datastore.', str(err))

def load_resource_via_api(resource_dict, ckan_api_key, ckan_site_url, chunk_size):
    """
    Push records in CKAN datastore
    """
    logging.info("Loading resource via API lib")
    try:
        # Push data untill records is empty 
        control = RemoteControl(http_timeout=50)
        have_unique_keys = resource_dict.get('datastore_unique_keys', False)
        method = 'insert'
        if have_unique_keys:
            method = 'upsert'
        status_dict = {
                    'res_id': resource_dict['ckan_resource_id'],
                    'state': 'progress',
                    'message': 'Data ingestion is in progress.'
                }
        aircan_status_update(ckan_site_url, ckan_api_key, status_dict)
        with Resource(resource_dict['path'], control=control) as resource:
            count = 0
            for i, records in enumerate(chunky(resource.row_stream, int(chunk_size))):
                count += len(records)
                payload = {
                        'resource_id': resource_dict['ckan_resource_id'],
                        'force': True,
                        'records': list(records),
                        'method': method
                    }
                url = urljoin(ckan_site_url, '/api/3/action/datastore_upsert')
                response = requests.post(url,
                                data=json.dumps(payload, cls=DatastoreEncoder),
                                headers={'Content-Type': 'application/json',
                                        'Authorization': ckan_api_key})
                response.raise_for_status()
                if response.status_code == 200:
                     logging.info('Ingested {number} of records.'.format(number=i))
                else:
                    raise requests.HTTPError('Failed to make request on CKAN API.')
        if count:
            logging.info('Successfully ingested {n} records of data "{res_id}".'.format(
                    n=count, res_id=resource_dict['ckan_resource_id']))
            status_dict = { 
                    'res_id': resource_dict['ckan_resource_id'],
                    'state': 'complete',
                    'message': 'Data ingestion completed successfully for "{res_id}".'.format(
                     res_id=resource_dict['ckan_resource_id'])
                }
            aircan_status_update(ckan_site_url, ckan_api_key, status_dict)
            return {'success': True}
            
    except Exception as err:
        raise AirflowCKANException('Data ingestion has failed.', str(err))
