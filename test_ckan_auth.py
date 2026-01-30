#!/usr/bin/env python3
"""
Script de diagnóstico para probar la autenticación CKAN
"""
import requests
import json
import tempfile
import os

# Configuración
API_KEY = "e95ebe05-8f4d-4dbb-8bde-fe1435d10090"
CKAN_URL = "https://ihp-wins.unesco.org"

def test_auth():
    print("=" * 60)
    print("Probando autenticación CKAN")
    print("=" * 60)
    
    # Test 1: Probar resource_update con X-CKAN-API-Key
    print("\n1. Probando resource_update con 'X-CKAN-API-Key'...")
    headers1 = {"X-CKAN-API-Key": API_KEY}
    
    # Crear archivo temporal de prueba
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump({"test": "data"}, f)
        temp_file = f.name
    
    try:
        # Primero obtener un resource_id existente
        response = requests.get(
            f"{CKAN_URL}/api/3/action/package_show",
            params={"id": "terriajs-map-catalog-in-json-format"},
            headers=headers1,
            timeout=30
        )
        if response.status_code == 200:
            resources = response.json().get('result', {}).get('resources', [])
            if resources:
                resource_id = resources[0]['id']
                print(f"   Resource ID encontrado: {resource_id}")
                
                # Intentar actualizar
                with open(temp_file, 'rb') as upload_file:
                    files = {'upload': upload_file}
                    data = {
                        'id': resource_id,
                    }
                    update_response = requests.post(
                        f"{CKAN_URL}/api/3/action/resource_update",
                        headers=headers1,
                        data=data,
                        files=files,
                        timeout=60
                    )
                    print(f"   Status: {update_response.status_code}")
                    print(f"   Response: {update_response.text[:500]}")
            else:
                print("   No hay resources en el paquete")
        else:
            print(f"   Error obteniendo paquete: {response.status_code}")
    except Exception as e:
        print(f"   Error: {e}")
    finally:
        os.unlink(temp_file)

    # Test 2: Probar con Authorization header
    print("\n2. Probando resource_update con 'Authorization'...")
    headers2 = {"Authorization": API_KEY}
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump({"test": "data"}, f)
        temp_file = f.name
    
    try:
        response = requests.get(
            f"{CKAN_URL}/api/3/action/package_show",
            params={"id": "terriajs-map-catalog-in-json-format"},
            headers=headers2,
            timeout=30
        )
        if response.status_code == 200:
            resources = response.json().get('result', {}).get('resources', [])
            if resources:
                resource_id = resources[0]['id']
                
                with open(temp_file, 'rb') as upload_file:
                    files = {'upload': upload_file}
                    data = {
                        'id': resource_id,
                    }
                    update_response = requests.post(
                        f"{CKAN_URL}/api/3/action/resource_update",
                        headers=headers2,
                        data=data,
                        files=files,
                        timeout=60
                    )
                    print(f"   Status: {update_response.status_code}")
                    print(f"   Response: {update_response.text[:500]}")
    except Exception as e:
        print(f"   Error: {e}")
    finally:
        os.unlink(temp_file)

    # Test 3: Verificar el usuario asociado a la API key
    print("\n3. Verificando permisos del usuario...")
    try:
        # Listar organizaciones donde el usuario tiene permisos
        response = requests.get(
            f"{CKAN_URL}/api/3/action/organization_list_for_user",
            headers={"X-CKAN-API-Key": API_KEY},
            timeout=30
        )
        print(f"   Status organization_list_for_user: {response.status_code}")
        if response.status_code == 200:
            orgs = response.json().get('result', [])
            print(f"   Organizaciones del usuario: {[o.get('name') for o in orgs]}")
            for org in orgs:
                print(f"     - {org.get('name')}: capacity={org.get('capacity')}")
        else:
            print(f"   Response: {response.text[:300]}")
    except Exception as e:
        print(f"   Error: {e}")

    print("\n" + "=" * 60)
    print("Diagnóstico completado")
    print("=" * 60)

if __name__ == "__main__":
    test_auth()
