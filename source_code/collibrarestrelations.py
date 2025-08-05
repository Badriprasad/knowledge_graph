import requests
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file
# Collibra API details
COLLIBRA_URL = os.getenv("COLLIBRA_URL")
COLLIBRA_USERNAME = os.getenv("COLLIBRA_USERNAME")
COLLIBRA_PASSWORD = os.getenv("COLLIBRA_PASSWORD")

# The id of the asset to retrieve relations for
COLLIBRA_ASSET_ID = os.getenv("COLLIBRA_ASSET_ID")

# --- Authentication ---
# For Basic Authentication (replace with your chosen method)
auth = (COLLIBRA_USERNAME, COLLIBRA_PASSWORD)
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# --- Generic Pagination Function ---
def get_paginated_data(endpoint, params):
    """
    Fetches all data from a paginated API endpoint.
    """
    all_results = []
    total = 1  # Initialize total to enter the loop
    offset = 0
    limit = 1000  # A good default for many APIs

    print(f"Fetching data from: {endpoint}...")

    while len(all_results) < total:
        # Update the offset for the current page
        params['offset'] = offset
        params['limit'] = limit

        try:
            # response = requests.get(endpoint, auth=auth, headers=headers, params=params)
            response = requests.get(endpoint, auth=auth, headers=headers)
            response.raise_for_status()  # Raise an exception for bad status codes
            print("response:", response.json())
            data = response.json()
            
            # Update total from the first response
            if offset == 0:
                total = data.get('total', 0)
                print(f"Total items to fetch: {total}")
            
            results = data.get('results', [])
            all_results.extend(results)
            
            print(f"  -> Fetched {len(results)} items. Total fetched: {len(all_results)} / {total}")
            
            offset += len(results)
            
            # Optional: Add a small delay to avoid hitting rate limits
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from {endpoint}: {e}")
            return []
    
    return all_results

# --- API Endpoints ---
def get_assets_by_type(asset_type_id):
    """
    Fetches all assets of a given type, using pagination.
    """
    endpoint = f"{COLLIBRA_URL}/rest/2.0/assets"
    params = {
        "typeId": asset_type_id
    }
    return get_paginated_data(endpoint, params)

def get_asset_relations(asset_id):
    """
    Fetches all relations for a specific asset, using pagination.
    """
    endpoint = f"{COLLIBRA_URL}/rest/2.0/relations"
    params = {
        "sourceId": asset_id
    }
    return get_paginated_data(endpoint, params)


def get_all_collibra_relations():
    asset_relationships = {}
    # for i, asset in enumerate(assets):
    #     asset_id = asset['id']
    #     asset_name = asset['name']
        
    #     print(f"Processing asset {i+1}/{len(assets)}: {asset_name}")
    
    asset_id = COLLIBRA_ASSET_ID
    relations = get_asset_relations(asset_id)
    asset_relationships = []

    if relations:
        # You might want to save this data more cleanly.
        # For this example, we'll just store the IDs.
        # asset_relationships[asset_name] = []
        for i, rel in enumerate(relations):
            print("rel:", rel)
            relation_id = rel['id']
            source_asset_id = rel['source']['id']
            target_asset_id = rel['target']['id']
            relation_type_id = rel['type']['id']
            print(f"  Relation from {source_asset_id} to {target_asset_id} of type {relation_type_id}")
            # Store the relationship in a dictionary
            print("i:", i)
            asset_relationships.append({
                "source_id": source_asset_id,
                "target_id": target_asset_id,
                "relation_type_id": relation_type_id
            })
    
    print("asset_relationships:", asset_relationships)
    return asset_relationships

    
# --- Main Script ---
if __name__ == "__main__":
    asset_relationships = get_all_collibra_relations()
    if asset_relationships:
        # You can now process the 'asset_relationships' list
        print("\nFirst 5 relationships (example):")
        for rel in asset_relationships[:5]:
            print(f"  Source ID: {rel.get('source_id')}, Target ID: {rel.get('target_id')}, Relation Type ID: {rel.get('relation_type_id')}")

        print(f"Total relationships retrieved: {len(asset_relationships)}")
    else:
        print("No relationships found.")