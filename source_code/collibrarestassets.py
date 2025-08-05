import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables from .env file
# Collibra API details
COLLIBRA_URL = os.getenv("COLLIBRA_URL")
COLLIBRA_USERNAME = os.getenv("COLLIBRA_USERNAME")
COLLIBRA_PASSWORD = os.getenv("COLLIBRA_PASSWORD")

def get_all_collibra_assets_basic_auth():
    """
    Connects to Collibra using Basic Authentication and retrieves all assets.
    """
    assets = []
    offset = 0
    limit = 100  # Max limit per request, adjust based on Collibra's documentation

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    print("Connecting to Collibra with Basic Authentication...")

    while True:
        # Construct the API URL with pagination parameters
        api_url = f"{COLLIBRA_URL}/rest/2.0/assets?offset={offset}&limit={limit}"
        print(f"Fetching assets from: {api_url}")
        
        try:
            response = requests.get(api_url, headers=headers, auth=(COLLIBRA_USERNAME, COLLIBRA_PASSWORD))
            response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)

            data = response.json()
            current_assets = data.get("results", [])
            total = data.get("total", 0)

            assets.extend(current_assets)

            print(f"Retrieved {len(assets)} of {total} assets...")

            if len(assets) >= total:
                break  # All assets retrieved
            else:
                offset += len(current_assets)

        except requests.exceptions.RequestException as e:
            print(f"Error connecting to Collibra: {e}")
            print(f"Response content: {response.text}")
            break
        except json.JSONDecodeError:
            print(f"Error decoding JSON from response: {response.text}")
            break

    print(f"Successfully retrieved {len(assets)} assets.")
    return assets

if __name__ == "__main__":
    all_assets = get_all_collibra_assets_basic_auth()
    if all_assets:
        # You can now process the 'all_assets' list
        print("\nFirst 5 assets (example):")
        for asset in all_assets[:5]:
            print(f"  ID: {asset.get('id')}, Name: {asset.get('name')}, Type: {asset.get('type', {}).get('name')}")
            # print(f"  Domain: {asset.get('domain', {}).get('id')}, Domain: {asset.get('type', {}).get('name')}")
    else:
        print("No assets retrieved or an error occurred.")