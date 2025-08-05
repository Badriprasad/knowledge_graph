from cosmosgremlinclient import CosmosGraphClient
from collibrarestassets import get_all_collibra_assets_basic_auth
from collibrarestrelations import get_all_collibra_relations
import logging
from azure.cosmos import exceptions # Re-added this import for specific error handling
from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables from .env file
# Configure logging for gremlinpython to see connection details and query execution
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


if __name__ == "__main__":
    # --- Configuration ---
    # IMPORTANT: Replace these with your actual Cosmos DB Gremlin API details
    # You can find these in your Azure Portal -> Cosmos DB Account -> Keys
    # The hostname is typically like: <your-account-name>.gremlin.cosmos.azure.com
    # The database_name is the name of your Cosmos DB database (e.g., 'graphdb').
    # The collection_name is the name of your graph collection (e.g., 'graphcontainer').
    # The password is one of your primary/secondary keys.

    COSMOS_DB_HOSTNAME = os.getenv("COSMOS_DB_HOSTNAME")
    COSMOS_DB_DATABASE_NAME = os.getenv("COSMOS_DB_DATABASE_NAME")
    COSMOS_DB_COLLECTION_NAME = os.getenv("COSMOS_DB_COLLECTION_NAME")
    COSMOS_DB_PASSWORD = os.getenv("COSMOS_DB_PASSWORD")
    COSMOS_DB_PORT = int(os.getenv("COSMOS_DB_PORT", 443))
    COSMOSDB_PARTITION_KEY_FIELD = os.getenv("COSMOSDB_PARTITION_KEY_FIELD", "resourceType")


    if "YOUR_COSMOS_DB_HOSTNAME" in COSMOS_DB_HOSTNAME or \
       "YOUR_COSMOS_DB_DATABASE_NAME" in COSMOS_DB_DATABASE_NAME or \
       "YOUR_COSMOS_DB_COLLECTION_NAME" in COSMOS_DB_COLLECTION_NAME or \
       "YOUR_COSMOS_DB_PRIMARY_KEY" in COSMOS_DB_PASSWORD:
        print("WARNING: Please replace placeholder credentials in the example usage.")
        print("Set environment variables COSMOS_DB_HOSTNAME, COSMOS_DB_DATABASE_NAME, COSMOS_DB_COLLECTION_NAME, COSMOS_DB_PASSWORD")
        print("Or directly edit the script with your Cosmos DB details.")
        # Exit if placeholders are still present to prevent connection errors
        # exit() # Uncomment this line if you want the script to exit immediately

    client = None
    try:
        # 1. Initialize the client
        client = CosmosGraphClient(
            hostname=COSMOS_DB_HOSTNAME,
            port=COSMOS_DB_PORT,
            database_name=COSMOS_DB_DATABASE_NAME, # Pass database name
            collection_name=COSMOS_DB_COLLECTION_NAME, # Pass collection name
            password=COSMOS_DB_PASSWORD,
            partition_key_field=COSMOSDB_PARTITION_KEY_FIELD, # Optional: Specify partition key field
        )

        # 2. Connect to the database
        client.connect()

        # 3. Clean up existing data (optional, for testing)
        print("\n--- Cleaning up existing data (optional) ---")
        # client.run_query("g.V().drop()")
        drop_limit = 10
        # Drop vertices in batches to avoid overwhelming the server
        print(f"Dropping vertices in batches of {drop_limit}...")
        while True:
            # Check if there are more vertices to drop
            remaining = client.run_query("g.V().count()")[0]
            print(f"Remaining vertices to drop: {remaining}")
            if remaining == 0:
                break
            client.run_query(f"g.V().limit({drop_limit}).drop()")
        print("Existing data cleaned up.")

        # 4. Fetch Collibra Assets
        print("\n--- Fetching Collibra Assets ---")
        all_assets = get_all_collibra_assets_basic_auth()
        if all_assets:
            print(f"Retrieved {len(all_assets)} assets from Collibra.")
        else:
            print("No assets retrieved or an error occurred.")

        # 5. Create vertices for all the assets
        asset_keys_to_extract = ['id', 'createdBy', 'createdOn', 'lastModifiedBy', 'lastModifiedOn', 'system', 'resourceType', 'name', 'displayName', 'articulationScore', 'excludedFromAutoHyperlinking', 'avgRating', 'ratingsCount']
        domain_keys_to_extract = ['id', 'resourceType', 'resourceDiscriminator', 'name']
        type_keys_to_extract = ['id', 'resourceType', 'resourceDiscriminator', 'name']
        status_keys_to_extract = ['id', 'resourceType', 'resourceDiscriminator', 'name']
        print("\n--- Creating Vertices for Collibra Assets ---")
        # Iterate through all assets and create vertices for each
        for asset in all_assets:
            asset_data = {}
            domain_data = {}
            type_data = {}
            status_data = {}


            for key in asset_keys_to_extract:
                try:
                    asset_data[key] = asset[key]
                except KeyError:
                    print(f"Warning: Key '{key}' not found in the original dictionary. Skipping.")
            print(f"Asset Data: {asset_data}")

            print("\n--- Creating Vertices for Collibra Assets ---")
            asset_vertex = client.create_vertex(
                label=asset_data.get('resourceType', 'Asset'),  # Use resourceType as label, default to 'Asset'
                properties=asset_data
            )
            print(f"Created Collibra Asset Vertex: {asset_vertex}")
            
            if 'domain' in asset:
                for key in domain_keys_to_extract:
                    try:
                        domain_data[key] = asset['domain'][key]
                    except KeyError:
                        print(f"Warning: Key '{key}' not found in the domain dictionary. Skipping.")
                print(f"Domain Data: {domain_data}")

                # Create Domain Vertex
                domain_vertex = client.upsert_vertex(
                    label=domain_data.get('resourceType', 'Domain'),
                    id_value=domain_data.get('id'),
                    unique_property_name='id',
                    unique_property_value=domain_data.get('id'),
                    properties=domain_data
                )
                print(f"Created Domain Vertex: {domain_vertex}")
                print(f"Creating Edge from Domain to Asset: {asset['id']} with Domain Vertex ID: {domain_vertex['id']}")
                domain_edge = client.create_edge(
                    from_vertex_id=domain_vertex['id'],
                    to_vertex_id=asset['id'],  # Assuming asset['id'] is the ID of the asset vertex
                    label='belongsTo_domain',
                    properties={'since': asset.get('createdOn', 'unknown')}
                )
                print(f"Created Edge from Domain to Asset: {domain_edge}")

            if 'type' in asset:
                for key in type_keys_to_extract:
                    try:
                        type_data[key] = asset['type'][key]
                    except KeyError:
                        print(f"Warning: Key '{key}' not found in the type dictionary. Skipping.")
                print(f"Type Data: {type_data}")

                # Create Type Vertex
                type_vertex = client.upsert_vertex(
                    label=type_data.get('resourceType', 'Type'),
                    id_value=type_data.get('id'),
                    unique_property_name='id',
                    unique_property_value=type_data.get('id'),
                    properties=type_data
                )
                print(f"Created Type Vertex: {type_vertex}")
                print(f"Creating Edge from Type to Asset: {asset['id']} with Type Vertex ID: {type_vertex['id']}")
                type_edge = client.create_edge(
                    from_vertex_id=type_vertex['id'],
                    to_vertex_id=asset['id'],  # Assuming asset['id'] is the ID of the asset vertex
                    label='belongsTo_type',
                    properties={'since': asset.get('createdOn', 'unknown')}
                )
                print(f"Created Edge from Type to Asset: {type_edge}")

            if 'status' in asset:
                for key in status_keys_to_extract:
                    try:
                        status_data[key] = asset['status'][key]
                    except KeyError:
                        print(f"Warning: Key '{key}' not found in the status dictionary. Skipping.")
                print(f"Status Data: {status_data}")

                # Create Status Vertex
                status_vertex = client.upsert_vertex(
                    label=status_data.get('resourceType', 'Status'),
                    id_value=status_data.get('id'),
                    unique_property_name='id',
                    unique_property_value=status_data.get('id'),
                    properties=status_data
                )
                print(f"Created Status Vertex: {status_vertex}")
                print(f"Creating Edge from Status to Asset: {asset['id']} with Status Vertex ID: {status_vertex['id']}")
                status_edge = client.create_edge(
                    from_vertex_id=status_vertex['id'],
                    to_vertex_id=asset['id'],  # Assuming asset['id'] is the ID of the asset vertex
                    label='belongsTo_status',
                    properties={'since': asset.get('createdOn', 'unknown')}
                )
                print(f"Created Edge from Status to Asset: {status_edge}")

        # 5. Fetch Collibra Relations
        print("\n--- Fetching Collibra Relations ---")
        all_relations = get_all_collibra_relations()
        if all_relations:
            print(f"Retrieved {len(all_relations)} relationships from Collibra.")
            for rel in all_relations:
                source_id = rel.get('source_id')
                target_id = rel.get('target_id')
                relation_type_id = rel.get('relation_type_id')

                if source_id and target_id:
                    print(f"Creating Edge from Source ID: {source_id} to Target ID: {target_id} with Relation Type ID: {relation_type_id}")
                    edge = client.create_edge(
                        from_vertex_id=source_id,
                        to_vertex_id=target_id,
                        label='relatedTo',
                        properties={'relationTypeId': relation_type_id}
                    )
                    print(f"Created Edge: {edge}")
        else:
            print("No relationships found.")














        # 4. Create Vertices
        # print("\n--- Creating Vertices ---")
        # person1 = client.create_vertex(
        #     label='person',
        #     properties={'name': 'Alice', 'age': 30, 'city': 'New York', 'resourceType':'Asset'}
        # )
        # print(f"Created person: {person1}")
        # alice_id = person1[0]['id'] if person1 else None

        # person2 = client.create_vertex(
        #     label='person',
        #     properties={'name': 'Bob', 'age': 25, 'city': 'London', 'resourceType':'Asset'}
        # )
        # print(f"Created person: {person2}")
        # bob_id = person2[0]['id'] if person2 else None

        # city1 = client.create_vertex(
        #     label='city',
        #     properties={'name': 'New York', 'country': 'USA', 'resourceType':'Asset'}
        # )
        # print(f"Created city: {city1}")
        # new_york_id = city1[0]['id'] if city1 else None

        # # 5. Create Edges
        # print("\n--- Creating Edges ---")
        # if alice_id and bob_id:
        #     edge1 = client.create_edge(
        #         from_vertex_id=alice_id,
        #         to_vertex_id=bob_id,
        #         label='knows',
        #         properties={'since': 2020}
        #     )
        #     print(f"Created edge (Alice knows Bob): {edge1}")
        # else:
        #     print("Could not create 'knows' edge: Alice or Bob vertex ID missing.")

        # if alice_id and new_york_id:
        #     edge2 = client.create_edge(
        #         from_vertex_id=alice_id,
        #         to_vertex_id=new_york_id,
        #         label='livesIn'
        #     )
        #     print(f"Created edge (Alice lives in New York): {edge2}")
        # else:
        #     print("Could not create 'livesIn' edge: Alice or New York vertex ID missing.")

        # # 6. Run Arbitrary Queries
        # print("\n--- Running Arbitrary Queries ---")

        # # Get all vertices
        # all_vertices = client.run_query("g.V()")
        # print("\nAll Vertices:")
        # for v in all_vertices:
        #     print(v)

        # # Get all edges
        # all_edges = client.run_query("g.E()")
        # print("\nAll Edges:")
        # for e in all_edges:
        #     print(e)

        # # Find people older than 28
        # older_people = client.run_query("g.V().hasLabel('person').has('age', gt(28))")
        # print("\nPeople older than 28:")
        # for p in older_people:
        #     print(p)

        # # Find who Alice knows
        # alice_knows = client.run_query(f"g.V('{alice_id}').out('knows')")
        # print(f"\nWho Alice knows:")
        # for p in alice_knows:
        #     print(p)

    except Exception as e:
        print(f"An error occurred during the example execution: {e}")
        print(f"  Error Type: {type(e).__name__}")
        print(f"  Error Message: {e}")
        # If it's a CosmosHttpResponseError, try to print more details
        if isinstance(e, exceptions.CosmosHttpResponseError):
            print(f"  Status Code: {e.status_code}")
            print(f"  Message: {e.message}")
            if e.status_code == 401:
                print("Check your master key and host settings.")
            elif e.status_code == 404:
                print("Check if the database or graph name is correct.")
    finally:
        # 7. Close the connection
        if client:
            client.close()
            print("\nConnection closed.")