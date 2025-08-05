import os
from gremlin_python.driver import client, serializer
from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import T

class CosmosGraphClient:
    """
    A synchronous client for connecting to Azure Cosmos DB Graph API
    using the Gremlin Python driver.

    This class provides methods to:
    - Establish and close a connection to the graph database.
    - Create new vertices with specified labels and properties.
    - Create new edges between existing vertices with specified labels and properties.
    - Run arbitrary Gremlin queries.
    """

    def __init__(self, hostname, port=443, database_name="", collection_name="", password="",
                 traversal_source='g', enable_ssl=True, partition_key_field=""):
        """
        Initializes the CosmosGraphClient.

        Args:
            hostname (str): The hostname of your Cosmos DB Gremlin endpoint.
                            Usually in the format: <your-account>.gremlin.cosmos.azure.com
            port (int): The port for the Gremlin endpoint (default is 443).
            database_name (str): The name of your Cosmos DB database.
            collection_name (str): The name of your Cosmos DB graph collection.
            password (str): The primary or secondary key of your Cosmos DB account.
            traversal_source (str): The traversal source (default is 'g').
            enable_ssl (bool): Whether to enable SSL for the connection (default is True).
                               Note: This parameter is kept for consistency but SSL is
                                     implicitly handled by 'wss://' in the URL.
        """
        self.hostname = hostname
        self.port = port
        self.database_name = database_name
        self.collection_name = collection_name
        self.password = password
        self.traversal_source = traversal_source
        self.enable_ssl = enable_ssl # Kept for __init__ signature, but not directly used in client init
        self.gremlin_client = None
        self.connection = None
        self.g = None # Traversal object
        self.partition_key_field = partition_key_field

        print(f"Initializing CosmosGraphClient for {self.hostname}:{self.port}")

    def _format_properties(self, properties):
        """
        Helper method to format a dictionary of properties into a Gremlin string
        suitable for .property() steps.

        Args:
            properties (dict): A dictionary of key-value pairs for properties.

        Returns:
            str: A string representation of Gremlin .property() steps.
        """
        if not properties:
            return ""
        props_str = ""
        for key, value in properties.items():
            # Ensure key is a string
            formatted_key = str(key)
            if isinstance(value, str):
                # Escape single quotes in string values for Gremlin
                escaped_value = value.replace("'", "\\'")
                props_str += f".property('{formatted_key}', '{escaped_value}')"
            elif isinstance(value, bool):
                # Gremlin uses 'true'/'false' for booleans
                props_str += f".property('{formatted_key}', {str(value).lower()})"
            else:
                # For numbers, None, etc., directly embed the value
                props_str += f".property('{formatted_key}', {value})"
        return props_str

    def connect(self):
        """
        Establishes a synchronous connection to the Cosmos DB Gremlin endpoint.
        """
        if self.gremlin_client:
            print("Already connected.")
            return

        try:
            # Construct the connection URL
            # For Cosmos DB, the URL format is typically wss://<hostname>:<port>/gremlin
            # Using 'wss://' implicitly enables SSL. The 'enable_ssl' argument is not
            # directly supported by the gremlin-python client's constructor.
            connection_url = f"wss://{self.hostname}:{self.port}/gremlin"

            # Cosmos DB expects the username in the format /dbs/<database>/colls/<collection>
            # The 'username' parameter in the client is used for SASL authentication.
            cosmos_username = f"/dbs/{self.database_name}/colls/{self.collection_name}"

            # Initialize the Gremlin client
            # The client handles the WebSocket connection and sends/receives Gremlin requests.
            self.gremlin_client = client.Client(
                url=connection_url,
                traversal_source=self.traversal_source,
                username=cosmos_username, # Updated username format
                password=self.password,
                message_serializer=serializer.GraphSONSerializersV2d0(),
            )
            print("Gremlin client initialized.")

            # Create a remote connection and a graph traversal source.
            # This is necessary for building complex traversals using 'g'.
            self.connection = DriverRemoteConnection(
                url=connection_url,
                traversal_source=self.traversal_source,
                username=cosmos_username, # Updated username format
                password=self.password,
                message_serializer=serializer.GraphSONSerializersV2d0(),
            )
            self.g = traversal().withRemote(self.connection)
            print("Successfully connected to Cosmos DB Graph.")

        except GremlinServerError as e:
            print(f"Gremlin Server Error during connection: {e}")
            self.gremlin_client = None
            self.connection = None
            self.g = None
            raise
        except Exception as e:
            print(f"An unexpected error occurred during connection: {e}")
            self.gremlin_client = None
            self.connection = None
            self.g = None
            raise

    def close(self):
        """
        Closes the connection to the Cosmos DB Gremlin endpoint.
        """
        if self.gremlin_client:
            try:
                self.gremlin_client.close()
                print("Gremlin client closed.")
            except Exception as e:
                print(f"Error closing Gremlin client: {e}")
            self.gremlin_client = None

        if self.connection:
            try:
                self.connection.close()
                print("Remote connection closed.")
            except Exception as e:
                print(f"Error closing remote connection: {e}")
            self.connection = None
        self.g = None

    def _execute_query(self, query_string):
        """
        Executes a raw Gremlin query string synchronously.

        Args:
            query_string (str): The Gremlin query string to execute.

        Returns:
            list: A list of results from the Gremlin query.
                  Returns an empty list if no results or on error.
        """
        if not self.gremlin_client:
            print("Error: Not connected to the graph database. Call connect() first.")
            return []

        print(f"\nExecuting Gremlin query: {query_string}")
        try:
            # Submit the query and wait for the result synchronously using .result()
            result_set = self.gremlin_client.submit(query_string)
            results = result_set.all().result()
            print("Query executed successfully.")
            return results
        except GremlinServerError as e:
            print(f"Gremlin Server Error during query execution: {e}")
            return []
        except Exception as e:
            print(f"An unexpected error occurred during query execution: {e}")
            return []

    def create_vertex(self, label, properties=None):
        """
        Creates a new vertex in the graph.

        Args:
            label (str): The label for the new vertex (e.g., 'person', 'city').
            properties (dict, optional): A dictionary of key-value pairs for vertex properties.
                                         Defaults to None.

        Returns:
            list: The result from the Gremlin query (usually the created vertex object).
        """
        if not self.g:
            print("Error: Graph traversal source 'g' not initialized. Call connect() first.")
            return []

        # Start building the Gremlin query for adding a vertex
        query = f"g.addV('{label}')"

        # Append properties to the query string
        query += self._format_properties(properties)

        return self._execute_query(query)

    def _execute_gremlin_query(self, query: str):
        """
        Executes a Gremlin query against the Cosmos DB graph.

        Args:
            query (str): The Gremlin query string.

        Returns:
            list: A list of results from the Gremlin query.
            None: If an error occurs during query execution.
        """
        try:
            # Submit the Gremlin query asynchronously and wait for results
            callback = self.gremlin_client.submitAsync(query)
            results = callback.result().all().result()
            return results
        except Exception as e:
            print(f"Error executing Gremlin query: {query}\nError: {e}")
            return None
        
    def _escape_gremlin_string(self, value: str) -> str:
        """
        Escapes single quotes in a string to be safely used in Gremlin queries.
        """
        return value.replace("'", "\\'")
    
    def _find_vertex(self, label: str, unique_property_name: str, unique_property_value: str):
        """
        Checks if a vertex exists based on its label and a unique property.

        Args:
            label (str): The label of the vertex.
            unique_property_name (str): The name of the property that is unique for this vertex type.
            unique_property_value (str): The value of the unique property.

        Returns:
            dict or None: The found vertex if it exists, otherwise None.
        """
        escaped_label = self._escape_gremlin_string(label)
        escaped_prop_name = self._escape_gremlin_string(unique_property_name)
        escaped_prop_value = self._escape_gremlin_string(unique_property_value)

        # Construct a Gremlin query to find the vertex
        # We use .has() for both label and the unique property to narrow down the search.
        # The partition_key_field is crucial for efficient lookup in Cosmos DB.
        # Assuming unique_property_name is NOT the partition_key_field, we also include the partition key.
        # If unique_property_name IS the partition_key_field, then the .has() for label is still good.
        find_query = (
            f"g.V().hasLabel('{escaped_label}')"
            f".has('{self.partition_key_field}', '{escaped_label}')" # Always include partition key
            f".has('{escaped_prop_name}', '{escaped_prop_value}')"
            f".limit(1)" # We only need to find one
        )

        results = self._execute_gremlin_query(find_query)
        if results and len(results) > 0:
            return results[0]  # Return the first found vertex
        return None


    def upsert_vertex(self, label: str, id_value: str, unique_property_name: str, unique_property_value: str, properties: dict):
        """
        Upserts a vertex into the Cosmos DB graph using a two-step process:
        1. Checks if the vertex exists based on label and a unique property.
        2. If it exists, updates its properties; otherwise, creates a new vertex.

        Args:
            label (str): The label of the vertex (e.g., "person", "product").
            id_value (str): A unique identifier for the vertex (e.g., "john_doe_123").
                            This will be set as the 'id' property of the vertex.
            unique_property_name (str): The name of a property that uniquely identifies this vertex
                                        within its label (e.g., "email", "productCode").
            unique_property_value (str): The value of the unique property.
            properties (dict): A dictionary of additional properties for the vertex.
                               This dictionary should include `unique_property_name` and its value,
                               and can also include other properties. Do NOT include 'id' or 'label' here.

        Returns:
            dict or None: The upserted vertex (or its ID) if successful, None otherwise.
        """
        if not label or not id_value or not unique_property_name or unique_property_value is None:
            print("Error: label, id_value, unique_property_name, and unique_property_value are required for upserting a vertex.")
            return None

        # Create a mutable copy of properties to avoid modifying the original dictionary
        # and ensure 'id' and 'label' are not included in generic properties
        filtered_properties = {k: v for k, v in properties.items() if k not in ['id', 'label']}

        # Ensure the unique property is also in the filtered_properties dictionary for creation/update
        # This is important if unique_property_name was not originally in the properties dict
        if unique_property_name not in filtered_properties:
            filtered_properties[unique_property_name] = unique_property_value


        # Step 1: Check if the vertex exists
        existing_vertex = self._find_vertex(label, unique_property_name, unique_property_value)

        gremlin_query = ""
        if existing_vertex:
            # Step 2a: Vertex exists, update its properties
            vertex_id = existing_vertex.get('id')
            if not vertex_id:
                print(f"Error: Found vertex has no 'id' property. Cannot update. Vertex: {existing_vertex}")
                return None

            escaped_vertex_id = self._escape_gremlin_string(vertex_id)
            escaped_label = self._escape_gremlin_string(label)

            # Start the update query by selecting the existing vertex
            gremlin_query = (
                f"g.V('{escaped_vertex_id}')"
                f".has('{self.partition_key_field}', '{escaped_label}')" # Ensure partition key is used for update
            )

            # Add property updates from the filtered_properties
            for key, value in filtered_properties.items():
                if key == unique_property_name or key == self.partition_key_field or key == 'id':
                    # Skip the unique property and partition key, as they are not updated
                    continue
                escaped_key = self._escape_gremlin_string(key)
                if isinstance(value, str):
                    escaped_value = self._escape_gremlin_string(value)
                    gremlin_query += f".property('{escaped_key}', '{escaped_value}')"
                else:
                    gremlin_query += f".property('{escaped_key}', {value})" # For non-string values

            print(f"Updating existing vertex with ID: {vertex_id}")

        else:
            # Step 2b: Vertex does not exist, create a new one
            escaped_label = self._escape_gremlin_string(label)
            escaped_id_value = self._escape_gremlin_string(id_value)

            # Start the creation query
            gremlin_query = f"g.addV('{escaped_label}')" \
                            f".property('id', '{escaped_id_value}')" \
                            f".property('{self.partition_key_field}', '{escaped_label}')" # Set partition key

            # Add all properties from the filtered_properties
            # for key, value in filtered_properties.items():
            for key, value in filtered_properties.items():
                if key == self.partition_key_field or key == 'id':
                    continue
                escaped_key = self._escape_gremlin_string(key)
                if isinstance(value, str):
                    escaped_value = self._escape_gremlin_string(value)
                    gremlin_query += f".property('{escaped_key}', '{escaped_value}')"
                else:
                    gremlin_query += f".property('{escaped_key}', {value})"

            print(f"Creating new vertex with ID: {id_value}")

        results = self._execute_gremlin_query(gremlin_query)

        if results and len(results) > 0:
            return results[0] # Return the upserted vertex object
        return None

    def create_edge(self, from_vertex_id, to_vertex_id, label, properties=None):
        """
        Creates a new edge between two existing vertices.

        Args:
            from_vertex_id: The ID of the source vertex.
            to_vertex_id: The ID of the target vertex.
            label (str): The label for the new edge (e.g., 'knows', 'livesIn').
            properties (dict, optional): A dictionary of key-value pairs for edge properties.
                                         Defaults to None.

        Returns:
            list: The result from the Gremlin query (usually the created edge object).
        """
        if not self.g:
            print("Error: Graph traversal source 'g' not initialized. Call connect() first.")
            return []

        # Gremlin query to add an edge between two vertices by their IDs
        # Note: Cosmos DB typically requires IDs to be strings.
        query = (
            f"g.V('{from_vertex_id}')"
            f".addE('{label}')"
            f".to(g.V('{to_vertex_id}'))"
        )

        # Append properties to the query string
        query += self._format_properties(properties)

        return self._execute_query(query)

    def run_query(self, query):
        """
        Executes an arbitrary Gremlin query string.

        Args:
            query (str): The full Gremlin query string to execute.

        Returns:
            list: A list of results from the Gremlin query.
                  Returns an empty list if no results or on error.
        """
        return self._execute_query(query)

# Example Usage (replace with your actual Cosmos DB credentials and endpoint)
# if __name__ == "__main__":
#     # --- Configuration ---
#     # IMPORTANT: Replace these with your actual Cosmos DB Gremlin API details
#     # You can find these in your Azure Portal -> Cosmos DB Account -> Keys
#     # The hostname is typically like: <your-account-name>.gremlin.cosmos.azure.com
#     # The database_name is the name of your Cosmos DB database (e.g., 'graphdb').
#     # The collection_name is the name of your graph collection (e.g., 'graphcontainer').
#     # The password is one of your primary/secondary keys.
#     COSMOS_DB_HOSTNAME = os.environ.get("COSMOS_DB_HOSTNAME", "YOUR_COSMOS_DB_HOSTNAME.gremlin.cosmos.azure.com")
#     COSMOS_DB_DATABASE_NAME = os.environ.get("COSMOS_DB_DATABASE_NAME", "YOUR_COSMOS_DB_DATABASE_NAME")
#     COSMOS_DB_COLLECTION_NAME = os.environ.get("COSMOS_DB_COLLECTION_NAME", "YOUR_COSMOS_DB_COLLECTION_NAME")
#     COSMOS_DB_PASSWORD = os.environ.get("COSMOS_DB_PASSWORD", "YOUR_COSMOS_DB_PRIMARY_KEY")
#     COSMOS_DB_PORT = 443 # Default Gremlin port for Cosmos DB

#     if "YOUR_COSMOS_DB_HOSTNAME" in COSMOS_DB_HOSTNAME or \
#        "YOUR_COSMOS_DB_DATABASE_NAME" in COSMOS_DB_DATABASE_NAME or \
#        "YOUR_COSMOS_DB_COLLECTION_NAME" in COSMOS_DB_COLLECTION_NAME or \
#        "YOUR_COSMOS_DB_PRIMARY_KEY" in COSMOS_DB_PASSWORD:
#         print("WARNING: Please replace placeholder credentials in the example usage.")
#         print("Set environment variables COSMOS_DB_HOSTNAME, COSMOS_DB_DATABASE_NAME, COSMOS_DB_COLLECTION_NAME, COSMOS_DB_PASSWORD")
#         print("Or directly edit the script with your Cosmos DB details.")
#         # Exit if placeholders are still present to prevent connection errors
#         # exit() # Uncomment this line if you want the script to exit immediately

#     client = None
#     try:
#         # 1. Initialize the client
#         client = CosmosGraphClient(
#             hostname=COSMOS_DB_HOSTNAME,
#             port=COSMOS_DB_PORT,
#             database_name=COSMOS_DB_DATABASE_NAME, # Pass database name
#             collection_name=COSMOS_DB_COLLECTION_NAME, # Pass collection name
#             password=COSMOS_DB_PASSWORD
#         )

#         # 2. Connect to the database
#         client.connect()

#         # 3. Clean up existing data (optional, for testing)
#         print("\n--- Cleaning up existing data (optional) ---")
#         client.run_query("g.V().drop()")

#         # 4. Create Vertices
#         print("\n--- Creating Vertices ---")
#         person1 = client.create_vertex(
#             label='person',
#             properties={'name': 'Alice', 'age': 30, 'city': 'New York'}
#         )
#         print(f"Created person: {person1}")
#         alice_id = person1[0]['id'] if person1 else None

#         person2 = client.create_vertex(
#             label='person',
#             properties={'name': 'Bob', 'age': 25, 'city': 'London'}
#         )
#         print(f"Created person: {person2}")
#         bob_id = person2[0]['id'] if person2 else None

#         city1 = client.create_vertex(
#             label='city',
#             properties={'name': 'New York', 'country': 'USA'}
#         )
#         print(f"Created city: {city1}")
#         new_york_id = city1[0]['id'] if city1 else None

#         # 5. Create Edges
#         print("\n--- Creating Edges ---")
#         if alice_id and bob_id:
#             edge1 = client.create_edge(
#                 from_vertex_id=alice_id,
#                 to_vertex_id=bob_id,
#                 label='knows',
#                 properties={'since': 2020}
#             )
#             print(f"Created edge (Alice knows Bob): {edge1}")
#         else:
#             print("Could not create 'knows' edge: Alice or Bob vertex ID missing.")

#         if alice_id and new_york_id:
#             edge2 = client.create_edge(
#                 from_vertex_id=alice_id,
#                 to_vertex_id=new_york_id,
#                 label='livesIn'
#             )
#             print(f"Created edge (Alice lives in New York): {edge2}")
#         else:
#             print("Could not create 'livesIn' edge: Alice or New York vertex ID missing.")

#         # 6. Run Arbitrary Queries
#         print("\n--- Running Arbitrary Queries ---")

#         # Get all vertices
#         all_vertices = client.run_query("g.V()")
#         print("\nAll Vertices:")
#         for v in all_vertices:
#             print(v)

#         # Get all edges
#         all_edges = client.run_query("g.E()")
#         print("\nAll Edges:")
#         for e in all_edges:
#             print(e)

#         # Find people older than 28
#         older_people = client.run_query("g.V().hasLabel('person').has('age', gt(28))")
#         print("\nPeople older than 28:")
#         for p in older_people:
#             print(p)

#         # Find who Alice knows
#         alice_knows = client.run_query(f"g.V('{alice_id}').out('knows')")
#         print(f"\nWho Alice knows:")
#         for p in alice_knows:
#             print(p)

#     except Exception as e:
#         print(f"An error occurred during the example execution: {e}")
#     finally:
#         # 7. Close the connection
#         if client:
#             client.close()
#             print("\nConnection closed.")
