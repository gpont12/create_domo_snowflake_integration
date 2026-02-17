import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file in parent directory
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# =============================================================================
# CONFIGURATION - Fill in these values before running
# =============================================================================

# Domo Instance (e.g., "mycompany.domo.com")
INSTANCE = ""

# Snowflake Account Configuration
ACCOUNT_NAME = ""           # Internal name for the account
DISPLAY_NAME = ""           # Display name shown in Domo

# Snowflake Connection Details
SNOWFLAKE_ACCOUNT = ""      # Snowflake account identifier
SNOWFLAKE_USERNAME = ""     # Snowflake username
SNOWFLAKE_ROLE = ""         # Snowflake role to use

# Private Key Authentication
PRIVATE_KEY_FILE = ""       # Path to your private key file (e.g., "/path/to/rsa_key.p8")
PASSPHRASE = ""             # Passphrase for the private key (leave empty if none)

# BYOS Integration Configuration
INTEGRATION_FRIENDLY_NAME = ""  # Friendly name for the integration (defaults to DISPLAY_NAME if empty)
INTEGRATION_DESCRIPTION = ""    # Optional description for the integration

# Warehouse Configuration
WAREHOUSE_NAME = ""             # Name of the Snowflake warehouse to assign to the integration

# Warehouse Activities - Available options: "query", "index", "dataflow"
# "query"    - Allows querying data from the warehouse
# "index"    - Allows indexing operations
# "dataflow" - Allows dataflow/ETL operations
WAREHOUSE_ACTIVITIES = ["query", "index", "dataflow"]  # List of activities to enable

# Existing Account ID (optional)
# If you already have a Snowflake account created in Domo, enter the account ID here
# to skip account creation and proceed directly to BYOS integration creation
EXISTING_ACCOUNT_ID = ""  # Leave empty to create a new account

# =============================================================================
# FUNCTIONS
# =============================================================================

def get_private_key_contents(file_path):
    """
    Reads the contents of a private key file and returns it as a string.
    
    Args:
        file_path: Path to the private key file
        
    Returns:
        String contents of the private key file
    """
    try:
        with open(file_path, 'r') as key_file:
            return key_file.read()
    except FileNotFoundError:
        print(f"Error: Private key file not found at '{file_path}'")
        exit(1)
    except PermissionError:
        print(f"Error: Permission denied when reading '{file_path}'")
        exit(1)
    except Exception as e:
        print(f"Error reading private key file: {e}")
        exit(1)


def create_snowflake_account():
    """
    Creates a Snowflake Cloud Amplifier account in Domo using key pair authentication.
    """
    # Validate required configuration
    if not INSTANCE:
        print("Error: INSTANCE is required. Please set it in the configuration section.")
        exit(1)
    if not PRIVATE_KEY_FILE:
        print("Error: PRIVATE_KEY_FILE is required. Please set it in the configuration section.")
        exit(1)
    
    # Get private key contents from file
    private_key = get_private_key_contents(PRIVATE_KEY_FILE)
    
    # Build the API URL
    url = f"https://{INSTANCE}/api/data/v1/accounts"
    
    # Build the payload
    payload = {
        "name": ACCOUNT_NAME,
        "displayName": DISPLAY_NAME,
        "dataProviderType": "snowflakekeypairauthentication",
        "configurations": {
            "account": SNOWFLAKE_ACCOUNT,
            "username": SNOWFLAKE_USERNAME,
            "privateKey": private_key,
            "passPhrase": PASSPHRASE,
            "role": SNOWFLAKE_ROLE
        }
    }
    print(os.getenv("access_token"))
    # Set headers
    headers = {
        "Content-Type": "application/json",
        "X-Domo-Developer-Token": os.getenv("access_token")
    }
    
    # Make the API call
    print(f"Creating Snowflake account '{DISPLAY_NAME}' on {INSTANCE}...")
    print(f"URL: {url}")
    print(f"Payload (private key truncated):")
    
    # Print payload with truncated private key for debugging
    debug_payload = payload.copy()
    debug_payload["configurations"] = payload["configurations"].copy()
    debug_payload["configurations"]["privateKey"] = "[PRIVATE KEY CONTENTS HIDDEN]"
    print(json.dumps(debug_payload, indent=4))
    
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    
    # Handle response
    print(f"\nStatus Code: {response.status_code}")
    
    if response.status_code == 200 or response.status_code == 201:
        print("Success! Account created.")
        print(response.text)
    else:
        print("Error creating account:")
        print(response.text)
    
    return response


def create_byos_integration(account_id, friendly_name=None, description=""):
    """
    Creates a BYOS (Bring Your Own Storage) integration in Domo for the Snowflake account.
    
    Args:
        account_id: The account ID returned from create_snowflake_account()
        friendly_name: Display name for the integration (defaults to DISPLAY_NAME if not provided)
        description: Optional description for the integration
        
    Returns:
        Response object from the API call
    """
    # Use DISPLAY_NAME as default friendly name if not provided
    if friendly_name is None:
        friendly_name = INTEGRATION_FRIENDLY_NAME if INTEGRATION_FRIENDLY_NAME else DISPLAY_NAME
    
    # Use configured description if not provided
    if not description:
        description = INTEGRATION_DESCRIPTION
    
    # Build the API URL
    url = f"https://{INSTANCE}/api/query/v1/byos/accounts"
    
    # Build the payload
    payload = {
        "engine": "SNOWFLAKE",
        "properties": {
            "friendlyName": {
                "key": "friendlyName",
                "configType": "CONFIG",
                "value": friendly_name
            },
            "description": {
                "key": "description",
                "configType": "CONFIG",
                "value": description
            },
            "serviceAccountId": {
                "key": "serviceAccountId",
                "configType": "CONFIG",
                "value": str(account_id)
            },
            "AUTH_METHOD": {
                "key": "AUTH_METHOD",
                "configType": "CONFIG",
                "value": "KEY_PAIR"
            },
            "ADMIN_AUTH_METHOD": {
                "key": "ADMIN_AUTH_METHOD",
                "configType": "CONFIG",
                "value": "KEY_PAIR"
            }
        }
    }
    
    # Set headers
    headers = {
        "Content-Type": "application/json",
        "X-Domo-Developer-Token": os.getenv("access_token")
    }
    
    # Make the API call
    print(f"\nCreating BYOS integration '{friendly_name}' on {INSTANCE}...")
    print(f"URL: {url}")
    print(f"Payload:")
    print(json.dumps(payload, indent=4))
    
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    
    # Handle response
    print(f"\nStatus Code: {response.status_code}")
    
    if response.status_code == 200 or response.status_code == 201:
        print("Success! BYOS integration created.")
        print(response.text)
    else:
        print("Error creating BYOS integration:")
        print(response.text)
    
    return response


def get_available_warehouses(byos_id):
    """
    Retrieves the list of available warehouses for a BYOS integration.
    
    Args:
        byos_id: The BYOS integration ID returned from create_byos_integration()
        
    Returns:
        List of warehouse objects, or None if the request failed
    """
    url = f"https://{INSTANCE}/api/query/v1/byos/warehouses/{byos_id}"
    
    headers = {
        "Content-Type": "application/json",
        "X-Domo-Developer-Token": os.getenv("access_token")
    }
    
    print(f"\nFetching available warehouses for BYOS integration '{byos_id}'...")
    print(f"URL: {url}")
    
    response = requests.get(url, headers=headers)
    
    print(f"\nStatus Code: {response.status_code}")
    
    if response.status_code == 200:
        warehouses = response.json()
        print(f"Found {len(warehouses)} available warehouses")
        return warehouses
    else:
        print("Error fetching warehouses:")
        print(response.text)
        return None


def assign_warehouse_to_integration(byos_id, warehouse_name, activities=None):
    """
    Assigns a warehouse to a BYOS integration with specified activities.
    
    Args:
        byos_id: The BYOS integration ID
        warehouse_name: Name of the warehouse to assign
        activities: List of activities to enable (defaults to WAREHOUSE_ACTIVITIES)
        
    Returns:
        Response object from the API call
    """
    if activities is None:
        activities = WAREHOUSE_ACTIVITIES
    
    # First, get the available warehouses to find the matching one
    warehouses = get_available_warehouses(byos_id)
    
    if warehouses is None:
        print("Error: Could not retrieve available warehouses")
        return None
    
    # Find the warehouse that matches the provided name
    matching_warehouse = None
    for wh in warehouses:
        if wh.get("warehouse") == warehouse_name:
            matching_warehouse = wh
            break
    
    if matching_warehouse is None:
        print(f"\nError: Warehouse '{warehouse_name}' not found in available warehouses.")
        print("Available warehouses:")
        for wh in warehouses:
            print(f"  - {wh.get('warehouse')}")
        return None
    
    print(f"\nFound matching warehouse: {warehouse_name}")
    print(f"  Device: {matching_warehouse.get('device')}")
    print(f"  Size: {matching_warehouse.get('warehouseSizeFriendlyName')}")
    
    # Build the API URL
    url = f"https://{INSTANCE}/api/query/v1/byos/warehouses/{byos_id}"
    
    # Build the payload using the warehouse details from the GET response
    payload = [
        {
            "deviceName": matching_warehouse.get("deviceName"),
            "warehouse": matching_warehouse.get("warehouse"),
            "device": matching_warehouse.get("device"),
            "instanceSize": matching_warehouse.get("instanceSize"),
            "warehouseSizeFriendlyName": matching_warehouse.get("warehouseSizeFriendlyName"),
            "activities": activities
        }
    ]
    
    headers = {
        "Content-Type": "application/json",
        "X-Domo-Developer-Token": os.getenv("access_token")
    }
    
    print(f"\nAssigning warehouse '{warehouse_name}' to BYOS integration...")
    print(f"URL: {url}")
    print(f"Payload:")
    print(json.dumps(payload, indent=4))
    
    response = requests.put(url, data=json.dumps(payload), headers=headers)
    
    print(f"\nStatus Code: {response.status_code}")
    
    if response.status_code == 200 or response.status_code == 201:
        print("Success! Warehouse assigned to integration.")
        print(response.text)
    else:
        print("Error assigning warehouse:")
        print(response.text)
    
    return response


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    # Check if using an existing account or creating a new one
    if EXISTING_ACCOUNT_ID:
        # Use existing account ID
        print("=" * 60)
        print("USING EXISTING ACCOUNT")
        print("=" * 60)
        print(f"Using existing account ID: {EXISTING_ACCOUNT_ID}")
        account_id = EXISTING_ACCOUNT_ID
    else:
        # Step 1: Create the Snowflake account
        account_response = create_snowflake_account()
        
        # Check if account creation succeeded
        if account_response.status_code in [200, 201]:
            account_data = account_response.json()
            account_id = account_data.get("id")
            if not account_id:
                print("Error: Could not extract account ID from response")
                exit(1)
        else:
            print("\nSkipping BYOS integration creation due to account creation failure.")
            exit(1)
    
    # Step 2: Create the BYOS integration
    print("\n" + "=" * 60)
    print("CREATING BYOS INTEGRATION")
    print("=" * 60)
    byos_response = create_byos_integration(account_id)
    
    # Step 3: If BYOS integration created and warehouse name provided, assign warehouse
    if byos_response.status_code in [200, 201] and WAREHOUSE_NAME:
        byos_data = byos_response.json()
        byos_id = byos_data.get("id")
        
        if byos_id:
            print("\n" + "=" * 60)
            print("ASSIGNING WAREHOUSE TO INTEGRATION")
            print("=" * 60)
            assign_warehouse_to_integration(byos_id, WAREHOUSE_NAME)
        else:
            print("Error: Could not extract BYOS integration ID from response")
    elif not WAREHOUSE_NAME:
        print("\nNo WAREHOUSE_NAME configured - skipping warehouse assignment.")
