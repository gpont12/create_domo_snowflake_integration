# Create Domo Snowflake Integration

A Python script to automate the creation of Snowflake Cloud Amplifier accounts and BYOS (Bring Your Own Storage) integrations in Domo using key pair authentication.

## Features

- Creates Snowflake Cloud Amplifier accounts in Domo with key pair authentication
- Creates BYOS integrations linked to the Snowflake account
- Automatically assigns warehouses to integrations with configurable activities

## Prerequisites

- Python 3.7+
- A Domo instance with API access
- A Snowflake account with key pair authentication configured
- A Domo Developer Token (access token)

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/gpont12/create_domo_snowflake_integration.git
   cd create_domo_snowflake_integration
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file in the parent directory with your Domo access token:
   ```
   access_token=your_domo_developer_token_here
   ```

## Configuration

Edit the configuration section at the top of `create_snowflake_account_integration.py`:

```python
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
PRIVATE_KEY_FILE = ""       # Path to your private key file
PASSPHRASE = ""             # Passphrase for the private key (leave empty if none)

# BYOS Integration Configuration
INTEGRATION_FRIENDLY_NAME = ""  # Friendly name for the integration
INTEGRATION_DESCRIPTION = ""    # Optional description

# Warehouse Configuration
WAREHOUSE_NAME = ""             # Name of the Snowflake warehouse
WAREHOUSE_ACTIVITIES = ["query", "index", "dataflow"]
```

## Usage

Run the script:

```bash
python create_snowflake_account_integration.py
```

The script will:
1. Create a Snowflake Cloud Amplifier account with key pair authentication
2. Create a BYOS integration linked to the account
3. Assign the specified warehouse to the integration (if configured)

## Warehouse Activities

Available warehouse activities:
- `query` - Allows querying data from the warehouse
- `index` - Allows indexing operations
- `dataflow` - Allows dataflow/ETL operations

## License

MIT License
