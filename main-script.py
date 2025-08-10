import requests
import pandas as pd
import time
import logging
import schedule
import os
import json
import sys
from pathlib import Path
import re
from datetime import datetime, timedelta
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pytz
from typing import Dict, Optional

# Config
BASE_API = 'https://api.moveworks.ai/export/v1beta2/records'
ENDPOINTS = [
    'interactions',
    'conversations', 
    'plugin-calls',
    'plugin-resources',
    'users'
]

# Configuration file path
CONFIG_FILE = 'moveworks_config.json'

# Setup logging with file output
def setup_logging():
    """Setup logging to both file and console"""
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    log_filename = log_dir / f"moveworks_pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# Retry config
MAX_RETRIES = 5

class MoveworksDataPipeline:
    def __init__(self, config_file=CONFIG_FILE):
        self.config_file = config_file
        self.config = self.load_config()
        
    def load_config(self):
        """Load configuration from JSON file"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                logger.info(f"Configuration loaded from {self.config_file}")
                return config
            except Exception as e:
                logger.error(f"Failed to load config: {str(e)}")
                return None
        else:
            logger.warning(f"Config file {self.config_file} not found. Run setup first.")
            return None
    
    def save_config(self, config):
        """Save configuration to JSON file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(config, f, indent=2)
            logger.info(f"Configuration saved to {self.config_file}")
        except Exception as e:
            logger.error(f"Failed to save config: {str(e)}")
            raise
    
    def setup_config(self):
        """Interactive setup for initial configuration"""
        print("\n" + "="*60)
        print("MOVEWORKS PIPELINE SETUP")
        print("="*60)
        print("This will create a configuration for both initial load and daily sync.")
        print("-"*60)
        
        # API credentials
        print("\n📡 MOVEWORKS API CREDENTIALS:")
        api_token = input("Enter Moveworks API Access Token: ").strip()
        
        # Snowflake credentials
        print("\n❄️  SNOWFLAKE CREDENTIALS:")
        sf_user = input("Snowflake Username: ").strip()
        sf_password = input("Snowflake Password: ").strip()
        sf_account = input("Snowflake Account Identifier: ").strip()
        sf_warehouse = input("Snowflake Warehouse: ").strip()
        sf_database = input("Snowflake Database: ").strip()
        sf_schema = input("Snowflake Schema: ").strip()
        
        # Optional role
        sf_role = input("Snowflake Role (press Enter for default): ").strip()
        if not sf_role:
            sf_role = None
        
        # Pipeline settings
        print("\n⚙️  DAILY SYNC SETTINGS:")
        print("Daily sync will UPSERT data (update existing records, insert new ones)")
        
        create_views = input("Create analytics views? (y/n): ").strip().lower() in ['y', 'yes']
        
        # Days to look back for daily sync (default 1 day)
        lookback_days = input("Daily sync lookback days (default 1): ").strip()
        try:
            lookback_days = int(lookback_days) if lookback_days else 1
        except ValueError:
            lookback_days = 1
        
        # Schedule time
        print("\n⏰ SCHEDULE SETTINGS:")
        schedule_time = input("Daily run time (HH:MM format, 24hr, default 22:00): ").strip()
        if not schedule_time:
            schedule_time = "22:00"
        
        # Validate time format
        try:
            datetime.strptime(schedule_time, "%H:%M")
        except ValueError:
            print("Invalid time format, using default 22:00")
            schedule_time = "22:00"
        
        config = {
            'api_token': api_token,
            'snowflake': {
                'user': sf_user,
                'password': sf_password,
                'account': sf_account,
                'warehouse': sf_warehouse,
                'database': sf_database,
                'schema': sf_schema,
                'role': sf_role
            },
            'pipeline': {
                'create_views': create_views,
                'daily_lookback_days': lookback_days,
                'schedule_time': schedule_time,
                'timezone': 'US/Pacific',
                'initial_load_completed': False,
                'use_upsert': True  # New flag for upsert behavior
            }
        }
        
        self.save_config(config)
        self.config = config
        
        print(f"\n✅ Configuration saved!")
        print(f"   Daily run time: {schedule_time} PST")
        print(f"   Analytics views: {'YES' if create_views else 'NO'}")
        print(f"   Daily lookback: {lookback_days} days")
        print(f"   Sync mode: UPSERT (update existing + insert new)")
        print("\n📋 NEXT STEPS:")
        print("   1. python3 main-script.py initial-load  # Load historical data")
        print("   2. python3 main-script.py start         # Start daily scheduler")
    
    def get_user_date_inputs_for_initial_load(self):
        """Get start and end dates from user for initial historical load"""
        print("\n" + "="*60)
        print("📅 INITIAL LOAD - DATE RANGE SELECTION")
        print("="*60)
        print("Enter the historical date range for your initial data load:")
        print("Date format: YYYY-MM-DD (e.g., 2024-01-01)")
        print("Recommendation: Start with last 30-90 days, you can always load more later")
        print("-"*60)
        
        while True:
            try:
                start_date_str = input("Enter start date (YYYY-MM-DD): ").strip()
                start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                break
            except ValueError:
                print("❌ Invalid date format. Please use YYYY-MM-DD format")
        
        while True:
            try:
                end_date_str = input("Enter end date (YYYY-MM-DD): ").strip()
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
                
                if end_date < start_date:
                    print("❌ End date cannot be earlier than start date. Please try again.")
                    continue
                    
                # Check if date range is very large
                days_diff = (end_date - start_date).days
                if days_diff > 365:
                    confirm = input(f"⚠️  Large date range ({days_diff} days). This may take a long time. Continue? (y/n): ")
                    if confirm.lower() not in ['y', 'yes']:
                        continue
                
                break
            except ValueError:
                print("❌ Invalid date format. Please use YYYY-MM-DD format")
        
        # Convert to timestamp format for API
        start_timestamp = start_date.strftime("%Y-%m-%dT00:00:00.000Z")
        end_timestamp = (end_date + timedelta(days=1) - timedelta(microseconds=1)).strftime("%Y-%m-%dT23:59:59.999Z")
        
        time_filter = f"last_updated_time ge '{start_timestamp}' and last_updated_time le '{end_timestamp}'"
        
        print(f"\n✅ Initial load date range:")
        print(f"   Start: {start_date_str} 00:00:00 UTC")
        print(f"   End:   {end_date_str} 23:59:59 UTC")
        print(f"   Total days: {days_diff + 1}")
        print("-"*60)
        
        return time_filter, start_date_str, end_date_str
    
    def get_date_range_for_daily_sync(self):
        """Calculate date range for daily automated sync"""
        pst = pytz.timezone('US/Pacific')
        now_pst = datetime.now(pst)
        
        # Look back the configured number of days
        lookback_days = self.config['pipeline']['daily_lookback_days']
        start_date = now_pst - timedelta(days=lookback_days)
        end_date = now_pst - timedelta(days=1)  # Yesterday
        
        # Convert to timestamp format for API
        start_timestamp = start_date.strftime("%Y-%m-%dT00:00:00.000Z")
        end_timestamp = end_date.strftime("%Y-%m-%dT23:59:59.999Z")
        
        time_filter = f"last_updated_time ge '{start_timestamp}' and last_updated_time le '{end_timestamp}'"
        
        logger.info(f"Daily sync date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        return time_filter, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

    def clean_illegal_chars(self, df):
        """Clean illegal characters from DataFrame"""
        def remove_illegal_chars(value):
            if isinstance(value, str):
                return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', value)
            return value
        return df.applymap(remove_illegal_chars)

    def flatten_record(self, record):
        """Flatten nested fields in record"""
        flat = record.copy()
        if isinstance(flat.get("detail"), dict):
            detail = flat.pop("detail", {}) or {}
            for key, value in detail.items():
                if isinstance(value, list):
                    value = ','.join(map(str, value))
                flat[f'detail_{key}'] = value
        
        # Handle external_ids array in users endpoint
        if isinstance(flat.get("external_ids"), list):
            external_ids = flat.pop("external_ids", [])
            for i, ext_id in enumerate(external_ids):
                if isinstance(ext_id, dict):
                    for key, value in ext_id.items():
                        flat[f'external_id_{i}_{key}'] = value
        
        return flat

    def fetch_data(self, endpoint, time_filter):
        """Fetch data from a specific endpoint"""
        url = f'{BASE_API}/{endpoint}'
        params = {
            '$orderby': 'id desc',
            '$filter': time_filter,
        }
        
        headers = {
            'Authorization': f'Bearer {self.config["api_token"]}',
        }

        data = []
        retries = 0
        page_count = 0

        logger.info(f"Fetching data from /{endpoint}")

        while url:
            try:
                logger.info(f"Requesting page {page_count + 1}: {url}")
                response = requests.get(url, headers=headers, params=params if url.endswith(endpoint) else None)

                if response.status_code == 200:
                    json_resp = response.json()
                    page_data = json_resp.get('value', [])
                    logger.info(f"Retrieved {len(page_data)} records from page {page_count + 1}")

                    if not page_data:
                        break

                    for record in page_data:
                        data.append(self.flatten_record(record))

                    url = json_resp.get('@odata.nextLink')
                    retries = 0
                    page_count += 1

                    if endpoint == 'users':
                        time.sleep(2)

                elif response.status_code == 429:
                    wait = 90 if endpoint == 'users' else 60
                    logger.warning(f"Rate limited on /{endpoint}. Waiting for {wait} seconds.")
                    time.sleep(wait)

                elif response.status_code in (500, 502, 503, 504):
                    if retries < MAX_RETRIES:
                        wait = 2 ** retries
                        logger.warning(f"Transient error {response.status_code} on /{endpoint}. Retrying in {wait} seconds...")
                        time.sleep(wait)
                        retries += 1
                    else:
                        logger.error(f"Max retries reached on /{endpoint}. Aborting.")
                        break

                else:
                    logger.error(f"Unexpected error {response.status_code} on /{endpoint}: {response.text}")
                    break

            except Exception as e:
                logger.exception(f"Exception occurred while calling /{endpoint}")
                break

        if data:
            df = pd.DataFrame(data)
            df = self.clean_illegal_chars(df)
            logger.info(f"Ingestion complete for /{endpoint}, {len(df)} records.")
            return df
        else:
            logger.info(f"No data retrieved from /{endpoint}.")
            return pd.DataFrame()

    def get_table_schema(self, table_name):
        """Get predefined table schema for each endpoint"""
        schemas = {
            'MOVEWORKS_CONVERSATIONS': """
                CREATE OR REPLACE TABLE {table_name} (
                    LAST_UPDATED_AT VARCHAR(16777216) COMMENT 'Last updated timestamp of the record',
                    ID VARCHAR(16777216) NOT NULL COMMENT 'Id identifying the unique conversation',
                    USER_ID VARCHAR(16777216) COMMENT 'Unique id of the user',
                    CREATED_AT VARCHAR(16777216) COMMENT 'The timestamp of the first interaction in the conversation',
                    ROUTE VARCHAR(16777216) COMMENT 'Route/origin of the conversation - DM, Ticket, Channel, Notification',
                    PRIMARY_DOMAIN VARCHAR(16777216) COMMENT 'The main detected domain in the conversation',
                    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Data load timestamp',
                    PRIMARY KEY (ID)
                )
            """,
            
            'MOVEWORKS_INTERACTIONS': """
                CREATE OR REPLACE TABLE {table_name} (
                    LAST_UPDATED_AT VARCHAR(16777216) COMMENT 'Last updated timestamp of the record',
                    CONVERSATION_ID VARCHAR(16777216) COMMENT 'Id identifying the unique conversation',
                    USER_ID VARCHAR(16777216) COMMENT 'Unique id of the user',
                    ID VARCHAR(16777216) NOT NULL COMMENT 'Id identifying the unique interaction',
                    CREATED_AT VARCHAR(16777216) COMMENT 'Timestamp of the interaction',
                    PLATFORM VARCHAR(16777216) COMMENT 'Platform of the interaction - Slack/msteams etc.',
                    TYPE VARCHAR(16777216) COMMENT 'Type of the interaction - INTERACTION_TYPE_UTTERANCE, INTERACTION_TYPE_BOT_MESSAGE etc.',
                    LABEL VARCHAR(16777216) COMMENT 'LABEL for the interaction type UI form submission',
                    PARENT_INTERACTION_ID VARCHAR(16777216) COMMENT 'Parent interaction id for UI Action link click and button click interactions',
                    DETAILS VARIANT COMMENT 'Details of the interaction (Json col)- content, type, domain, detail, entity etc.',
                    ACTOR VARCHAR(16777216) COMMENT 'Actor - User/Bot',
                    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Data load timestamp',
                    PRIMARY KEY (ID)
                )
            """,
            
            'MOVEWORKS_PLUGIN_CALLS': """
                CREATE OR REPLACE TABLE {table_name} (
                    LAST_UPDATED_AT VARCHAR(16777216) COMMENT 'Last updated timestamp of the record',
                    CONVERSATION_ID VARCHAR(16777216) COMMENT 'Id identifying the unique conversation related to the plugin call',
                    USER_ID VARCHAR(16777216) COMMENT 'Unique id of the user',
                    INTERACTION_ID VARCHAR(16777216) COMMENT 'Id identifying the unique interaction related to the plugin call',
                    ID VARCHAR(16777216) NOT NULL COMMENT 'Unique Id identifying the plugin call',
                    CREATED_AT VARCHAR(16777216) COMMENT 'Timestamp of the plugin call',
                    PLUGIN_UPDATE_TIME VARCHAR(16777216) COMMENT 'Timestamp of the latest plugin call update',
                    PLUGIN_NAME VARCHAR(16777216) COMMENT 'Name of the executed Plugin - Product display name for the Native plugin and Config name for the Custom plugin',
                    PLUGIN_STATUS VARCHAR(16777216) COMMENT 'Semantic Plugin status for each of the executed Plugin call',
                    SERVED BOOLEAN COMMENT 'Whether or not plugin is served to the user - Plugin served or plugin waiting for user input',
                    USED BOOLEAN COMMENT 'Whether or not plugin is successfully executed',
                    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Data load timestamp',
                    PRIMARY KEY (ID)
                )
            """,
            
            'MOVEWORKS_PLUGIN_RESOURCES': """
                CREATE OR REPLACE TABLE {table_name} (
                    ID VARCHAR(16777216) NOT NULL COMMENT 'Plugin resource item id as the hash of interaction_id, resource_id and resource_type',
                    CONVERSATION_ID VARCHAR(16777216) COMMENT 'conversation_id identifying unique conversation (series_id)',
                    INTERACTION_ID VARCHAR(16777216) COMMENT 'interaction_id identifying interaction corresponding to the resource being used',
                    PLUGIN_CALL_ID VARCHAR(16777216) COMMENT 'Id for the plugin call',
                    USER_ID VARCHAR(16777216) COMMENT 'Id for the user record',
                    TYPE VARCHAR(16777216) COMMENT 'Type of Resource - FILE, FORM, KNOWLEDGE, Ticket etc.',
                    RESOURCE_ID VARCHAR(16777216) COMMENT 'Resource ID - KB ID, TICKET_ID, etc.',
                    DETAILS VARIANT COMMENT 'Details of resources searched in the plugin - e.g., json of details',
                    CITED BOOLEAN COMMENT 'If the resource is cited in the AI Assistant response',
                    LAST_UPDATED_AT VARCHAR(16777216) COMMENT 'Last updated timestamp of the record',
                    CREATED_AT VARCHAR(16777216) COMMENT 'timestamp of plugin call',
                    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Data load timestamp',
                    PRIMARY KEY (ID)
                )
            """,
            
            'MOVEWORKS_USERS': """
                CREATE OR REPLACE TABLE {table_name} (
                    ID VARCHAR(16777216) NOT NULL COMMENT 'Unique id of the user',
                    FIRST_NAME VARCHAR(16777216) COMMENT 'First name of the user',
                    LAST_NAME VARCHAR(16777216) COMMENT 'Last name of the user',
                    EMAIL_ADDR VARCHAR(16777216) COMMENT 'Email address of the user',
                    USER_PREFERRED_LANGUAGE VARCHAR(16777216) COMMENT 'User preferred language configured in AI Assistant',
                    HAS_ACCESS_TO_BOT BOOLEAN COMMENT 'Whether or not the user has access to AI Assistant',
                    EXTERNAL_IDS VARCHAR(16777216) COMMENT 'All external system identities from which user data is ingested',
                    LAST_UPDATED_AT VARCHAR(16777216) COMMENT 'Timestamp at which user was last updated in Moveworks',
                    FIRST_INTERACTION_AT VARCHAR(16777216) COMMENT 'Timestamp at which the users first interaction was recorded',
                    LAST_INTERACTION_AT VARCHAR(16777216) COMMENT 'Timestamp at which the users latest interaction was recorded',
                    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Data load timestamp',
                    PRIMARY KEY (ID)
                )
            """
        }
        
        return schemas.get(table_name)

    def get_primary_key_column(self, endpoint):
        """Get the primary key column name for each endpoint"""
        # All endpoints use 'ID' as the primary key
        return 'ID'

    def connect_to_snowflake(self):
        """Create connection to Snowflake"""
        try:
            sf_config = self.config['snowflake']
            conn_params = {
                'user': sf_config['user'],
                'password': sf_config['password'],
                'account': sf_config['account'],
                'warehouse': sf_config['warehouse'],
                'database': sf_config['database'],
                'schema': sf_config['schema']
            }
            
            if sf_config['role']:
                conn_params['role'] = sf_config['role']
            
            conn = snowflake.connector.connect(**conn_params)
            logger.info("Successfully connected to Snowflake")
            return conn
        
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise

    def create_table_if_not_exists(self, conn, table_name, df=None):
        """Create table in Snowflake using predefined schema"""
        cursor = conn.cursor()
        
        try:
            # Get predefined schema
            schema_sql = self.get_table_schema(table_name)
            
            if not schema_sql:
                raise ValueError(f"No predefined schema found for table: {table_name}")
            
            # Format with actual table name
            create_table_sql = schema_sql.format(table_name=table_name)
            
            cursor.execute(create_table_sql)
            logger.info(f"Created/verified table: {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {str(e)}")
            raise
        finally:
            cursor.close()

    def create_temp_table(self, conn, temp_table_name, df, table_name):
        """Create a temporary table for staging data during upsert"""
        cursor = conn.cursor()
        
        try:
            # Get the main table schema
            schema_sql = self.get_table_schema(table_name)
            if not schema_sql:
                raise ValueError(f"No predefined schema found for table: {table_name}")
            
            # Create temp table with same structure but without PRIMARY KEY constraint
            temp_schema_sql = schema_sql.replace('PRIMARY KEY (ID)', '').replace(f'{table_name}', f'{temp_table_name}')
            temp_schema_sql = temp_schema_sql.replace('CREATE OR REPLACE TABLE', 'CREATE OR REPLACE TEMPORARY TABLE')
            
            cursor.execute(temp_schema_sql)
            logger.info(f"Created temporary table: {temp_table_name}")
            
        except Exception as e:
            logger.error(f"Failed to create temp table {temp_table_name}: {str(e)}")
            raise
        finally:
            cursor.close()

    def perform_upsert(self, conn, table_name, temp_table_name, endpoint):
        """Perform MERGE (upsert) operation from temp table to main table"""
        cursor = conn.cursor()
        
        try:
            primary_key = self.get_primary_key_column(endpoint)
            
            # Get all columns from the temp table except LOAD_TIMESTAMP (we'll use CURRENT_TIMESTAMP)
            cursor.execute(f"DESCRIBE TABLE {temp_table_name}")
            columns_result = cursor.fetchall()
            all_columns = [row[0] for row in columns_result if row[0] not in ['LOAD_TIMESTAMP']]
            
            # Build the MERGE statement
            merge_sql = f"""
                MERGE INTO {table_name} AS target
                USING {temp_table_name} AS source
                ON target.{primary_key} = source.{primary_key}
                WHEN MATCHED THEN
                    UPDATE SET
                        {', '.join([f'{col} = source.{col}' for col in all_columns if col != primary_key])},
                        LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join(all_columns)}, LOAD_TIMESTAMP)
                    VALUES ({', '.join([f'source.{col}' for col in all_columns])}, CURRENT_TIMESTAMP())
            """
            
            logger.info(f"Executing MERGE operation for {table_name}")
            result = cursor.execute(merge_sql)
            
            # Get merge statistics
            merge_result = cursor.fetchone()
            if merge_result:
                rows_inserted = merge_result[0] if len(merge_result) > 0 else 0
                rows_updated = merge_result[1] if len(merge_result) > 1 else 0
                total_rows = rows_inserted + rows_updated
                logger.info(f"MERGE completed - Inserted: {rows_inserted}, Updated: {rows_updated}, Total: {total_rows}")
                return True, total_rows, rows_inserted, rows_updated
            else:
                logger.info("MERGE completed successfully")
                return True, 0, 0, 0
            
        except Exception as e:
            logger.error(f"Failed to perform upsert on {table_name}: {str(e)}")
            raise
        finally:
            cursor.close()

    def transform_dataframe_for_schema(self, df, endpoint):
        """Transform DataFrame to match the expected schema"""
        df_transformed = df.copy()
        
        # Handle the LOAD_TIMESTAMP properly - convert to Snowflake-compatible timestamp
        current_timestamp = datetime.now()
        df_transformed['LOAD_TIMESTAMP'] = current_timestamp
        
        if endpoint == 'conversations':
            column_mapping = {
                'last_updated_time': 'LAST_UPDATED_AT',
                'id': 'ID',
                'user_id': 'USER_ID', 
                'created_time': 'CREATED_AT',
                'route': 'ROUTE',
                'primary_domain': 'PRIMARY_DOMAIN'
            }
            
        elif endpoint == 'interactions':
            if 'detail' in df.columns:
                detail_cols = [col for col in df.columns if col.startswith('detail_')]
                df_transformed['DETAILS'] = df[detail_cols].apply(
                    lambda row: {col.replace('detail_', ''): row[col] for col in detail_cols if pd.notna(row[col])}, 
                    axis=1
                ).apply(lambda x: json.dumps(x) if x else None)
            else:
                df_transformed['DETAILS'] = None
                
            column_mapping = {
                'last_updated_time': 'LAST_UPDATED_AT',
                'conversation_id': 'CONVERSATION_ID',
                'user_id': 'USER_ID',
                'id': 'ID',
                'created_time': 'CREATED_AT',
                'platform': 'PLATFORM',
                'type': 'TYPE',
                'label': 'LABEL',
                'parent_interaction_id': 'PARENT_INTERACTION_ID',
                'DETAILS': 'DETAILS',
                'actor': 'ACTOR'
            }
            
        elif endpoint == 'plugin-calls':
            column_mapping = {
                'last_updated_time': 'LAST_UPDATED_AT',
                'conversation_id': 'CONVERSATION_ID',
                'user_id': 'USER_ID',
                'interaction_id': 'INTERACTION_ID',
                'id': 'ID',
                'created_time': 'CREATED_AT',
                'plugin_update_time': 'PLUGIN_UPDATE_TIME',
                'plugin_name': 'PLUGIN_NAME',
                'plugin_status': 'PLUGIN_STATUS',
                'served': 'SERVED',
                'used': 'USED'
            }
            
        elif endpoint == 'plugin-resources':
            # Fix the detail handling to create proper JSON for VARIANT column
            detail_cols = [col for col in df.columns if col.startswith('detail_')]
            if detail_cols:
                df_transformed['DETAILS'] = df[detail_cols].apply(
                    lambda row: {col.replace('detail_', ''): row[col] for col in detail_cols if pd.notna(row[col])}, 
                    axis=1
                ).apply(lambda x: json.dumps(x) if x else None)
            else:
                df_transformed['DETAILS'] = None
                
            column_mapping = {
                'id': 'ID',
                'conversation_id': 'CONVERSATION_ID',
                'interaction_id': 'INTERACTION_ID',
                'plugin_call_id': 'PLUGIN_CALL_ID',
                'user_id': 'USER_ID',
                'type': 'TYPE',
                'resource_id': 'RESOURCE_ID',
                'DETAILS': 'DETAILS',
                'cited': 'CITED',
                'last_updated_time': 'LAST_UPDATED_AT',
                'created_time': 'CREATED_AT'
            }
            
        elif endpoint == 'users':
            external_id_cols = [col for col in df.columns if col.startswith('external_id_')]
            if external_id_cols:
                df_transformed['EXTERNAL_IDS'] = df[external_id_cols].apply(
                    lambda row: [val for val in row.values if pd.notna(val)], axis=1
                ).apply(lambda x: str(x) if x else None)
            
            column_mapping = {
                'id': 'ID',
                'first_name': 'FIRST_NAME',
                'last_name': 'LAST_NAME',
                'email_addr': 'EMAIL_ADDR',
                'user_preferred_language': 'USER_PREFERRED_LANGUAGE',
                'access_to_bot': 'HAS_ACCESS_TO_BOT',
                'EXTERNAL_IDS': 'EXTERNAL_IDS',
                'last_updated_time': 'LAST_UPDATED_AT',
                'first_interaction_time': 'FIRST_INTERACTION_AT',
                'latest_interaction_time': 'LAST_INTERACTION_AT'
            }
        
        else:
            return df_transformed
        
        # Create new DataFrame with only the mapped columns
        result_df = pd.DataFrame()
        for api_col, schema_col in column_mapping.items():
            if api_col in df_transformed.columns:
                result_df[schema_col] = df_transformed[api_col]
            else:
                result_df[schema_col] = None
        
        # Add load timestamp - ensure it's a proper timestamp
        result_df['LOAD_TIMESTAMP'] = pd.to_datetime(current_timestamp)
        
        return result_df

    def load_data_to_snowflake(self, conn, table_name, df, endpoint, is_initial_load=False):
        """Load DataFrame to Snowflake table using REPLACE for initial load or UPSERT for daily sync"""
        try:
            if df.empty:
                logger.warning(f"No data to load for {table_name}")
                return False, 0
            
            # Transform DataFrame to match schema
            df_transformed = self.transform_dataframe_for_schema(df, endpoint)
            
            # Create main table if it doesn't exist
            self.create_table_if_not_exists(conn, table_name, df_transformed)
            
            if is_initial_load:
                # For initial load, use direct replace (overwrite)
                # Remove LOAD_TIMESTAMP from DataFrame since it's auto-generated
                df_for_load = df_transformed.drop(columns=['LOAD_TIMESTAMP'])
                
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=df_for_load,
                    table_name=table_name.upper(),
                    auto_create_table=False,
                    overwrite=True,
                    quote_identifiers=False
                )
                
                if success:
                    logger.info(f"Successfully loaded {nrows} rows to {table_name.upper()} (REPLACE) in {nchunks} chunks")
                else:
                    logger.error(f"Failed to load data to {table_name.upper()}")
                    
                return success, nrows
                
            else:
                # For daily sync, use upsert via temp table and MERGE
                use_upsert = self.config['pipeline'].get('use_upsert', True)
                
                if use_upsert:
                    return self.upsert_data_to_snowflake(conn, table_name, df_transformed, endpoint)
                else:
                    # Fallback to simple append if upsert is disabled
                    df_for_load = df_transformed.drop(columns=['LOAD_TIMESTAMP'])
                    
                    success, nchunks, nrows, _ = write_pandas(
                        conn=conn,
                        df=df_for_load,
                        table_name=table_name.upper(),
                        auto_create_table=False,
                        overwrite=False,
                        quote_identifiers=False
                    )
                    
                    if success:
                        logger.info(f"Successfully appended {nrows} rows to {table_name.upper()} in {nchunks} chunks")
                    else:
                        logger.error(f"Failed to append data to {table_name.upper()}")
                        
                    return success, nrows
                    
        except Exception as e:
            logger.error(f"Error loading data to {table_name}: {str(e)}")
            raise

    def upsert_data_to_snowflake(self, conn, table_name, df_transformed, endpoint):
        """Perform upsert operation using temporary table and MERGE"""
        temp_table_name = f"TEMP_{table_name}_{int(time.time())}"
        
        try:
            # Create temporary table
            self.create_temp_table(conn, temp_table_name, df_transformed, table_name)
            
            # Remove LOAD_TIMESTAMP from DataFrame since it's auto-generated
            df_for_load = df_transformed.drop(columns=['LOAD_TIMESTAMP'])
            
            # Load data to temp table
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df_for_load,
                table_name=temp_table_name,
                auto_create_table=False,
                overwrite=True,  # Always overwrite temp table
                quote_identifiers=False
            )
            
            if not success:
                logger.error(f"Failed to load data to temp table {temp_table_name}")
                return False, 0
            
            logger.info(f"Loaded {nrows} rows to temporary table {temp_table_name}")
            
            # Perform MERGE operation
            merge_success, total_rows, rows_inserted, rows_updated = self.perform_upsert(
                conn, table_name, temp_table_name, endpoint
            )
            
            if merge_success:
                logger.info(f"UPSERT completed for {table_name}: {rows_inserted} inserted, {rows_updated} updated")
                return True, total_rows
            else:
                return False, 0
                
        except Exception as e:
            logger.error(f"Error during upsert operation for {table_name}: {str(e)}")
            return False, 0
        finally:
            # Clean up temp table
            try:
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                logger.info(f"Dropped temporary table {temp_table_name}")
                cursor.close()
            except Exception as e:
                logger.warning(f"Failed to drop temp table {temp_table_name}: {str(e)}")

    def get_analytics_views(self, database, schema):
        """Define all analytics views to be created"""
        views = {
            'V_TOTAL_CONVERSATIONS': f"""
                CREATE OR REPLACE VIEW {database}.{schema}.V_TOTAL_CONVERSATIONS AS
                SELECT 
                    COUNT(DISTINCT ID) AS total_conversations,
                    DATE(CREATED_AT) AS conversation_date,
                    ROUTE,
                    PRIMARY_DOMAIN
                FROM {database}.{schema}.MOVEWORKS_CONVERSATIONS
                WHERE CREATED_AT IS NOT NULL
                GROUP BY DATE(CREATED_AT), ROUTE, PRIMARY_DOMAIN
                ORDER BY conversation_date DESC
            """,
            
            'V_TOTAL_INTERACTIONS': f"""
                CREATE OR REPLACE VIEW {database}.{schema}.V_TOTAL_INTERACTIONS AS
                SELECT 
                    COUNT(ID) AS total_interactions,
                    COUNT(DISTINCT CONVERSATION_ID) AS unique_conversations,
                    DATE(CREATED_AT) AS interaction_date,
                    PLATFORM,
                    TYPE,
                    ACTOR
                FROM {database}.{schema}.MOVEWORKS_INTERACTIONS
                WHERE CREATED_AT IS NOT NULL
                GROUP BY DATE(CREATED_AT), PLATFORM, TYPE, ACTOR
                ORDER BY interaction_date DESC
            """,
            
            'V_ACTIVE_USERS': f"""
                CREATE OR REPLACE VIEW {database}.{schema}.V_ACTIVE_USERS AS
                SELECT 
                    COUNT(DISTINCT USER_ID) AS active_users,
                    DATE(CREATED_AT) AS activity_date,
                    PLATFORM
                FROM {database}.{schema}.MOVEWORKS_INTERACTIONS
                WHERE CREATED_AT IS NOT NULL
                  AND USER_ID IS NOT NULL
                GROUP BY DATE(CREATED_AT), PLATFORM
                ORDER BY activity_date DESC
            """,
            
            'V_KNOWLEDGE_PERFORMANCE': f"""
                CREATE OR REPLACE VIEW {database}.{schema}.V_KNOWLEDGE_PERFORMANCE AS
                SELECT
                    PARSE_JSON(PR.DETAILS):name::STRING AS knowledge_name,
                    COUNT(DISTINCT PR.INTERACTION_ID) AS times_cited,
                    COUNT(DISTINCT CASE
                        WHEN IT.LABEL = 'feedback'
                          AND PARSE_JSON(IT.DETAILS):detail::STRING = 'HELPFUL'
                        THEN IT.CONVERSATION_ID
                    END) AS helpful_feedback,
                    COUNT(DISTINCT CASE
                        WHEN IT.LABEL = 'feedback'
                          AND PARSE_JSON(IT.DETAILS):detail::STRING = 'UNHELPFUL'
                        THEN IT.CONVERSATION_ID
                    END) AS unhelpful_feedback,
                    COUNT(DISTINCT CASE
                        WHEN (IT.LABEL = 'file_ticket' OR IT.LABEL = 'handoff')
                          AND PARSE_JSON(IT.DETAILS):detail::STRING = 'File IT Ticket'
                        THEN IT.CONVERSATION_ID
                    END) AS tickets_filed,
                    DATE(PR.CREATED_AT) AS report_date
                FROM {database}.{schema}.MOVEWORKS_PLUGIN_RESOURCES PR
                LEFT JOIN {database}.{schema}.MOVEWORKS_INTERACTIONS IT
                    ON PR.CONVERSATION_ID = IT.CONVERSATION_ID
                WHERE PR.TYPE = 'RESOURCE_TYPE_KNOWLEDGE'
                  AND PR.CITED = TRUE
                GROUP BY knowledge_name, DATE(PR.CREATED_AT)
                ORDER BY times_cited DESC
            """,
            
            'V_DAILY_SUMMARY': f"""
                CREATE OR REPLACE VIEW {database}.{schema}.V_DAILY_SUMMARY AS
                SELECT 
                    conv.activity_date,
                    conv.total_conversations,
                    inter.total_interactions,
                    inter.active_users,
                    kb_success.knowledge_success,
                    ROUND((kb_success.knowledge_success / NULLIF(conv.total_conversations, 0)) * 100, 2) AS knowledge_success_rate
                FROM (
                    SELECT DATE(CREATED_AT) AS activity_date,
                           COUNT(DISTINCT ID) AS total_conversations
                    FROM {database}.{schema}.MOVEWORKS_CONVERSATIONS
                    WHERE CREATED_AT IS NOT NULL
                    GROUP BY DATE(CREATED_AT)
                ) conv
                LEFT JOIN (
                    SELECT DATE(CREATED_AT) AS activity_date,
                           COUNT(ID) AS total_interactions,
                           COUNT(DISTINCT USER_ID) AS active_users
                    FROM {database}.{schema}.MOVEWORKS_INTERACTIONS
                    WHERE CREATED_AT IS NOT NULL
                    GROUP BY DATE(CREATED_AT)
                ) inter ON conv.activity_date = inter.activity_date
                LEFT JOIN (
                    SELECT DATE(CT.CREATED_AT) AS activity_date,
                           COUNT(DISTINCT CT.ID) AS knowledge_success
                    FROM {database}.{schema}.MOVEWORKS_CONVERSATIONS CT
                    JOIN {database}.{schema}.MOVEWORKS_PLUGIN_CALLS PC
                        ON CT.ID = PC.CONVERSATION_ID
                    WHERE PC.PLUGIN_NAME = 'Knowledge Base' 
                        AND PC.USED = TRUE
                    GROUP BY DATE(CT.CREATED_AT)
                ) kb_success ON conv.activity_date = kb_success.activity_date
                ORDER BY conv.activity_date DESC
            """
        }
        
        return views

    def create_analytics_views(self, conn):
        """Create all analytics views after data load"""
        cursor = conn.cursor()
        sf_config = self.config['snowflake']
        database = sf_config['database']
        schema = sf_config['schema']
        
        try:
            logger.info("Starting analytics views creation...")
            
            views = self.get_analytics_views(database, schema)
            created_views = []
            failed_views = []
            
            for view_name, view_sql in views.items():
                try:
                    cursor.execute(view_sql)
                    created_views.append(view_name)
                    logger.info(f"Created view: {view_name}")
                except Exception as e:
                    failed_views.append((view_name, str(e)))
                    logger.error(f"Failed to create view {view_name}: {str(e)}")
            
            return created_views, failed_views
            
        except Exception as e:
            logger.error(f"Error creating analytics views: {str(e)}")
            raise
        finally:
            cursor.close()

    def run_initial_load(self):
        """Run initial historical data load"""
        try:
            if not self.config:
                logger.error("No configuration found. Run setup first.")
                return False
            
            # Check if initial load was already completed
            if self.config['pipeline'].get('initial_load_completed', False):
                print("\n⚠️  Initial load was already completed.")
                confirm = input("Do you want to run it again? This will REPLACE all existing data (y/n): ")
                if confirm.lower() not in ['y', 'yes']:
                    print("Initial load cancelled.")
                    return False
            
            pipeline_start = datetime.now()
            logger.info("=" * 60)
            logger.info("STARTING INITIAL HISTORICAL DATA LOAD")
            logger.info("=" * 60)
            
            # Get date range for initial load from user
            time_filter, start_date_str, end_date_str = self.get_user_date_inputs_for_initial_load()
            create_views = self.config['pipeline']['create_views']
            
            logger.info(f"Load type: INITIAL (REPLACE)")
            logger.info(f"Date range: {start_date_str} to {end_date_str}")
            logger.info(f"Analytics views: {'YES' if create_views else 'NO'}")
            
            # Connect to Snowflake
            conn = self.connect_to_snowflake()
            
            # Process all endpoints
            load_summary = {}
            total_rows = 0
            successful_loads = 0
            
            print(f"\n🚀 Starting initial load...")
            
            for endpoint in ENDPOINTS:
                try:
                    print(f"📡 Processing {endpoint}...")
                    logger.info(f"Processing endpoint: {endpoint}")
                    df = self.fetch_data(endpoint, time_filter)
                    
                    if not df.empty:
                        # Load to Snowflake (initial load = replace)
                        table_name = f"MOVEWORKS_{endpoint.replace('-', '_').upper()}"
                        success, nrows = self.load_data_to_snowflake(conn, table_name, df, endpoint, is_initial_load=True)
                        
                        load_summary[endpoint] = {
                            'table_name': table_name,
                            'rows_loaded': nrows if success else 0,
                            'success': success
                        }
                        
                        if success:
                            total_rows += nrows
                            successful_loads += 1
                    else:
                        load_summary[endpoint] = {
                            'table_name': f"MOVEWORKS_{endpoint.replace('-', '_').upper()}",
                            'rows_loaded': 0,
                            'success': True  # No data is not a failure
                        }
                        successful_loads += 1
                        
                except Exception as e:
                    logger.error(f"Failed processing endpoint {endpoint}: {str(e)}")
                    load_summary[endpoint] = {
                        'table_name': f"MOVEWORKS_{endpoint.replace('-', '_').upper()}",
                        'rows_loaded': 0,
                        'success': False
                    }
            
            # Create analytics views if configured
            views_created = []
            views_failed = []
            if create_views:
                try:
                    print(f"\n📊 Creating analytics views...")
                    views_created, views_failed = self.create_analytics_views(conn)
                except Exception as e:
                    logger.error(f"Analytics views creation failed: {str(e)}")
            
            # Close Snowflake connection
            conn.close()
            
            # Mark initial load as completed if successful
            if successful_loads == len(ENDPOINTS):
                self.config['pipeline']['initial_load_completed'] = True
                self.config['pipeline']['initial_load_date'] = datetime.now().isoformat()
                self.save_config(self.config)
            
            # Log final summary
            pipeline_duration = datetime.now() - pipeline_start
            
            print("\n" + "=" * 80)
            print("🎉 INITIAL LOAD COMPLETED!")
            print("=" * 80)
            print(f"Duration: {pipeline_duration}")
            print(f"Date range: {start_date_str} to {end_date_str}")
            print(f"Total endpoints processed: {len(ENDPOINTS)}")
            print(f"Successful loads: {successful_loads}/{len(ENDPOINTS)}")
            print(f"Total rows loaded: {total_rows:,}")
            
            if create_views:
                print(f"Analytics views created: {len(views_created)}")
            
            print("-" * 80)
            print("TABLES LOADED:")
            
            for endpoint, summary in load_summary.items():
                status = "✅ SUCCESS" if summary['success'] else "❌ FAILED"
                rows = summary['rows_loaded']
                print(f"  {summary['table_name']:<35} {status:<12} {rows:>10,} rows")
            
            if views_created:
                print(f"\nVIEWS CREATED:")
                for view in views_created:
                    print(f"  {view:<45} ✅ SUCCESS")
            
            print("=" * 80)
            
            if successful_loads == len(ENDPOINTS):
                print("🎉 Initial load completed successfully!")
                print("\n📋 NEXT STEPS:")
                print("   • Your historical data is now loaded")
                print("   • Analytics views are ready for BI tools")
                print("   • Run 'python3 main-script.py start' to begin daily sync")
                print(f"   • Daily sync will run at {self.config['pipeline']['schedule_time']} PST")
                print("   • Daily sync uses UPSERT to handle duplicate IDs properly")
            else:
                print("⚠️  Initial load completed with some errors. Check logs for details.")
            
            return successful_loads == len(ENDPOINTS)
            
        except Exception as e:
            logger.exception("Fatal error in initial load")
            return False

    def run_daily_sync(self):
        """Run the daily data sync (upsert mode)"""
        try:
            pipeline_start = datetime.now()
            logger.info("=" * 60)
            logger.info("STARTING DAILY DATA SYNC")
            logger.info("=" * 60)
            
            if not self.config:
                logger.error("No configuration found. Run setup first.")
                return False
            
            # Check if initial load was completed
            if not self.config['pipeline'].get('initial_load_completed', False):
                logger.warning("Initial load not completed. Run 'python moveworks_pipeline.py initial-load' first.")
                return False
            
            # Get date range for daily sync
            time_filter, start_date_str, end_date_str = self.get_date_range_for_daily_sync()
            create_views = self.config['pipeline']['create_views']
            use_upsert = self.config['pipeline'].get('use_upsert', True)
            
            sync_mode = "UPSERT" if use_upsert else "APPEND"
            logger.info(f"Load type: DAILY SYNC ({sync_mode})")
            logger.info(f"Date range: {start_date_str} to {end_date_str}")
            logger.info(f"Analytics views: {'YES' if create_views else 'NO'}")
            
            # Connect to Snowflake
            conn = self.connect_to_snowflake()
            
            # Process all endpoints
            load_summary = {}
            total_rows = 0
            successful_loads = 0
            
            for endpoint in ENDPOINTS:
                try:
                    logger.info(f"Processing endpoint: {endpoint}")
                    df = self.fetch_data(endpoint, time_filter)
                    
                    if not df.empty:
                        # Load to Snowflake (daily sync = upsert or append based on config)
                        table_name = f"MOVEWORKS_{endpoint.replace('-', '_').upper()}"
                        success, nrows = self.load_data_to_snowflake(conn, table_name, df, endpoint, is_initial_load=False)
                        
                        load_summary[endpoint] = {
                            'table_name': table_name,
                            'rows_loaded': nrows if success else 0,
                            'success': success,
                            'sync_mode': sync_mode
                        }
                        
                        if success:
                            total_rows += nrows
                            successful_loads += 1
                    else:
                        load_summary[endpoint] = {
                            'table_name': f"MOVEWORKS_{endpoint.replace('-', '_').upper()}",
                            'rows_loaded': 0,
                            'success': True,  # No data is not a failure
                            'sync_mode': sync_mode
                        }
                        successful_loads += 1
                        
                except Exception as e:
                    logger.error(f"Failed processing endpoint {endpoint}: {str(e)}")
                    load_summary[endpoint] = {
                        'table_name': f"MOVEWORKS_{endpoint.replace('-', '_').upper()}",
                        'rows_loaded': 0,
                        'success': False,
                        'sync_mode': sync_mode
                    }
            
            # Create analytics views if configured
            views_created = []
            views_failed = []
            if create_views:
                try:
                    views_created, views_failed = self.create_analytics_views(conn)
                except Exception as e:
                    logger.error(f"Analytics views creation failed: {str(e)}")
            
            # Close Snowflake connection
            conn.close()
            
            # Log final summary
            pipeline_duration = datetime.now() - pipeline_start
            
            logger.info("=" * 60)
            logger.info("DAILY SYNC COMPLETED")
            logger.info("=" * 60)
            logger.info(f"Duration: {pipeline_duration}")
            logger.info(f"Sync mode: {sync_mode}")
            logger.info(f"Total endpoints processed: {len(ENDPOINTS)}")
            logger.info(f"Successful loads: {successful_loads}/{len(ENDPOINTS)}")
            logger.info(f"Total rows processed: {total_rows:,}")
            
            if create_views:
                logger.info(f"Analytics views updated: {len(views_created)}")
            
            for endpoint, summary in load_summary.items():
                status = "SUCCESS" if summary['success'] else "FAILED"
                rows = summary['rows_loaded']
                mode = summary['sync_mode']
                logger.info(f"  {summary['table_name']:<35} {status:<10} {rows:>10,} rows ({mode})")
            
            return successful_loads == len(ENDPOINTS)
            
        except Exception as e:
            logger.exception("Fatal error in daily sync")
            return False

    def start_scheduler(self):
        """Start the scheduled daily sync"""
        if not self.config:
            logger.error("No configuration found. Run 'python moveworks_pipeline.py setup' first.")
            return
        
        # Check if initial load was completed
        if not self.config['pipeline'].get('initial_load_completed', False):
            logger.error("Initial load not completed. Run 'python moveworks_pipeline.py initial-load' first.")
            return
        
        schedule_time = self.config['pipeline']['schedule_time']
        timezone = self.config['pipeline']['timezone']
        use_upsert = self.config['pipeline'].get('use_upsert', True)
        sync_mode = "UPSERT" if use_upsert else "APPEND"
        
        # Schedule the daily job
        schedule.every().day.at(schedule_time).do(self.run_daily_sync)
        
        logger.info(f"Scheduler started! Daily sync will run at {schedule_time} {timezone}")
        logger.info(f"Sync mode: {sync_mode} (handles duplicate IDs properly)")
        logger.info("Press Ctrl+C to stop the scheduler")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")

    def run_once(self):
        """Run the daily sync once manually"""
        if not self.config['pipeline'].get('initial_load_completed', False):
            print("❌ Initial load not completed. Run 'python moveworks_pipeline.py initial-load' first.")
            return False
        
        success = self.run_daily_sync()
        if success:
            logger.info("Daily sync completed successfully!")
        else:
            logger.error("Daily sync completed with errors. Check logs for details.")
        return success

def main():
    """Main function with command line interface"""
    pipeline = MoveworksDataPipeline()
    
    if len(sys.argv) < 2:
        print("\nMoveworks Dual-Mode Data Pipeline with UPSERT Support")
        print("=" * 60)
        print("SETUP:")
        print("  python moveworks_pipeline.py setup         - Initial configuration")
        print("")
        print("INITIAL LOAD (Historical Data):")
        print("  python moveworks_pipeline.py initial-load  - Load historical data (user selects dates)")
        print("")
        print("DAILY SYNC (UPSERT Mode):")
        print("  python moveworks_pipeline.py run           - Run daily sync once")
        print("  python moveworks_pipeline.py start         - Start daily scheduler")
        print("")
        print("MANAGEMENT:")
        print("  python moveworks_pipeline.py status        - Check configuration")
        print("  python moveworks_pipeline.py reset         - Reset initial load flag")
        print("")
        print("Features:")
        print("  • UPSERT support: Updates existing records, inserts new ones")
        print("  • No duplicate IDs in daily sync")
        print("  • Primary key constraints on all tables")
        print("  • Temporary tables for efficient MERGE operations")
        return
    
    command = sys.argv[1].lower()
    
    if command == 'setup':
        pipeline.setup_config()
        
    elif command == 'initial-load':
        if not pipeline.config:
            print("❌ No configuration found. Run 'python moveworks_pipeline.py setup' first.")
            return
        pipeline.run_initial_load()
        
    elif command == 'run':
        if not pipeline.config:
            print("❌ No configuration found. Run 'python moveworks_pipeline.py setup' first.")
            return
        pipeline.run_once()
        
    elif command == 'start':
        if not pipeline.config:
            print("❌ No configuration found. Run 'python moveworks_pipeline.py setup' first.")
            return
        pipeline.start_scheduler()
        
    elif command == 'status':
        if not pipeline.config:
            print("❌ No configuration found. Run 'python moveworks_pipeline.py setup' first.")
            return
        
        config = pipeline.config
        use_upsert = config['pipeline'].get('use_upsert', True)
        sync_mode = "UPSERT" if use_upsert else "APPEND"
        
        print("\nCurrent Configuration:")
        print("=" * 40)
        print(f"Initial load completed: {'YES' if config['pipeline'].get('initial_load_completed', False) else 'NO'}")
        if config['pipeline'].get('initial_load_date'):
            print(f"Initial load date: {config['pipeline']['initial_load_date']}")
        print(f"Schedule time: {config['pipeline']['schedule_time']} {config['pipeline']['timezone']}")
        print(f"Analytics views: {'YES' if config['pipeline']['create_views'] else 'NO'}")
        print(f"Daily lookback: {config['pipeline']['daily_lookback_days']} days")
        print(f"Sync mode: {sync_mode}")
        print(f"Snowflake DB: {config['snowflake']['database']}.{config['snowflake']['schema']}")
        
    elif command == 'reset':
        if not pipeline.config:
            print("❌ No configuration found. Run 'python moveworks_pipeline.py setup' first.")
            return
        
        confirm = input("⚠️  Reset initial load flag? This will allow you to run initial-load again (y/n): ")
        if confirm.lower() in ['y', 'yes']:
            pipeline.config['pipeline']['initial_load_completed'] = False
            pipeline.config['pipeline'].pop('initial_load_date', None)
            pipeline.save_config(pipeline.config)
            print("✅ Initial load flag reset. You can now run initial-load again.")
        else:
            print("Reset cancelled.")
        
    else:
        print(f"❌ Unknown command: {command}")
        print("Valid commands: setup, initial-load, run, start, status, reset")

if __name__ == "__main__":
    main()
