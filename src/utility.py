import snowflake.connector
import os
from os.path import join, dirname
from dotenv import load_dotenv
from typing import Any, Dict, List, Optional
from snowflake.connector.errors import DatabaseError, ProgrammingError
load_dotenv()

# Function to establish Snowflake connection
def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT_LOCATOR'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
    )
    return conn

def get_variables():
    # Get the credentials from .env
    config_dict = {'SF_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT_LOCATOR'),
                'SF_USER': os.getenv('SNOWFLAKE_USER'),
                'SF_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE'),
                'SF_DATABASE': os.getenv('SNOWFLAKE_DATABASE'),
                'SF_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA'),
                'SF_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
                'SF_HOST': os.getenv('SNOWFLAKE_HOST'),
                'SF_ROLE': os.getenv('SNOWFLAKE_ROLE'),
                'SF_PORT': os.getenv('SNOWFLAKE_PORT'),
                'SF_STAGE': os.getenv('SNOWFLAKE_STAGE'),
                'SF_SEMANTIC_FILE_1': os.getenv('SNOWFLAKE_SEMANTIC_FILE_1'),
                'SF_STAGE_CDP':os.getenv('SNOWFLAKE_STAGE_CDP')
                }
    print(config_dict)
    return config_dict

def create_connection(config: dict) -> Any:
    try:
        config = get_variables()

        conn = snowflake.connector.connect(
            user=config['SF_USER'],
            password=config['SF_PASSWORD'],
            account=config['SF_ACCOUNT'],
            host=config['SF_HOST'],
            port=config['SF_PORT'],
            warehouse=config['SF_WAREHOUSE'],
            role=config['SF_ROLE']
        )
    except DatabaseError as db_ex:
        if db_ex.errno == 250001:
            print(f"Invalid username/password, please re-enter username and password...")
            raise
        else:
            raise
    except Exception as ex:
        # Log this
        print(f"Some error you don't know how to handle {ex}")
        raise
    return conn


load_dotenv()
# Function to establish Snowflake connection
# def get_snowflake_connection():
#     conn = snowflake.connector.connect(
#         user=user,
#         password=password,
#         account=account,
#         warehouse='COMPUTE_WH',
#         database='SNOW_DB',
#         schema='SNOW_SCHEMA'
#     )
#     return conn


def get_snowflake_connection_analyze(config: dict) -> Any:
    # create_connection
    try:
        config = get_variables()

        conn = snowflake.connector.connect(
            user=config['SF_USER'],
            password=config['SF_PASSWORD'],
            account=config['SF_ACCOUNT'],
            host=config['SF_HOST'],
            port=config['SF_PORT'],
            warehouse=config['SF_WAREHOUSE'],
            role=config['SF_ROLE']
        )
    except DatabaseError as db_ex:
        if db_ex.errno == 250001:
            print(f"Invalid username/password, please re-enter username and password...")
            raise
        else:
            raise
    except Exception as ex:
        # Log this
        print(f"Some error you don't know how to handle {ex}")
        raise
    return conn
