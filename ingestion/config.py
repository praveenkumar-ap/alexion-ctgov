# ingestion/config.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
project_root = Path(__file__).parent.parent
load_dotenv(project_root / '.env')

def get_snowflake_config():
    """Get Snowflake configuration from environment variables"""
    return {
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'CLINICAL_TRIALS_DEV'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'RAW'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'TRANSFORM_WH'),
        'role': os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN')
    }

def get_api_config():
    """Get API configuration"""
    return {
        'url': os.getenv('CLINICAL_TRIALS_API_URL', 'https://clinicaltrials.gov/api/v2/studies'),
        'timeout': int(os.getenv('API_TIMEOUT', '30'))
    }