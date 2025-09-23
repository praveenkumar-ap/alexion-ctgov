# ingestion/test_connection.py
import snowflake.connector
import os

# Use your exact account identifier
os.environ['SNOWFLAKE_ACCOUNT'] = 'WCOFXCF-NO82177'
os.environ['SNOWFLAKE_USER'] = 'PRAVEENSETUP96'
os.environ['SNOWFLAKE_PASSWORD'] = 'Hare@1830198766'

try:
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        database='CLINICAL_TRIALS_DEV',
        schema='RAW',
        warehouse='TRANSFORM_WH'
    )
    print("✅ Snowflake connection successful!")
    
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION()")
    version = cursor.fetchone()[0]
    print(f"✅ Snowflake version: {version}")
    
    # Test if our table exists
    cursor.execute("SELECT COUNT(*) FROM RAW_CTGOV_STUDIES")
    count = cursor.fetchone()[0]
    print(f"✅ Table has {count} records")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Snowflake connection failed: {e}")