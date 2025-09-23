import os
import sys
from main import create_ingestion_service

# Set test environment variables
os.environ['SNOWFLAKE_USER'] = 'your_username'
os.environ['SNOWFLAKE_PASSWORD'] = 'your_password' 
os.environ['SNOWFLAKE_ACCOUNT'] = 'your_account'
os.environ['ENVIRONMENT'] = 'dev'
os.environ['MAX_PAGES'] = '1'  # Limit to 1 page for testing

def test_ingestion():
    """Test the ingestion pipeline"""
    try:
        print("🚀 Starting ingestion test...")
        
        service = create_ingestion_service()
        result = service.run_ingestion()
        
        if result.success:
            print(f"✅ SUCCESS: Processed {result.studies_processed} studies")
            print(f"⏱️  Execution time: {result.execution_time:.2f} seconds")
            print(f"📦 Batch ID: {result.batch_id}")
        else:
            print(f"❌ FAILED: {result.error_message}")
            
    except Exception as e:
        print(f"💥 ERROR: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = test_ingestion()
    sys.exit(0 if success else 1)