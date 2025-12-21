import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

class SnowflakeConnector:
    """Handle all Snowflake connections and operations"""
    
    def __init__(self):
        self.account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.user = os.getenv('SNOWFLAKE_USER')
        self.password = os.getenv('SNOWFLAKE_PASSWORD')
        self.warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        self.database = os.getenv('SNOWFLAKE_DATABASE')
        self.schema = os.getenv('SNOWFLAKE_SCHEMA')
        self.role = os.getenv('SNOWFLAKE_ROLE')
    
    def get_connection(self):
        """Create Snowflake connection"""
        return snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
            role=self.role
        )
    
    def execute_query(self, query):
        """Execute a SQL query and return results"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            conn.close()
    
    def load_dataframe(self, df, table_name, schema='source', overwrite=False):
        """
        Load pandas DataFrame directly into Snowflake
        This is the fastest way to load data!
        """
        conn = self.get_connection()
        
        try:
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=table_name,
                schema=schema,
                database=self.database,
                auto_create_table=True,  # Creates table if doesn't exist
                overwrite=overwrite
            )
            
            if success:
                print(f"✅ Loaded {nrows} rows to {schema}.{table_name}")
                return True
            else:
                print(f"❌ Failed to load data")
                return False
        finally:
            conn.close()
    
    def test_connection(self):
        """Test if connection works"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    CURRENT_VERSION() as version,
                    CURRENT_WAREHOUSE() as warehouse,
                    CURRENT_DATABASE() as database,
                    CURRENT_SCHEMA() as schema
            """)
            
            result = cursor.fetchone()
            
            print("✅ Snowflake Connection Successful!")
            print(f"   Version: {result[0]}")
            print(f"   Warehouse: {result[1]}")
            print(f"   Database: {result[2]}")
            print(f"   Schema: {result[3]}")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False

# Convenience function
def get_snowflake_connector():
    return SnowflakeConnector()

if __name__ == "__main__":
    # Test connection when running this file directly
    sf = SnowflakeConnector()
    sf.test_connection()
