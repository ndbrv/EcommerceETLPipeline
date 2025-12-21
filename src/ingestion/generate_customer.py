import os
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
import random
import sys

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import Snowflake connector (or use standalone version)
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

class CustomerDataGenerator:
    """Generate realistic fake customer data"""
    
    def __init__(self, num_customers=10000):
        self.num_customers = num_customers
        self.fake = Faker()
        Faker.seed(42)  # For reproducibility
        
        # Snowflake credentials
        self.sf_account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.sf_user = os.getenv('SNOWFLAKE_USER')
        self.sf_password = os.getenv('SNOWFLAKE_PASSWORD')
        self.sf_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        self.sf_database = os.getenv('SNOWFLAKE_DATABASE')
        self.sf_schema = os.getenv('SNOWFLAKE_SCHEMA')
        self.sf_role = os.getenv('SNOWFLAKE_ROLE')
    
    def get_snowflake_connection(self):
        """Create Snowflake connection"""
        return snowflake.connector.connect(
            account=self.sf_account,
            user=self.sf_user,
            password=self.sf_password,
            warehouse=self.sf_warehouse,
            database=self.sf_database,
            schema=self.sf_schema,
            role=self.sf_role
        )
    
    def generate_customers(self):
        """Generate fake customer records"""
        print(f" Generating {self.num_customers:,} fake customers...")
        
        customers = []
        
        for i in range(1, self.num_customers + 1):
            # Random registration date (last 3 years)
            registration_date = self.fake.date_time_between(
                start_date='-3y',
                end_date='now'
            )
            
            # Random last login (within last 30 days for active users)
            is_active = random.choices([True, False], weights=[0.7, 0.3])[0]
            
            if is_active:
                last_login = self.fake.date_time_between(
                    start_date='-30d',
                    end_date='now'
                )
            else:
                last_login = self.fake.date_time_between(
                    start_date='-1y',
                    end_date='-30d'
                )
            
            # Customer segment (based on when they joined)
            days_since_registration = (datetime.now() - registration_date).days
            if days_since_registration < 90:
                segment = 'New'
            elif days_since_registration < 365:
                segment = 'Growing'
            else:
                segment = 'Established'
            
            customer = {
                'CUSTOMER_ID': i,
                'FIRST_NAME': self.fake.first_name(),
                'LAST_NAME': self.fake.last_name(),
                'EMAIL': self.fake.email(),
                'PHONE': self.fake.phone_number(),
                'DATE_OF_BIRTH': self.fake.date_of_birth(minimum_age=18, maximum_age=80),
                'GENDER': random.choice(['Male', 'Female', 'Other', 'Prefer not to say']),
                
                # Address
                'STREET_ADDRESS': self.fake.street_address(),
                'CITY': self.fake.city(),
                'STATE': self.fake.state_abbr(),
                'ZIP_CODE': self.fake.zipcode(),
                'COUNTRY': self.fake.country(),
                
                # Account info
                'REGISTRATION_DATE': registration_date.strftime('%Y-%m-%dT%H:%M:%S'),
                'LAST_LOGIN': last_login.strftime('%Y-%m-%dT%H:%M:%S'),
                'IS_ACTIVE': is_active,
                'EMAIL_VERIFIED': random.choices([True, False], weights=[0.9, 0.1])[0],
                'PHONE_VERIFIED': random.choices([True, False], weights=[0.6, 0.4])[0],
                
                # Marketing preferences
                'MARKETING_OPT_IN': random.choices([True, False], weights=[0.6, 0.4])[0],
                'PREFERRED_CONTACT_METHOD': random.choice(['Email', 'Phone', 'SMS', 'None']),
                
                # Customer segment
                'CUSTOMER_SEGMENT': segment,
                
                # Metadata
                'GENERATED_AT': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                'SOURCE': 'faker_generated'
            }
            
            customers.append(customer)
            
            # Progress indicator
            if i % 1000 == 0:
                print(f"   Generated {i:,} customers...")
        
        df = pd.DataFrame(customers)
        print(f"Generated {len(df):,} customers")
        
        return df
    
    def display_statistics(self, df):
        """Show customer data statistics"""
        print(f"\n Customer Data Summary:")
        print(f"   Total customers: {len(df):,}")
        print(f"   Active customers: {df['IS_ACTIVE'].sum():,} ({df['IS_ACTIVE'].mean()*100:.1f}%)")
        print(f"   Email verified: {df['EMAIL_VERIFIED'].sum():,} ({df['EMAIL_VERIFIED'].mean()*100:.1f}%)")
        print(f"   Marketing opt-in: {df['MARKETING_OPT_IN'].sum():,} ({df['MARKETING_OPT_IN'].mean()*100:.1f}%)")
        
        print(f"\n📊 Customer segments:")
        print(df['CUSTOMER_SEGMENT'].value_counts())
        
        print(f"\n📊 Gender distribution:")
        print(df['GENDER'].value_counts())
        
        print(f"\n📊 Top 10 states:")
        print(df['STATE'].value_counts().head(10))
        
        print(f"\n📋 Sample data:")
        print(df[['CUSTOMER_ID', 'FIRST_NAME', 'LAST_NAME', 'EMAIL', 'CITY', 'STATE', 'CUSTOMER_SEGMENT']].head(10))
    
    def save_local(self, df):
        """Save DataFrame to local parquet file"""
        date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        os.makedirs('data/raw', exist_ok=True)
        filepath = f'data/raw/customers_{date_str}.parquet'
        
        df.to_parquet(filepath, index=False)
        print(f"\n Saved local backup to: {filepath}")
        
        return filepath
    
    def load_to_snowflake(self, df):
        """Load DataFrame to Snowflake"""
        print(f"\n📤 Loading to Snowflake Source layer...")
        
        conn = self.get_snowflake_connection()
        
        # Add metadata
        df['BATCH_ID'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name='customers',
            schema='source',
            database=self.sf_database,
            auto_create_table=True,
            overwrite=False,
            quote_identifiers=False
        )
        
        conn.close()
        
        if success:
            print(f" Loaded {nrows:,} rows to raw.customers")
        
        return success
    
    def run(self, load_to_snowflake=True, save_local=True):
        """Main execution workflow"""
        print("="*70)
        print("🚀 CUSTOMER DATA GENERATION")
        print("="*70)
        
        # Generate customers
        df = self.generate_customers()
        
        # Show statistics
        self.display_statistics(df)
        
        # Save local backup
        if save_local:
            self.save_local(df)
        
        # Load to Snowflake
        if load_to_snowflake:
            self.load_to_snowflake(df)
        
        print("\n" + "="*70)
        print("✅ CUSTOMER GENERATION COMPLETE!")
        print(f"   Total customers: {len(df):,}")
        print("="*70)
        
        return df

if __name__ == "__main__":
    # Generate 10,000 customers
    generator = CustomerDataGenerator(num_customers=10000)
    df = generator.run()