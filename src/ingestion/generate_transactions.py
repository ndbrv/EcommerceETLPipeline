import os
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
import random
import sys
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

class TransactionDataGenerator:
    """Generate realistic fake transaction data"""
    
    def __init__(self, num_transactions=100000):
        self.num_transactions = num_transactions
        self.fake = Faker()
        Faker.seed(42)
        
        # Snowflake credentials
        self.sf_account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.sf_user = os.getenv('SNOWFLAKE_USER')
        self.sf_password = os.getenv('SNOWFLAKE_PASSWORD')
        self.sf_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        self.sf_database = os.getenv('SNOWFLAKE_DATABASE')
        self.sf_schema = os.getenv('SNOWFLAKE_SCHEMA')
        self.sf_role = os.getenv('SNOWFLAKE_ROLE')
        
        # Will load products and customers from Snowflake
        self.products_df = None
        self.customers_df = None
    
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
    
    def load_reference_data(self):
        """Load products and customers from Snowflake"""
        print(" Loading reference data from Snowflake...")
        
        conn = self.get_snowflake_connection()
        
        # Load products
        print("   Loading products...")
        self.products_df = pd.read_sql(
            "SELECT product_id, product_name, category, price, stock FROM source.products",
            conn
        )
        print(f"Loaded {len(self.products_df)} products")
        
        # Load customers
        print("Loading customers...")
        self.customers_df = pd.read_sql(
            "SELECT customer_id, customer_segment, is_active FROM source.customers",
            conn
        )
        print(f" Loaded {len(self.customers_df)} customers")
        
        conn.close()
    
    def generate_transactions(self):
        """Generate fake transaction records"""
        print(f"\nGenerating {self.num_transactions:,} fake transactions...")
        
        if self.products_df is None or self.customers_df is None:
            self.load_reference_data()
        
        transactions = []
        
        # Get list of product IDs and customer IDs for sampling
        product_ids = self.products_df['PRODUCT_ID'].tolist()
        customer_ids = self.customers_df['CUSTOMER_ID'].tolist()
        
        # Active customers are more likely to make purchases
        active_customer_ids = self.customers_df[
            self.customers_df['IS_ACTIVE'] == True
        ]['CUSTOMER_ID'].tolist()
        
        for i in range(1, self.num_transactions + 1):
            # 80% of transactions from active customers
            if random.random() < 0.8:
                customer_id = random.choice(active_customer_ids)
            else:
                customer_id = random.choice(customer_ids)
            
            # Random order date (last 2 years, with more recent weighted higher)
            days_ago = int(random.expovariate(1/180))  # Exponential distribution
            days_ago = min(days_ago, 730)  # Cap at 2 years
            order_date = (datetime.now() - timedelta(days=days_ago))
            
            # Number of items in order (1-5, most orders have 1-2 items)
            num_items = random.choices([1, 2, 3, 4, 5], weights=[0.5, 0.3, 0.12, 0.05, 0.03])[0]
            
            # Select random products for this order
            order_products = random.sample(product_ids, k=num_items)
            
            # Calculate order totals
            order_items = []
            subtotal = 0
            
            for product_id in order_products:
                product = self.products_df[self.products_df['PRODUCT_ID'] == product_id].iloc[0]
                quantity = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]
                
                item_price = float(product['PRICE'])
                item_total = item_price * quantity
                subtotal += item_total
                
                order_items.append({
                    'product_id': product_id,
                    'product_name': product['PRODUCT_NAME'],
                    'quantity': quantity,
                    'unit_price': item_price,
                    'item_total': item_total
                })
            
            # Calculate tax and shipping
            tax_rate = 0.08  # 8% sales tax
            tax = round(subtotal * tax_rate, 2)
            
            # Shipping: free over $50, otherwise $5.99
            shipping = 0 if subtotal > 50 else 5.99
            
            total = round(subtotal + tax + shipping, 2)
            
            # Order status (90% completed, 5% cancelled, 5% pending)
            status = random.choices(
                ['Completed', 'Cancelled', 'Pending', 'Shipped'],
                weights=[0.85, 0.05, 0.03, 0.07]
            )[0]
            
            # Payment method
            payment_method = random.choices(
                ['Credit Card', 'Debit Card', 'PayPal', 'Apple Pay', 'Google Pay'],
                weights=[0.50, 0.25, 0.15, 0.06, 0.04]
            )[0]
            
            # Shipping method
            shipping_method = random.choices(
                ['Standard', 'Express', 'Next Day'],
                weights=[0.75, 0.20, 0.05]
            )[0]
            
            transaction = {
                'TRANSACTION_ID': i,
                'ORDER_ID': f'ORD-{i:08d}',
                'CUSTOMER_ID': customer_id,
                'ORDER_DATE': order_date.strftime('%Y-%m-%dT%H:%M:%S'),
                'ORDER_STATUS': status,
                
                # Order totals
                'SUBTOTAL': round(subtotal, 2),
                'TAX': tax,
                'SHIPPING_COST': shipping,
                'TOTAL_AMOUNT': total,
                
                # Order details
                'NUM_ITEMS': num_items,
                'ITEMS_DETAIL': str(order_items),  # Store as JSON string
                
                # Payment info
                'PAYMENT_METHOD': payment_method,
                'SHIPPING_METHOD': shipping_method,
                
                # Metadata
                'GENERATED_AT': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                'SOURCE': 'faker_generated'
            }
            
            transactions.append(transaction)
            
            # Progress indicator
            if i % 10000 == 0:
                print(f"   Generated {i:,} transactions...")
        
        df = pd.DataFrame(transactions)
        print(f"✅ Generated {len(df):,} transactions")
        
        return df
    
    def display_statistics(self, df):
        """Show transaction data statistics"""
        print(f"\n📊 Transaction Data Summary:")
        print(f"   Total transactions: {len(df):,}")
        print(f"   Total revenue: ${df['TOTAL_AMOUNT'].sum():,.2f}")
        print(f"   Average order value: ${df['TOTAL_AMOUNT'].mean():.2f}")
        print(f"   Median order value: ${df['TOTAL_AMOUNT'].median():.2f}")
        
        print(f"\n📊 Order status distribution:")
        print(df['ORDER_STATUS'].value_counts())
        
        print(f"\n📊 Payment method distribution:")
        print(df['PAYMENT_METHOD'].value_counts())
        
        print(f"\n📊 Items per order:")
        print(df['NUM_ITEMS'].value_counts().sort_index())
        
        # Revenue by month
        monthly_revenue = df.groupby('ORDER_DATE')['TOTAL_AMOUNT'].sum()
        print(f"\n📊 Monthly revenue (last 6 months):")
        print(monthly_revenue.tail(6))
        
        print(f"\n📋 Sample transactions:")
    
    def save_local(self, df):
        """Save DataFrame to local parquet file"""
        date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        os.makedirs('data/source', exist_ok=True)
        filepath = f'data/source/transactions_{date_str}.parquet'
        
        df.to_parquet(filepath, index=False)
        print(f"\n💾 Saved local backup to: {filepath}")
        
        return filepath
    
    def load_to_snowflake(self, df):
        """Load DataFrame to Snowflake"""
        print(f"\n Loading to Snowflake source layer...")
        
        conn = self.get_snowflake_connection()

        # Add metadata
        df['BATCH_ID'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name='transactions',
            schema='source',
            database=self.sf_database,
            auto_create_table=True,
            overwrite=False,
            quote_identifiers=False
        )
        
        conn.close()
        
        if success:
            print(f"✅ Loaded {nrows:,} rows to sourec.transactions")
        
        return success
    
    def run(self, load_to_snowflake=True, save_local=True):
        """Main execution workflow"""
        print("="*70)
        print("TRANSACTION DATA GENERATION")
        print("="*70)
        
        # Generate transactions
        df = self.generate_transactions()
        
        # Show statistics
        self.display_statistics(df)
        
        # Save local backup
        if save_local:
            self.save_local(df)
        
        # Load to Snowflake
        if load_to_snowflake:
            self.load_to_snowflake(df)
        
        print("\n" + "="*70)
        print(" TRANSACTION GENERATION COMPLETE!")
        print(f"   Total transactions: {len(df):,}")
        print(f"   Total revenue: ${df['TOTAL_AMOUNT'].sum():,.2f}")
        print("="*70)
        
        return df

if __name__ == "__main__":
    # Generate 100,000 transactions
    generator = TransactionDataGenerator(num_transactions=100000)
    df = generator.run()