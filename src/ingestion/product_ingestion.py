import os
import requests
import pandas as pd
from datetime import datetime
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.utils.snowflake_connector import get_snowflake_connector

class DummyJSONIngestion:
    """Fetch product data from DummyJSON API and load to Snowflake"""
    
    def __init__(self):
        self.base_url = 'https://dummyjson.com'
    
    def fetch_products(self):
        """Fetch all products from DummyJSON API"""
        print(f"🔍 Fetching products from DummyJSON API...")
        
        try:
            # Get all products (limit=0 returns all)s
            url = f'{self.base_url}/products?limit=0'
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            products = data.get('products', [])
            
            print(f"✅ Fetched {len(products)} products")
            return products
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching data: {e}")
            return []
    
    def clean_products(self, products):
        """Transform raw API data into clean DataFrame"""
        print(f" Cleaning product data...")
        
        cleaned = []
        for product in products:
            cleaned.append({
                'PRODUCT_ID': product.get('id'),
                'PRODUCT_NAME': product.get('title'),
                'BRAND': product.get('brand'),
                'CATEGORY': product.get('category'),
                'PRICE': float(product.get('price', 0)),
                'DISCOUNT_PERCENTAGE': float(product.get('discountPercentage', 0)),
                'RATING': float(product.get('rating', 0)),
                'STOCK': int(product.get('stock', 0)),
                'DESCRIPTION': product.get('description'),
                'THUMBNAIL': product.get('thumbnail'),
                'WEIGHT': product.get('weight'),
                'WIDTH': product.get('dimensions', {}).get('width'),
                'HEIGHT': product.get('dimensions', {}).get('height'),
                'DEPTH': product.get('dimensions', {}).get('depth'),
                'WARRANTY_INFO': product.get('warrantyInformation'),
                'SHIPPING_INFO': product.get('shippingInformation'),
                'AVAILABILITY_STATUS': product.get('availabilityStatus'),
                'RETURN_POLICY': product.get('returnPolicy'),
                'MINIMUM_ORDER_QTY': product.get('minimumOrderQuantity'),
                'BARCODE': product.get('meta', {}).get('barcode'),
                'QR_CODE': product.get('meta', {}).get('qrCode'),
                'GENERATED_AT': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                'SOURCE': 'dummyjson_api'
            })
        
        df = pd.DataFrame(cleaned)
        
        print(f" Cleaned {len(df)} products")
        
        # Show statistics
        print(f"\n Data Summary:")
        print(f"   Total products: {len(df)}")
        
        print(f"\n📋 Sample data:")
        print(df[['PRODUCT_ID', 'PRODUCT_NAME', 'BRAND', 'CATEGORY', 'PRICE', 'RATING', 'STOCK']].head(10))
        
        return df
    
    def save_local(self, df):
        """Save DataFrame to local parquet file (backup)"""
        date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        filepath = f'data/raw/dummyjson_products_{date_str}.parquet'
        
        df.to_parquet(filepath, index=False)
        print(f"\n Saved local backup to: {filepath}")
        
        return filepath
    
    def load_to_snowflake(self, df):
        """Load DataFrame to Snowflake RAW layer"""
        print(f"\n Loading to Snowflake RAW layer...")
        
        sf = get_snowflake_connector()
        
        # Add metadata for tracking
        df['BATCH_ID'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        
        success = sf.load_dataframe(
            df=df,
            table_name='PRODUCTS',
            schema='SOURCE',
            overwrite=False  # Append mode - never delete raw data
        )
        
        return success
    
    def run(self, load_to_snowflake=True, save_local=True):
        """Main execution workflow"""
        print("="*70)
        print(" DUMMYJSON PRODUCT INGESTION")
        print("="*70)
        
        # Step 1: Fetch from API
        products = self.fetch_products()
        
        if not products:
            print(" No products fetched. Exiting.")
            return None
        
        # Step 2: Clean data
        df = self.clean_products(products)
        
        # Step 3: Save local backup
        if save_local:
            self.save_local(df)
        
        # Step 4: Load to Snowflake
        if load_to_snowflake:
            self.load_to_snowflake(df)
        
        print("\n" + "="*70)
        print("✅ INGESTION COMPLETE!")
        print(f"   Products ingested: {len(df)}")
        print(f"   Categories: {df['CATEGORY'].nunique()}")
        print(f"   Brands: {df['BRAND'].nunique()}")
        print("="*70)
        
        return df

if __name__ == "__main__":
    ingestion = DummyJSONIngestion()
    df = ingestion.run()