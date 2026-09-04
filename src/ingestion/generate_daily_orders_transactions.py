"""
Generate Daily Transactions (Orders, Order Items, Payment Transactions)
Includes loading to Snowflake
"""

import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import json
from typing import Tuple, Dict, Optional
import os
import sys
from snowflake.connector.pandas_tools import write_pandas

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.utils.id_manager import get_id_manager
from src.utils.snowflake_connector import get_snowflake_connector


# ============================================================
# CORE GENERATION FUNCTIONS (Modular & Reusable)
# ============================================================

def generate_order_items_for_order(order_id: int,
                                   order_item_id_start: int,
                                   num_items: int = None,
                                   min_product_id: int = 1,
                                   max_product_id: int = 194) -> Tuple[list, float]:
    """
    Generate order items for a single order
    
    Args:
        order_id: Order ID these items belong to
        order_item_id_start: Starting order_item_id
        num_items: Number of items (random 1-5 if None)
        min_product_id: Minimum product ID
        max_product_id: Maximum product ID
    
    Returns:
        Tuple of (list of order_items, total_amount)
    """
    
    if num_items is None:
        num_items = random.randint(1, 5)
    
    order_items = []
    order_total = 0
    
    for i in range(num_items):
        item_price = round(random.uniform(10, 200), 2)
        item_qty = random.randint(1, 3)
        item_total = round(item_price * item_qty, 2)
        
        order_item = {
            'SOURCE_ORDER_ITEM_ID': order_item_id_start + i,
            'SOURCE_ORDER_ID': order_id,
            'SOURCE_PRODUCT_ID': random.randint(min_product_id, max_product_id),
            'QUANTITY': item_qty,
            'UNIT_PRICE': item_price,
            'TOTAL_PRICE': item_total,
            'GENERATED_AT': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            'SOURCE': 'faker_generated'
        }
        
        order_items.append(order_item)
        order_total += item_total
    
    return order_items, round(order_total, 2)


def generate_single_order(order_id: int,
                         customer_id: int,
                         total_amount: float,
                         order_items: list,
                         faker: Faker) -> dict:
    """
    Generate a single order
    
    Args:
        order_id: Order ID
        customer_id: Customer ID
        total_amount: Total order amount
        order_items: List of order items
        faker: Faker instance
    
    Returns:
        Order dictionary
    """
    
    order_status = faker.random_element([
        'pending',
        'processing',
        'shipped',
        'delivered',
        'cancelled'
    ])
    
    payment_method = faker.random_element([
        'credit_card',
        'paypal',
        'apple_pay',
        'google_pay',
        'bank_transfer'
    ])
    
    return {
        'SOURCE_ORDER_ID': order_id,
        'SOURCE_CUSTOMER_ID': customer_id,
        'ORDER_DATE': faker.date_time_this_month().strftime('%Y-%m-%dT%H:%M:%S'),
        'ORDER_STATUS': order_status,
        'TOTAL_AMOUNT': total_amount,
        'PAYMENT_METHOD': payment_method,
        'SHIPPING_ADDRESS': faker.address(),
        'GENERATED_AT': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        'SOURCE': 'faker_generated'
    }


def generate_payment_transaction(transaction_id: int,
                                 order: dict,
                                 faker: Faker) -> dict:
    """
    Generate payment transaction for an order
    
    Args:
        transaction_id: Transaction ID
        order: Order dictionary
        faker: Faker instance
    
    Returns:
        Payment transaction dictionary
    """
    
    # Determine transaction status based on order status
    if order['ORDER_STATUS'] == 'pending':
        transaction_status = faker.random_element(['pending', 'processing'])
    elif order['ORDER_STATUS'] == 'cancelled':
        transaction_status = faker.random_element(['declined', 'failed'])
    else:
        transaction_status = 'approved'
    
    return {
        'SOURCE_TRANSACTION_ID': transaction_id,
        'SOURCE_ORDER_ID': order['SOURCE_ORDER_ID'],
        'TRANSACTION_TYPE': 'payment',
        'AMOUNT': order['TOTAL_AMOUNT'],
        'PAYMENT_METHOD': order['PAYMENT_METHOD'],
        'TRANSACTION_STATUS': transaction_status,
        'TRANSACTION_DATE': order['ORDER_DATE'],
        'PROCESSOR_TRANSACTION_ID': faker.uuid4(),
        'PROCESSOR_NAME': faker.random_element(['Stripe', 'PayPal', 'Square']),
        'CARD_LAST_FOUR': faker.credit_card_number()[-4:] if random.random() > 0.3 else None,
        'GENERATED_AT': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        'SOURCE': 'faker_generated'
    }


def generate_refund_transaction(transaction_id: int,
                               order: dict,
                               original_payment: dict,
                               faker: Faker) -> dict:
    """
    Generate refund transaction
    
    Args:
        transaction_id: Transaction ID
        order: Order dictionary
        original_payment: Original payment transaction
        faker: Faker instance
    
    Returns:
        Refund transaction dictionary
    """
    
    # Partial or full refund
    is_partial_refund = random.random() < 0.6
    
    if is_partial_refund:
        refund_amount = round(order['TOTAL_AMOUNT'] * random.uniform(0.3, 0.7), 2)
    else:
        refund_amount = order['TOTAL_AMOUNT']
    
    # Refund happens 1-14 days after order
    refund_date = pd.to_datetime(order['ORDER_DATE']) + timedelta(days=random.randint(1, 14))
    
    return {
        'SOURCE_TRANSACTION_ID': transaction_id,
        'SOURCE_ORDER_ID': order['SOURCE_ORDER_ID'],
        'TRANSACTION_TYPE': 'refund',
        'AMOUNT': -refund_amount,
        'PAYMENT_METHOD': order['PAYMENT_METHOD'],
        'TRANSACTION_STATUS': 'approved',
        'TRANSACTION_DATE': refund_date.strftime('%Y-%m-%dT%H:%M:%S'),
        'PROCESSOR_TRANSACTION_ID': faker.uuid4(),
        'PROCESSOR_NAME': original_payment['PROCESSOR_NAME'],
        'CARD_LAST_FOUR': original_payment.get('CARD_LAST_FOUR'),
        'GENERATED_AT': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        'SOURCE': 'faker_generated'
    }


# ============================================================
# LOADING FUNCTION
# ============================================================

def load_to_snowflake(orders_df: pd.DataFrame,
                     order_items_df: pd.DataFrame,
                     transactions_df: pd.DataFrame,
                     schema: str = 'raw') -> Dict[str, int]:
    """
    Load orders, order items, and payment transactions to Snowflake
    Uses your existing Snowflake connector
    
    Args:
        orders_df: Orders DataFrame
        order_items_df: Order items DataFrame
        transactions_df: Payment transactions DataFrame
        schema: Target schema (default: 'raw')
    
    Returns:
        Dict with row counts loaded
    """
    
    print("\n" + "=" * 70)
    print("📤 LOADING DATA TO SNOWFLAKE")
    print("=" * 70)
    
    # Get connection from your existing connector
    sf_connector = get_snowflake_connector()
    conn = sf_connector.get_connection()
    
    results = {}
    
    try:
        # Load orders (parent table)
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=orders_df,
            table_name='raw_orders',
            schema=schema,
            database=sf_connector.database,
            auto_create_table=True,
            overwrite=False,
            quote_identifiers=False
        )
        results['orders'] = nrows
        print(f"✅ Loaded {nrows:,} orders to {schema}.orders")
        
        # Load order items (child of orders)
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=order_items_df,
            table_name='raw_order_items',
            schema=schema,
            database=sf_connector.database,
            auto_create_table=True,
            overwrite=False,
            quote_identifiers=False
        )
        results['order_items'] = nrows
        print(f"✅ Loaded {nrows:,} order items to {schema}.order_items")
        
        # Load payment transactions (child of orders)
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=transactions_df,
            table_name='raw_transactions',
            schema=schema,
            database=sf_connector.database,
            auto_create_table=True,
            overwrite=False,
            quote_identifiers=False
        )
        results['payment_transactions'] = nrows
        print(f"✅ Loaded {nrows:,} payment transactions to {schema}.payment_transactions")
        
    except Exception as e:
        print(f"❌ Error loading to Snowflake: {e}")
        raise
    finally:
        conn.close()
        print("🔌 Snowflake connection closed")
    
    print("=" * 70)
    print("✅ LOAD COMPLETE")
    print("=" * 70 + "\n")
    
    return results


# ============================================================
# MAIN GENERATION FUNCTION
# ============================================================

def generate_daily_transactions(num_orders: int = 1000,
                                load_to_db: bool = True,
                                schema: str = 'raw') -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Generate daily orders, order items, and payment transactions
    Optionally load to Snowflake
    
    Args:
        num_orders: Number of orders to generate
        load_to_db: Whether to load to Snowflake (default: True)
        schema: Target Snowflake schema (default: 'raw')
    
    Returns:
        Tuple of (orders_df, order_items_df, payment_transactions_df)
    """
    
    faker = Faker()
    id_manager = get_id_manager()
    
    print("\n" + "=" * 70)
    print(f"🏭 GENERATING {num_orders:,} DAILY ORDERS")
    print("=" * 70)
    
    # ============================================================
    # Get ID Ranges
    # ============================================================
    
    order_ids = id_manager.get_next_batch('order', num_orders)
    max_customer_id = id_manager.get_current_max('customer')
    
    if max_customer_id == 0:
        print("⚠️  No customers exist! Generate customers first.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    print(f"📊 Order IDs: {order_ids[0]:,} to {order_ids[-1]:,}")
    print(f"👥 Customer range: 1 to {max_customer_id:,}")
    
    # ============================================================
    # Generate Data
    # ============================================================
    
    orders = []
    all_order_items = []
    all_transactions = []
    
    order_item_counter = id_manager.get_current_max('order_item')
    transaction_counter = id_manager.get_current_max('payment_transaction')
    
    for order_id in order_ids:
        
        # Random customer
        customer_id = random.randint(1, max_customer_id)
        
        # Generate order items
        order_items, order_total = generate_order_items_for_order(
            order_id=order_id,
            order_item_id_start=order_item_counter + 1
        )
        
        # Update counter
        order_item_counter += len(order_items)
        
        # Generate order
        order = generate_single_order(
            order_id=order_id,
            customer_id=customer_id,
            total_amount=order_total,
            order_items=order_items,
            faker=faker
        )
        
        orders.append(order)
        all_order_items.extend(order_items)
        
        # Generate payment transaction(s)
        if order['ORDER_STATUS'] != 'cancelled':
            
            # Initial payment
            transaction_counter += 1
            
            payment_txn = generate_payment_transaction(
                transaction_id=transaction_counter,
                order=order,
                faker=faker
            )
            
            all_transactions.append(payment_txn)
            
            # Potentially generate refund (10% chance for delivered orders)
            if order['ORDER_STATUS'] == 'delivered' and random.random() < 0.10:
                transaction_counter += 1
                
                refund_txn = generate_refund_transaction(
                    transaction_id=transaction_counter,
                    order=order,
                    original_payment=payment_txn,
                    faker=faker
                )
                
                all_transactions.append(refund_txn)
        
        else:  # Cancelled order
            # 50% chance there was a failed payment attempt
            if random.random() < 0.5:
                transaction_counter += 1
                
                failed_txn = generate_payment_transaction(
                    transaction_id=transaction_counter,
                    order=order,
                    faker=faker
                )
                
                all_transactions.append(failed_txn)
    
    # ============================================================
    # Update ID Manager and Create DataFrames
    # ============================================================
    
    id_manager.last_ids['order_item'] = order_item_counter
    id_manager.last_ids['payment_transaction'] = transaction_counter
    id_manager.save()
    
    orders_df = pd.DataFrame(orders)
    order_items_df = pd.DataFrame(all_order_items)
    transactions_df = pd.DataFrame(all_transactions)
    
    # ============================================================
    # Summary
    # ============================================================
    
    print("\n" + "=" * 70)
    print("✅ GENERATION COMPLETE")
    print("=" * 70)
    print(f"📦 Orders: {len(orders_df):,}")
    print(f"📋 Order Items: {len(order_items_df):,}")
    print(f"💳 Payment Transactions: {len(transactions_df):,}")
    
    if len(transactions_df) > 0:
        print("\n💰 Transaction Breakdown:")
        print(transactions_df['TRANSACTION_TYPE'].value_counts().to_string())
        
        print("\n📊 Transaction Status:")
        print(transactions_df['TRANSACTION_STATUS'].value_counts().to_string())
        
        # Revenue summary
        net_revenue = transactions_df['AMOUNT'].sum()
        gross_revenue = transactions_df[transactions_df['TRANSACTION_TYPE'] == 'payment']['AMOUNT'].sum()
        refunds_total = transactions_df[transactions_df['TRANSACTION_TYPE'] == 'refund']['AMOUNT'].sum()
        
        print(f"\n💵 Revenue:")
        print(f"   Gross: ${gross_revenue:,.2f}")
        print(f"   Refunds: ${refunds_total:,.2f}")
        print(f"   Net: ${net_revenue:,.2f}")
    
    print("=" * 70 + "\n")
    
    # ============================================================
    # Load to Snowflake (Optional)
    # ============================================================
    
    if load_to_db:
        load_results = load_to_snowflake(orders_df, order_items_df, transactions_df, schema)
        print(f"📊 Load Results: {load_results}")
    
    return orders_df, order_items_df, transactions_df


# ============================================================
# ALTERNATIVE FUNCTIONS (Optional Use Cases)
# ============================================================

def generate_orders_only(num_orders: int = 1000,
                        load_to_db: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generate only orders and order items (no payment transactions)
    
    Args:
        num_orders: Number of orders to generate
        load_to_db: Whether to load to Snowflake (default: False)
    
    Returns:
        Tuple of (orders_df, order_items_df)
    """
    
    orders_df, items_df, _ = generate_daily_transactions(num_orders, load_to_db=False)
    
    if load_to_db:
        sf_connector = get_snowflake_connector()
        conn = sf_connector.get_connection()
        try:
            # Load orders
            write_pandas(
                conn=conn,
                df=orders_df,
                table_name='raw_orders',
                schema='raw',
                database=sf_connector.database,
                auto_create_table=True,
                overwrite=False,
                quote_identifiers=False
            )
            # Load order items
            write_pandas(
                conn=conn,
                df=items_df,
                table_name='raw_order_items',
                schema='raw',
                database=sf_connector.database,
                auto_create_table=True,
                overwrite=False,
                quote_identifiers=False
            )
            print(f"✅ Loaded {len(orders_df)} orders and {len(items_df)} items")
        finally:
            conn.close()
    
    return orders_df, items_df


def generate_transactions_for_orders(orders_df: pd.DataFrame,
                                     load_to_db: bool = False) -> pd.DataFrame:
    """
    Generate payment transactions for existing orders
    Useful for backfilling transactions
    
    Args:
        orders_df: Existing orders DataFrame
        load_to_db: Whether to load to Snowflake (default: False)
    
    Returns:
        Payment transactions DataFrame
    """
    
    faker = Faker()
    id_manager = get_id_manager()
    
    all_transactions = []
    transaction_counter = id_manager.get_current_max('payment_transaction')
    
    for _, order in orders_df.iterrows():
        
        # Generate payment
        if order['ORDER_STATUS'] != 'cancelled':
            transaction_counter += 1
            
            payment_txn = generate_payment_transaction(
                transaction_id=transaction_counter,
                order=order.to_dict(),
                faker=faker
            )
            
            all_transactions.append(payment_txn)
            
            # Potential refund
            if order['ORDER_STATUS'] == 'delivered' and random.random() < 0.10:
                transaction_counter += 1
                
                refund_txn = generate_refund_transaction(
                    transaction_id=transaction_counter,
                    order=order.to_dict(),
                    original_payment=payment_txn,
                    faker=faker
                )
                
                all_transactions.append(refund_txn)
    
    id_manager.last_ids['payment_transaction'] = transaction_counter
    id_manager.save()
    
    transactions_df = pd.DataFrame(all_transactions)
    
    if load_to_db:
        sf_connector = get_snowflake_connector()
        conn = sf_connector.get_connection()
        try:
            write_pandas(
                conn=conn,
                df=transactions_df,
                table_name='raw_transactions',
                schema='raw',
                database=sf_connector.database,
                auto_create_table=True,
                overwrite=False,
                quote_identifiers=False
            )
            print(f"✅ Loaded {len(transactions_df)} transactions")
        finally:
            conn.close()
    
    return transactions_df


# ============================================================
# MAIN EXECUTION (for testing)
# ============================================================

if __name__ == "__main__":
    
    # Test: Generate and preview (don't load)
    print("🧪 TEST MODE: Generating data without loading to Snowflake\n")
    
    orders_df, items_df, transactions_df = generate_daily_transactions(
        num_orders=100,
        load_to_db=True  # Don't load when testing
    )
    
    # Preview
    print("\n SAMPLE ORDERS:")
    print(orders_df.head())
    ##print(orders_df[['order_id', 'customer_id', 'order_date', 'order_status', 'total_amount']].head())
    
    print("\n SAMPLE ORDER ITEMS:")
    print(items_df.head())
    ##print(items_df.head())
    
    print("\n SAMPLE PAYMENT TRANSACTIONS:")
    print(transactions_df.head())
    ##print(transactions_df[['transaction_id', 'order_id', 'transaction_type', 'amount', 'transaction_status']].head(10))
    
   