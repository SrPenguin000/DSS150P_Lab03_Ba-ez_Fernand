import pandas as pd
import json
from datetime import datetime, timezone
from pathlib import Path
from src.config import SETTINGS

def build_staging(raw_dir, run_id: str):
    """Create cleaned, typed staging datasets with robust quarantine checks."""
    raw_path = Path(raw_dir)
    staged_at = datetime.now(timezone.utc)
    quarantine_frames = []

    # 1. Customers
    df_cust = pd.read_csv(raw_path / "customers.csv")
    df_cust['updated_at'] = pd.to_datetime(df_cust['updated_at'], utc=True)
    
    # Quarantine Null Customer IDs before groupby
    null_cust = df_cust['customer_id'].isna()
    if null_cust.any():
        q_cust = df_cust[null_cust].copy()
        q_cust['quarantine_reason'] = 'Missing customer_id'
        quarantine_frames.append(q_cust)
    
    df_cust = df_cust[~null_cust].sort_values('updated_at').groupby('customer_id').tail(1)
    df_cust['email'] = df_cust['email'].str.lower().str.strip()
    df_cust['city'] = df_cust['city'].str.strip().str.title()
    
    # 2. Products
    with open(raw_path / "products.json", 'r') as f:
        df_prod = pd.json_normalize(json.load(f))
    
    df_prod['updated_at'] = pd.to_datetime(df_prod['updated_at'], utc=True)
    df_prod['unit_price'] = pd.to_numeric(df_prod['unit_price'], errors='coerce')
    
    # Quarantine Null Product IDs
    null_prod = df_prod['product_id'].isna()
    if null_prod.any():
        q_prod_null = df_prod[null_prod].copy()
        q_prod_null['quarantine_reason'] = 'Missing product_id'
        quarantine_frames.append(q_prod_null)
        
    df_prod = df_prod[~null_prod]
    
    # Quarantine invalid prices
    invalid_price = df_prod['unit_price'].isna() | (df_prod['unit_price'] < 0)
    if invalid_price.any():
        q_prod = df_prod[invalid_price].copy()
        q_prod['quarantine_reason'] = 'Invalid or negative unit_price'
        quarantine_frames.append(q_prod)
    
    df_prod = df_prod[~invalid_price].sort_values('updated_at').groupby('product_id').tail(1)

    # 3. Orders
    df_ord = pd.read_csv(raw_path / "orders.csv")
    df_ord['updated_at'] = pd.to_datetime(df_ord['updated_at'], utc=True)
    df_ord['order_timestamp'] = pd.to_datetime(df_ord['order_timestamp'], utc=True)
    df_ord['quantity'] = pd.to_numeric(df_ord['quantity'], errors='coerce')
    
    # Quarantine Null Order IDs
    null_ord = df_ord['order_id'].isna()
    if null_ord.any():
        q_ord_null = df_ord[null_ord].copy()
        q_ord_null['quarantine_reason'] = 'Missing order_id'
        quarantine_frames.append(q_ord_null)
        
    df_ord = df_ord[~null_ord]
    
    # Validate quantity (1 to 20 AND no fractions) and dynamic statuses
    allowed_statuses = set(SETTINGS.get('order_statuses', ['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED']))
    
    valid_qty = df_ord['quantity'].between(1, 20) & (df_ord['quantity'] % 1 == 0)
    valid_status = df_ord['status'].str.upper().isin(allowed_statuses)
    
    invalid_ord = ~(valid_qty & valid_status)
    if invalid_ord.any():
        q_ord = df_ord[invalid_ord].copy()
        q_ord['quarantine_reason'] = 'Invalid quantity (fraction/out-of-bounds) or status'
        quarantine_frames.append(q_ord)
    
    df_ord = df_ord[~invalid_ord].sort_values('updated_at').groupby('order_id').tail(1)

    # Apply audit columns and save to run-specific Parquet directory
    staging_dfs = {'customers': df_cust, 'products': df_prod, 'orders': df_ord}
    out_dir = Path(f"data/staging/run_id={run_id}")
    out_dir.mkdir(parents=True, exist_ok=True)
    
    for name, df in staging_dfs.items():
        df['pipeline_run_id'] = run_id
        df['staged_at_utc'] = staged_at
        df.to_parquet(out_dir / f"{name}.parquet", index=False)
        
    # Handle Quarantine compilation
    quarantine_df = pd.concat(quarantine_frames, ignore_index=True) if quarantine_frames else pd.DataFrame()
    if not quarantine_df.empty:
        q_dir = Path(f"data/quarantine/run_id={run_id}")
        q_dir.mkdir(parents=True, exist_ok=True)
        quarantine_df.to_csv(q_dir / "quarantine.csv", index=False)

    return staging_dfs, quarantine_df