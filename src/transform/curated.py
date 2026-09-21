import pandas as pd
import hashlib
from datetime import datetime, timezone
from pathlib import Path

def build_curated(staging: dict, run_id: str):
    """Join staging orders/customers/products and create analysis-ready sales rows."""
    
    # 1. Extract the DataFrames passed from the staging step
    customers = staging.get('customers')
    products = staging.get('products')
    orders = staging.get('orders')

    # 2. Join Orders to Customers and Products
    df = orders.merge(customers, on="customer_id", how="left", suffixes=("", "_cust"), indicator='_merge_cust')
    df = df.merge(products, on="product_id", how="left", suffixes=("", "_prod"), indicator='_merge_prod')

    # 3. Quarantine Orphan Records
    is_orphan = (df['_merge_cust'] == 'left_only') | (df['_merge_prod'] == 'left_only')
    orphans = df[is_orphan].copy()
    
    if not orphans.empty:
        orphans['quarantine_reason'] = "Orphan reference: Missing customer or product"
        q_dir = Path("data/quarantine")
        q_dir.mkdir(parents=True, exist_ok=True)
        orphans.to_csv(q_dir / f"quarantine_orphans_{run_id}.csv", index=False)

    # 4. Process Valid Records
    valid = df[~is_orphan].copy()
    
    if 'discount_pct' not in valid.columns:
        valid['discount_pct'] = 0.0
    else:
        valid['discount_pct'] = pd.to_numeric(valid['discount_pct'], errors='coerce').fillna(0.0)

    # Calculate financial metrics
    valid['gross_amount'] = valid['quantity'] * valid['unit_price']
    valid['discount_amount'] = valid['gross_amount'] * valid['discount_pct']
    valid['net_amount'] = valid['gross_amount'] - valid['discount_amount']

    # 5. Audit Columns & Deterministic Hash
    valid['pipeline_run_id'] = run_id
    valid['processed_at_utc'] = datetime.now(timezone.utc)
    valid['source_updated_at'] = valid[['updated_at', 'updated_at_cust', 'updated_at_prod']].max(axis=1)

    hash_cols = ['order_id', 'customer_id', 'product_id', 'quantity', 'status', 'net_amount']
    
    def generate_hash(row):
        val_str = "|".join([str(row[c]) for c in hash_cols])
        return hashlib.sha256(val_str.encode('utf-8')).hexdigest()
        
    valid['record_hash'] = valid.apply(generate_hash, axis=1)

    # Clean up temporary merge columns
    cols_to_drop = [c for c in valid.columns if c.endswith('_cust') or c.endswith('_prod') or c.startswith('_merge')]
    valid = valid.drop(columns=cols_to_drop)

    # Save to Curated layer
    out_dir = Path("data/curated")
    out_dir.mkdir(parents=True, exist_ok=True)
    valid.to_parquet(out_dir / "sales_order_lines.parquet", index=False)
    
    print(f"Curated {len(valid)} sales order lines. Quarantined {len(orphans)} orphans.")
    return valid