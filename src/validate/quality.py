import pandas as pd

def validate_curated(df: pd.DataFrame) -> list[str]:
    """Return a list of human-readable validation errors."""
    errors = []

    # 1. order_id uniqueness and non-null
    if df['order_id'].isnull().any():
        errors.append("Found NULL order_id values.")
    if df['order_id'].duplicated().any():
        errors.append("Found duplicate order_id values.")

    # 2. quantity range (1 to 20 based on our staging rules)
    if not df['quantity'].between(1, 20).all():
        errors.append("Found quantity outside allowed range (1-20).")

    # 3. nonnegative amounts
    if (df['gross_amount'] < 0).any() or (df['net_amount'] < 0).any():
        errors.append("Found negative gross or net amounts.")

    # 4. allowed statuses
    allowed = {'PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED'}
    if not df['status'].str.upper().isin(allowed).all():
        errors.append("Found invalid order statuses.")

    # 5. required audit fields
    audit_fields = ['pipeline_run_id', 'processed_at_utc', 'record_hash']
    missing_fields = [f for f in audit_fields if f not in df.columns]
    if missing_fields:
        errors.append(f"Missing required audit fields: {', '.join(missing_fields)}")

    return errors