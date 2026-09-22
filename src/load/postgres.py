import psycopg2
import psycopg2.extras
import pandas as pd
from src.config import DB

def upsert_curated(df: pd.DataFrame, run_id: str) -> int:
    """
    Load curated.sales_order_lines using rerun-safe UPSERT semantics.
    Dynamically filters dataframe columns to match the target database schema.
    """
    conn = psycopg2.connect(**DB)
    try:
        # 1. Ask PostgreSQL for the exact columns in the target table
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'curated' AND table_name = 'sales_order_lines'"
            )
            db_cols = [row[0] for row in cur.fetchall()]
            
        if not db_cols:
            raise ValueError("Could not find table curated.sales_order_lines in the database.")

        # 2. Filter our dataframe to ONLY keep columns that exist in the database
        cols_to_keep = [c for c in df.columns if c in db_cols]
        df_clean = df[cols_to_keep].copy()
        
        # 3. Convert NaNs and NaTs to standard Python None for DB compatibility
        df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)
        
        # 4. Build the dynamic SQL query
        cols_str = ', '.join(cols_to_keep)
        set_clauses = ', '.join([f"{col} = EXCLUDED.{col}" for col in cols_to_keep if col != 'order_id'])
        
        query = f"""
            INSERT INTO curated.sales_order_lines ({cols_str})
            VALUES %s
            ON CONFLICT (order_id) 
            DO UPDATE SET {set_clauses}
            WHERE sales_order_lines.record_hash IS DISTINCT FROM EXCLUDED.record_hash;
        """
        
        # 5. Execute the bulk UPSERT
        data_tuples = [tuple(x) for x in df_clean.to_numpy()]
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, query, data_tuples)
            
        conn.commit()
        return len(df_clean)
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def load_partition(df: pd.DataFrame, year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads."""
    # 1. Upsert the dataframe using our existing robust function
    rows_loaded = upsert_curated(df, run_id)
    
    # 2. Record the load event in the audit table
    conn = psycopg2.connect(**DB)
    try:
        partition_key = f"order_year={year}/order_month={month}"
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO audit.partition_loads 
                (partition_key, pipeline_run_id, loaded_at_utc, row_count)
                VALUES (%s, %s, NOW() AT TIME ZONE 'UTC', %s)
                ON CONFLICT (partition_key) 
                DO UPDATE SET 
                    pipeline_run_id = EXCLUDED.pipeline_run_id,
                    loaded_at_utc = EXCLUDED.loaded_at_utc,
                    row_count = EXCLUDED.row_count;
            """, (partition_key, run_id, rows_loaded))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()
        
    return rows_loaded