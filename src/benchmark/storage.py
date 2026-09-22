import time
import statistics
import pandas as pd
from pathlib import Path
import psycopg2
from src.config import DB

def _time_it(func, repeats):
    """Helper to run a function multiple times and get the median time."""
    times = []
    for _ in range(repeats):
        start = time.perf_counter()
        func()
        times.append(time.perf_counter() - start)
    return statistics.median(times)

def run_benchmark(curated_path, output_dir, repeats: int = 5):
    """Compare the same logical dataset in CSV, JSON Lines, Parquet, and PostgreSQL."""
    print(f"--- Running Benchmarks ({repeats} repeats for reads) ---")
    
    curated_path = Path(curated_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    csv_path = output_dir / "sales_order_lines.csv"
    jsonl_path = output_dir / "sales_order_lines.jsonl"
    parquet_path = output_dir / "sales_order_lines_bench.parquet"
    
    # Load base dataset
    df_base = pd.read_parquet(curated_path)
    
    print("Materializing formats...")
    # CSV Write
    start = time.perf_counter()
    df_base.to_csv(csv_path, index=False)
    csv_write_time = time.perf_counter() - start
    csv_size = csv_path.stat().st_size
    
    # JSONL Write
    start = time.perf_counter()
    df_base.to_json(jsonl_path, orient='records', lines=True, date_format='iso')
    jsonl_write_time = time.perf_counter() - start
    jsonl_size = jsonl_path.stat().st_size
    
    # Parquet Write
    start = time.perf_counter()
    df_base.to_parquet(parquet_path, engine='pyarrow', compression='snappy')
    parquet_write_time = time.perf_counter() - start
    parquet_size = parquet_path.stat().st_size

    print("Benchmarking reads...")
    # Full Reads
    csv_read_full = _time_it(lambda: pd.read_csv(csv_path, low_memory=False), repeats)
    jsonl_read_full = _time_it(lambda: pd.read_json(jsonl_path, orient='records', lines=True), repeats)
    parquet_read_full = _time_it(lambda: pd.read_parquet(parquet_path), repeats)
    
    # Filtered Reads
    csv_read_filt = _time_it(lambda: pd.read_csv(csv_path, low_memory=False).query("status == 'DELIVERED'"), repeats)
    jsonl_read_filt = _time_it(lambda: pd.read_json(jsonl_path, orient='records', lines=True).query("status == 'DELIVERED'"), repeats)
    parquet_read_filt = _time_it(lambda: pd.read_parquet(parquet_path, filters=[('status', '==', 'DELIVERED')]), repeats)

    # PostgreSQL Reads
    def pg_full_read():
        with psycopg2.connect(**DB) as conn:
            pd.read_sql("SELECT * FROM curated.sales_order_lines", conn)
            
    def pg_filt_read():
        with psycopg2.connect(**DB) as conn:
            pd.read_sql("SELECT * FROM curated.sales_order_lines WHERE status = 'DELIVERED'", conn)
    
    pg_read_full = _time_it(pg_full_read, repeats)
    pg_read_filt = _time_it(pg_filt_read, repeats)
    
    with psycopg2.connect(**DB) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_total_relation_size('curated.sales_order_lines');")
            pg_size = cur.fetchone()[0]

    print("\n--- BENCHMARK RESULTS ---")
    print(f"{'Metric':<25} | {'CSV':<15} | {'JSONL':<15} | {'Parquet':<15} | {'PostgreSQL':<15}")
    print("-" * 95)
    print(f"{'Size (bytes)':<25} | {csv_size:<15,d} | {jsonl_size:<15,d} | {parquet_size:<15,d} | {pg_size:<15,d} (server)")
    print(f"{'Write Time (sec)':<25} | {csv_write_time:<15.4f} | {jsonl_write_time:<15.4f} | {parquet_write_time:<15.4f} | {'N/A':<15}")
    print(f"{'Full Read Median (sec)':<25} | {csv_read_full:<15.4f} | {jsonl_read_full:<15.4f} | {parquet_read_full:<15.4f} | {pg_read_full:<15.4f}")
    print(f"{'Filtered Read Median (sec)':<25} | {csv_read_filt:<15.4f} | {jsonl_read_filt:<15.4f} | {parquet_read_filt:<15.4f} | {pg_read_filt:<15.4f}")


def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month."""
    print("\nWriting Partitioned Parquet (Task C)...")
    
    # 1. Derive partition columns
    df['order_timestamp'] = pd.to_datetime(df['order_timestamp'])
    df['order_year'] = df['order_timestamp'].dt.year
    df['order_month'] = df['order_timestamp'].dt.month
    
    # 2. Write partitioned structure
    partition_dir = Path(output_dir) / "partitioned"
    df.to_parquet(
        partition_dir,
        engine='pyarrow',
        compression='snappy',
        partition_cols=['order_year', 'order_month']
    )
    print(f"Successfully partitioned data to {partition_dir}/")