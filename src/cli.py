import argparse
from src.config import PROJECT_ROOT, DB, SETTINGS
from src.common.audit import new_run_id
from src.extract.extract import extract_sources

def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
    b = sub.add_parser('benchmark'); b.add_argument('--repeats', type=int, default=5)
    p = sub.add_parser('load-partition'); p.add_argument('--year', type=int, required=True); p.add_argument('--month', type=int, required=True)
    sub.add_parser('run-all')
    args = parser.parse_args()

    if args.command == 'validate-env':
        print('PROJECT_ROOT=', PROJECT_ROOT)
        print('DB host/database=', DB['host'], DB['dbname'])
        print('Configured source=', SETTINGS['pipeline']['source_dir'])
        return
        
    elif args.command == 'extract':
        run_id = new_run_id()
        print(f"Starting extraction with run_id: {run_id}")
        extract_sources(run_id)
        return
        
    elif args.command == 'transform':
        from src.transform.staging import build_staging
        from src.transform.curated import build_curated
        from pathlib import Path
    
        try:
            raw_base = Path('data/raw')
            raw_dirs = sorted([d for d in raw_base.iterdir() if d.is_dir()])
            if not raw_dirs:
                raise FileNotFoundError("No raw data found. Run extract first.")
            
            latest_raw = raw_dirs[-1]
            run_id = latest_raw.name.split('=')[1]
        
            print(f"Staging data from {latest_raw}...")
            staging_dfs, quarantine_df = build_staging(latest_raw, run_id)
            print("Staging transformations complete.")
        
            print("Building curated dataset...")
            build_curated(staging_dfs, run_id)
            print("Curated transformations complete.")
        
        except Exception as e:
            print(f"Pipeline Stage Failure [Transform]: {str(e)}")
            raise e
        return

    elif args.command == 'load':
        from src.load.postgres import upsert_curated
        import pandas as pd
        from pathlib import Path
        
        try:
            curated_file = Path("data/curated/sales_order_lines.parquet")
            if not curated_file.exists():
                raise FileNotFoundError("Curated data not found. Run transform first.")
                
            print(f"Loading curated data from {curated_file}...")
            df = pd.read_parquet(curated_file)
            
            run_id = df['pipeline_run_id'].iloc[0]
            
            rows_loaded = upsert_curated(df, run_id)
            print(f"Successfully UPSERTed {rows_loaded} rows into PostgreSQL curated.sales_order_lines.")
            
        except Exception as e:
            print(f"Pipeline Stage Failure [Load]: {str(e)}")
            raise e
        return

    elif args.command == 'validate':
        from src.validate.quality import validate_curated
        import pandas as pd
        from pathlib import Path
        
        try:
            curated_file = Path("data/curated/sales_order_lines.parquet")
            if not curated_file.exists():
                raise FileNotFoundError("Curated data not found. Run transform first.")
                
            print(f"Validating curated data from {curated_file}...")
            df = pd.read_parquet(curated_file)
            
            errors = validate_curated(df)
            
            if errors:
                for error in errors:
                    print(f"{error}")
                raise ValueError(f"Data validation failed with {len(errors)} errors.")
            else:
                print("PASSED: Curated dataset meets all quality constraints.")
                
        except Exception as e:
            print(f"Pipeline Stage Failure [Validate]: {str(e)}")
            raise e
        return
        
    elif args.command == 'benchmark':
        from src.benchmark.storage import run_benchmark, write_partitioned_parquet
        import pandas as pd
        from pathlib import Path
        
        try:
            curated_path = Path("data/curated/sales_order_lines.parquet")
            if not curated_path.exists():
                raise FileNotFoundError("Curated data not found. Run transform first.")
                
            output_dir = Path("data/curated/benchmark")
            run_benchmark(curated_path, output_dir, repeats=args.repeats)
            
            df = pd.read_parquet(curated_path)
            write_partitioned_parquet(df, Path("data"))
            
        except Exception as e:
            print(f"Pipeline Stage Failure [Benchmark]: {str(e)}")
            raise e
        return

    elif args.command == 'load-partition':
        from src.load.postgres import load_partition
        import pandas as pd
        from pathlib import Path
        
        try:
            run_id = new_run_id()
            part_dir = Path(f"data/partitioned/order_year={args.year}/order_month={args.month}")
            
            if not part_dir.exists():
                raise FileNotFoundError(f"Partition directory {part_dir} not found.")
                
            print(f"Loading partitioned data from {part_dir}...")
            df = pd.read_parquet(part_dir)
            
            rows = load_partition(df, args.year, args.month, run_id)
            print(f"Successfully loaded and audited {rows} rows for {args.year}-{args.month}.")
            
        except Exception as e:
            print(f"Pipeline Stage Failure [Load-Partition]: {str(e)}")
            raise e
        return

    elif args.command == 'run-all':
        print("--- Running Full ETL Pipeline ---")
        run_id = new_run_id()
        extract_sources(run_id)
        
        from src.transform.staging import build_staging
        from src.transform.curated import build_curated
        from src.load.postgres import upsert_curated
        from src.validate.quality import validate_curated
        import pandas as pd
        from pathlib import Path
        
        try:
            latest_raw = sorted([d for d in Path('data/raw').iterdir() if d.is_dir()])[-1]
            staging_dfs, quarantine_df = build_staging(latest_raw, run_id)
            build_curated(staging_dfs, run_id)
            
            curated_file = Path("data/curated/sales_order_lines.parquet")
            df = pd.read_parquet(curated_file)
            rows = upsert_curated(df, run_id)
            
            errors = validate_curated(df)
            if errors:
                raise ValueError("Validation failed after load.")
            print(f"--- Pipeline complete! Upserted {rows} rows. ---")
            
        except Exception as e:
            print(f"Pipeline Stage Failure [Run-All]: {str(e)}")
            raise e
        return

    # TODO: Wire the modular functions together. Keep orchestration logic thin.
    raise NotImplementedError(f'Wire command: {args.command}')

if __name__ == '__main__':
    main()