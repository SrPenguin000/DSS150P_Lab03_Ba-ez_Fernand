import shutil
from pathlib import Path

def extract_sources(run_id: str) -> str:
    """
    Creates a run-specific raw folder and copies source files into it.
    """
    source_dir = Path("data/source")
    raw_dir = Path(f"data/raw/run_id={run_id}")
    
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    files_to_copy = ["customers.csv", "products.json", "orders.csv"]
    
    for file_name in files_to_copy:
        src_path = source_dir / file_name
        dest_path = raw_dir / file_name
        
        if src_path.exists():
            shutil.copy2(src_path, dest_path)
        else:
            raise FileNotFoundError(f"Required source file missing: {src_path}")
            
    print(f"Successfully extracted sources to: {raw_dir}")
    return str(raw_dir)