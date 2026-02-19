"""THis script updates the ETL process by populating a newly added Dim_Product.Description. 
"""

import duckdb
import pandas as pd
from pathlib import Path

from analytics.data.connect_db import get_warehouse_conn

def update_warehouse_descriptions():
    # Paths
    PROJECT_ROOT = Path(__file__).resolve().parents[1]

    warehouse_path = PROJECT_ROOT / "warehouse" / "warehouse.duckdb"
    csv_path = PROJECT_ROOT / "data" / "core.Product_with_Descriptions.csv"

    if not csv_path.exists():
        print(f"❌ Error: {csv_path} not found. Please ensure the AI descriptions were generated.")
        return

    # Load the new data
    print("Reading AI-generated descriptions...")
    df_new = pd.read_csv(csv_path)

    # Connect and Update
    conn = duckdb.connect(str(warehouse_path))
    
    try:
        # temp table from df
        conn.register('temp_descriptions', df_new)
        
        # JOINED update
        # This matches on ProductID and fills the Description column
        conn.execute("""
            UPDATE dw.Dim_Product
            SET Description = temp_descriptions.Description
            FROM temp_descriptions
            WHERE dw.Dim_Product.ProductID = temp_descriptions.ProductID
        """)
        
        # Verify
        count = conn.execute("SELECT COUNT(*) FROM dw.Dim_Product WHERE Description IS NOT NULL").fetchone()[0]
        print(f"✅ Success! {count} products updated with descriptions in DuckDB.")

    except Exception as e:
        print(f"❌ critical Error during update: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    update_warehouse_descriptions()