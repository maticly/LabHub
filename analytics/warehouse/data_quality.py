import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path
import os

def run_dq_checks():
    # 1. DYNAMIC PATHING
    script_path = Path(__file__).resolve() 
    project_root = script_path.parent
    db_path = project_root / 'warehouse.duckdb'
    
    # 2. VITAL DEBUG PRINT
    print(f"🔍 Searching for database at: {db_path}")

    if not db_path.exists():
        print(f"❌ ERROR: Database file NOT FOUND at {db_path}")
        return False

    # Connect to the verified path
    conn = duckdb.connect(str(db_path))
    checks_passed = True

    print(f"--- 🛡️ LabHub Warehouse Audit: {datetime.now().strftime('%Y-%m-%d %H:%M')} ---")

    try:
        # --- CHECK 1: Negative Inventory Check ---
        neg_stock = conn.execute("""
            SELECT COUNT(*) 
            FROM dw.Fact_Inventory_Transactions 
            WHERE AbsoluteQuantity < 0
        """).fetchone()[0]

        if neg_stock > 0:
            print(f"❌ FAIL: Found {neg_stock} records with negative AbsoluteQuantity!")
            checks_passed = False
        else:
            print("✅ PASS: No negative stock levels detected.")

        # --- CHECK 2: Orphaned Transactions ---
        orphans = conn.execute("""
            SELECT COUNT(*) 
            FROM dw.Fact_Inventory_Transactions f
            LEFT JOIN dw.Dim_Product p ON f.ProductKey = p.ProductKey
            WHERE p.ProductKey IS NULL
        """).fetchone()[0]

        if orphans > 0:
            print(f"❌ FAIL: Found {orphans} orphaned transactions (missing Product mapping)!")
            checks_passed = False
        else:
            print("✅ PASS: All transactions mapped to valid Products.")

        # --- CHECK 3: Future Dating Check ---
        future_dates = conn.execute("""
            SELECT COUNT(*) 
            FROM dw.Fact_Inventory_Transactions 
            WHERE DateKey > CAST(strftime(CURRENT_DATE, '%Y%m%d') AS INTEGER)
        """).fetchone()[0]

        if future_dates > 0:
            print(f"❌ FAIL: Found {future_dates} transactions with future dates!")
            checks_passed = False
        else:
            print("✅ PASS: All transaction dates are valid.")

        # --- CHECK 4: Freshness ---
        latest_date_key = conn.execute("SELECT MAX(DateKey) FROM dw.Fact_Inventory_Transactions").fetchone()[0]
        if latest_date_key:
            today_key = int(datetime.now().strftime('%Y%m%d'))
            if latest_date_key < today_key:
                print(f"⚠️  WARNING: Data is STALE. Last update: {latest_date_key}.")
            else:
                print("✅ PASS: Warehouse data is fresh.")

        # --- CHECK 5: Semantic Metadata Audit (NEW!) ---
        # This confirms every product has a description for the Vector Search
        missing_desc = conn.execute("""
            SELECT COUNT(*) FROM dw.Dim_Product 
            WHERE Description IS NULL OR TRIM(Description) = ''
        """).fetchone()[0]
        
        threshold = missing_desc * 0.1

        if missing_desc > threshold:
            print(f"❌ FAIL: {missing_desc} products are missing Descriptions. Vector search will be incomplete!")
            checks_passed = False
        else:
            print("✅ PASS: 90% of products have valid descriptions for semantic search.")

    except Exception as e:
        print(f"❌ CRITICAL ERROR: Could not query warehouse. {e}")
        checks_passed = False
    finally:
        conn.close()

    return checks_passed

if __name__ == "__main__":
    if run_dq_checks():
        print("\n🚀 Warehouse is healthy.")
    else:
        print("\n⚠️ Warehouse audit failed. Please check the logs above.")