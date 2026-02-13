from analytics.data.connect_db import get_warehouse_conn
import pandas as pd

def run_health_check():
    conn = get_warehouse_conn()
    
    print("--- 📊 Warehouse Health Check ---")
    
    # 1. Row Counts
    tables = ['dw.Dim_Product', 'dw.Dim_User', 'dw.Dim_Location', 'dw.Dim_Date', 'dw.Fact_Inventory_Transactions']
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f" {table:30} | Rows: {count}")

    # 2. Check for facts that didn't join correctly
    # 0 means the referential integrity is perfect.
    orphan_query = """
    SELECT COUNT(*) 
    FROM dw.Fact_Inventory_Transactions 
    WHERE ProductKey IS NULL OR UserKey IS NULL OR LocationKey IS NULL
    """
    orphans = conn.execute(orphan_query).fetchone()[0]
    print(f"Orphaned Records: {orphans}")

    # 3. Business Preview - Top Products by Transaction Volume
    print("\n--- 📈 Top 5 Products by Activity ---")
    top_products = conn.execute("""
        SELECT Dim_Product.ProductName, COUNT(*) as TransactionCount
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_Product ON Fact_Inventory_Transactions.ProductKey = Dim_Product.ProductKey
        GROUP BY Dim_Product.ProductName
        ORDER BY 2 DESC
        LIMIT 5
    """).df()
    print(top_products)

    conn.close()

if __name__ == "__main__":
    run_health_check()