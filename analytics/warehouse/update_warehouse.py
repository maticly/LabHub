from multiprocessing.dummy import connection
import duckdb
from analytics.data.connect_db import get_warehouse_conn

conn = get_warehouse_conn()

conn.execute("""
    ALTER TABLE dw.Dim_Product
    ADD COLUMN IF NOT EXISTS Description TEXT;
    """)
print("✅ Description column added to Dim_Product (if it didn't exist).")

print(conn.execute("DESCRIBE dw.Dim_Product").fetchdf())