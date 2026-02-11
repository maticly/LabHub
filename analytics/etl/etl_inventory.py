"""
THIS IS THE ETL FILE FOR THE INVENTORY DOMAIN.

Extracts from OLTP (SQL Server),
transforms into Dim_Product shape,
loads into DuckDB warehouse.
"""

import os
from pathlib import Path

import duckdb
import pyodbc
import pandas as pd
from analytics.data.connect_db import get_connection as get_oltp_connection

# -------------------------
# Config
# -------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE_DB = PROJECT_ROOT / "warehouse" / "warehouse.duckdb"

# -------------------------
# Connections
# -------------------------
def get_warehouse_conn():
    return duckdb.connect(str(WAREHOUSE_DB))


def get_duckdb_conn():
    return duckdb.connect(str(WAREHOUSE_DB))

# -------------------------
# Extract
# -------------------------
def extract_products():
    """
    Pulls product data from the OLTP database, joining to get category and unit names.
    Adjusts table names to match the warehouse schema.
    """
    query = """
        SELECT
            Product.ProductID,
            Product.ProductName,
            ProductCategory.CategoryName,
            UnitOfMeasure.UnitName AS UnitOfMeasure
        FROM core.Product
        JOIN core.ProductCategory ON Product.ProductCategoryID = ProductCategory.CategoryID
        JOIN core.UnitOfMeasure ON Product.UnitID = UnitOfMeasure.UnitID;
    """
    conn = get_oltp_connection()
    try:
        df_products = pd.read_sql(query, conn)
    finally:
        conn.close()
    return df_products


# -------------------------
# Transform
# -------------------------
def transform_dim_product(df_products: pd.DataFrame) -> pd.DataFrame:
    """
    Shapes OLTP product data into Dim_Product rows.
    For now select/rename columns and enforce order.
    Later we add deduping, SCD logic, etc.
    """
    dim_df = df_products.copy()

    dim_df = dim_df[["ProductID", "ProductName", "CategoryName", "UnitOfMeasure"]]
    return dim_df


# -------------------------
# Load
# -------------------------
def load_dim_product(dim_df: pd.DataFrame):
    """
    Simple full reload of dw.Dim_Product.
    Later i'll do it incremental.
    """
    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")
        duck_conn.execute("DELETE FROM dw.Dim_Product;")  # simple full reload for now
        duck_conn.register("tmp_dim_product", dim_df) # register pandas DataFrame as a DuckDB table
        duck_conn.execute("""
            INSERT INTO dw.Dim_Product (ProductID, ProductName, CategoryName, UnitOfMeasure)
            SELECT ProductID, ProductName, CategoryName, UnitOfMeasure
            FROM tmp_dim_product;
        """)
        duck_conn.execute("COMMIT;")
        print("✅ Loaded dw.Dim_Product")
    except Exception as e:
        duck_conn.execute("ROLLBACK;")
        print("❌ Failed loading dw.Dim_Product:", e)
        raise
    finally: 
        duck_conn.close()


# -------------------------
# Orchestration
# -------------------------
def run_etl_dim_product():
    print("ETL: Dim_Product")
    
    # 1) Extract
    products_df = extract_products()
    print(f"Extracted {len(products_df)} products from OLTP")

    # 2) Transform
    dim_product_df = transform_dim_product(products_df)
    print(f"Transformed into {len(dim_product_df)} Dim_Product rows")

    # 3) Load
    load_dim_product(dim_product_df)

    print("✅ ETL run completed successfully.")


if __name__ == "__main__":
    run_etl_dim_product()