import logging
import pandas as pd
from analytics.data.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Dim_Product] - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_products():
    """
    Extracts raw product data from SQL Server.
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
        logger.info(f"Successfully extracted {len(df_products)} products.")
        return df_products
    except Exception as e:
        logger.error(f"Failed to extract products: {e}")
        raise
    finally:
        conn.close()

# -------------------------
# Transform
# -------------------------
def transform_dim_product(df_products: pd.DataFrame) -> pd.DataFrame:
    """
    Shapes data for the Warehouse.
    """
    logger.info("Transforming product data...")
    dim_product_df = df_products.copy()

    dim_product_df['ProductName'] = df_products['ProductName'].str.strip()
    dim_product_df = dim_product_df[["ProductID", "ProductName", "CategoryName", "UnitOfMeasure"]]
    return dim_product_df

# -------------------------
# Load
# -------------------------
def load_dim_product(dim_df: pd.DataFrame):
    """
    Loads transformed data into DuckDB.
    """
    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")
        duck_conn.execute("DELETE FROM dw.Dim_Product;") 
        duck_conn.register("tmp_dim_product", dim_df)
        duck_conn.execute("""
            INSERT INTO dw.Dim_Product (ProductID, ProductName, CategoryName, UnitOfMeasure)
            SELECT ProductID, ProductName, CategoryName, UnitOfMeasure
            FROM tmp_dim_product;
        """)
        duck_conn.execute("COMMIT;")
        logger.info(f"Load complete. {len(dim_df)} rows inserted.")
    except Exception as e:
        duck_conn.execute("ROLLBACK;")
        logger.error(f"Load failed: {e}")
        raise
    finally:
        duck_conn.close()

# -------------------------
# Orchestration
# -------------------------
def run_dim_product_etl():
    """Orchestrates the Product Dimension ETL."""
    try:
        raw_products = extract_products()
        dim_product_df = transform_dim_product(raw_products)
        load_dim_product(dim_product_df)
        logger.info("Dim_Product ETL completed successfully.")
    except Exception as e:
        logger.error(f"Dim_Product ETL failed: {e}")

# --- Self-Testing Block ---
def test_product_load():
    """Verify data exists in the warehouse."""
    logger.info("🧪 Running Post-Load Test...")
    duck_conn = get_warehouse_conn()
    try:
        count = duck_conn.execute("SELECT COUNT(*) FROM dw.Dim_Product").fetchone()[0]
        if count > 0:
            logger.info(f"✅ Test Passed: {count} records found in Dim_Product.")
        else:
            logger.warning("❌ Test Failed: Dim_Product is empty.")
    finally:
        duck_conn.close()

if __name__ == "__main__":
    run_dim_product_etl()
    test_product_load()