import logging
import pandas as pd
from pathlib import Path
from analytics.data.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Dim_Product] - %(message)s'
)
logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1] 
CSV_DESCRIPTION_PATH = PROJECT_ROOT / "data" / "generated_data_OLTP" / "core.Product_with_Descriptions.csv"

# 1. EXTRACT
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
        
def extract_descriptions() -> pd.DataFrame | None:
    """
    Loads product descriptions from CSV.
    Returns a DataFrame with ProductID and Description columns, or None if unavailable.
    """
    if not CSV_DESCRIPTION_PATH.exists():
        logger.warning(f"Descriptions CSV not found at {CSV_DESCRIPTION_PATH}. Description column will be NULL.")
        return None
    try:
        df_desc = pd.read_csv(CSV_DESCRIPTION_PATH, usecols=["ProductID", "Description"])
        logger.info(f"Successfully loaded {len(df_desc)} descriptions from CSV.")
        return df_desc
    except Exception as e:
        logger.warning(f"Failed to load descriptions CSV: {e}. Description column will be NULL.")
        return None

# 2.Transform
# -------------------------
def transform_dim_product(df_products: pd.DataFrame, df_descriptions: pd.DataFrame | None) -> pd.DataFrame:
    """
    Shapes data for the Warehouse, merging in AI-generated descriptions where available.
    """
    logger.info("Transforming product data...")
    dim_product_df = df_products.copy()
    dim_product_df['ProductName'] = dim_product_df['ProductName'].str.strip()

    if df_descriptions is not None:
        dim_product_df = dim_product_df.merge(df_descriptions, on="ProductID", how="left")
        logger.info("Descriptions merged into product data.")
    else:
        dim_product_df['Description'] = None

    dim_product_df = dim_product_df[["ProductID", "ProductName", "CategoryName", "UnitOfMeasure", "Description"]]
    return dim_product_df


# 3. Load
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
            INSERT INTO dw.Dim_Product (ProductID, ProductName, CategoryName, UnitOfMeasure, Description)
            SELECT ProductID, ProductName, CategoryName, UnitOfMeasure, Description
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


# 4. Orchestration
# -------------------------
def run_dim_product_etl():
    """Orchestrates the Product Dimension ETL."""
    try:
        raw_products = extract_products()
        df_descriptions = extract_descriptions()
        dim_product_df = transform_dim_product(raw_products, df_descriptions)
        load_dim_product(dim_product_df)
        logger.info("Dim_Product ETL completed successfully.")
    except Exception as e:
        logger.error(f"Dim_Product ETL failed: {e}")

# 5. Self-Testing Block ---
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
        desc_count = duck_conn.execute("SELECT COUNT(*) FROM dw.Dim_Product WHERE Description IS NOT NULL").fetchone()[0]
        logger.info(f"{desc_count} products have descriptions populated.")
    finally:
        duck_conn.close()

if __name__ == "__main__":
    run_dim_product_etl()
    test_product_load()