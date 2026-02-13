import logging
from analytics.data.connect_db import get_warehouse_conn
from analytics.etl.dimensions import dim_product, dim_user, dim_location, dim_date
from analytics.etl.facts import fact_inventory

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [PIPELINE] - %(message)s'
)
logger = logging.getLogger(__name__)

def run_inventory_warehouse():
    """
    Main entry point for the Inventory Data Warehouse ETL.
    Coordinates all dimension and fact loads.
    """
    try:
        duck_conn = get_warehouse_conn()
        try:
            # 1. Fact table pre-clenaing to allow dimension refresh without FK conflicts
            duck_conn.execute("DELETE FROM dw.Fact_Inventory_Transactions;")
        finally:
            duck_conn.close()

        # 2. Load Dimensions
        logger.info("Step 1/2: Loading Dimensions...")
        dim_date.run_dim_date_etl()
        dim_product.run_dim_product_etl()
        dim_user.run_dim_user_etl()
        dim_location.run_dim_location_etl()

        # 3. Load Facts 
        logger.info("Step 2/2: Loading Fact Tables...")
        fact_inventory.run_fact_inventory_etl()

        logger.info("✅ Full Warehouse Refresh Completed Successfully.")
    except Exception as e:
        logger.critical(f"💥 Pipeline failed during execution: {e}")
        # Here can go logic to send an email or Slack alert
        raise

if __name__ == "__main__":
    run_inventory_warehouse()