import logging
from analytics.data.connect_db import get_warehouse_conn
from analytics.etl.dimensions import dim_product, dim_user, dim_location, dim_date
from analytics.etl.facts import fact_inventory
from analytics.warehouse.create_views import create_analytics_views
from analytics.etl.data_quality import run_dq_checks, print_dq_report

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [PIPELINE] - %(message)s'
)
logger = logging.getLogger(__name__)

def run_inventory_warehouse():
    """
    Run the Inventory Data Warehouse ETL pipeline.

    Coordinates dimension and fact loads:
    - Loads dimensions (date, product, user, location).
    - Loads facts: fact skip rows already present in OLAP
    """
    try:
        duck_conn = get_warehouse_conn()
        duck_conn.execute("BEGIN TRANSACTION;")

        # 1. Load Dimensions
        logger.info("Step 1/3: Refreshing Dimensions...")
        dim_date.run_dim_date_etl(duck_conn)
        dim_product.run_dim_product_etl(duck_conn)
        dim_user.run_dim_user_etl(duck_conn)
        dim_location.run_dim_location_etl(duck_conn)

        # 1B. Dimensions Quality Check
        logger.info("Running Dimension Data Quality Checks...") 
        dim_dq_report = run_dq_checks(duck_conn, scope="dimensions") 
        print_dq_report(dim_dq_report)

        if not dim_dq_report["passed_all"]: 
            logger.error("🛑 Dimension DQ FAILED — rolling back and aborting pipeline.") 
            duck_conn.execute("ROLLBACK;") 
            return
    
        # 2. Load Facts 
        logger.info("Step 2/3: Performing Incremental Fact Load...")
        fact_inventory.run_fact_inventory_etl(duck_conn)

        # 3. Data Quality
        logger.info("Step 3/3: Running Data Quality Audit...")
        dq_report = run_dq_checks(duck_conn)
        print_dq_report(dq_report)

        if dq_report["passed_all"]:
            logger.info("✅ All checks PASS")
            duck_conn.execute("COMMIT;")
        else:
            logger.error("🛑 FAIL detected — redirecting data to staging and rolling back warehouse.")

            # staging table exists
            duck_conn.execute("""
                CREATE TABLE IF NOT EXISTS staging.Fact_Inventory_Transactions_Failed AS
                SELECT * FROM dw.Fact_Inventory_Transactions WHERE 1=0;
            """)

            # Move the bad load into staging
            duck_conn.execute("""
                INSERT INTO staging.Fact_Inventory_Transactions_Failed
                SELECT * FROM dw.Fact_Inventory_Transactions;
            """)

            # Undo warehouse changes
            duck_conn.execute("ROLLBACK;")

            logger.error("❌ Warehouse NOT updated. Data moved to staging for review.")


        # 4. Refresh Views
        create_analytics_views()

        logger.info("✅ Warehouse Refresh Completed Successfully.")
    except Exception as e:
        logger.critical(f"Pipeline failed during execution: {e}")
        raise
    finally:
        duck_conn.close()

if __name__ == "__main__":
    run_inventory_warehouse()

