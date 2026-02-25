import logging
import pandas as pd
from analytics.data.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Fact_Inventory] - %(message)s'
)
logger = logging.getLogger(__name__)


# 1.Extract
# -------------------------

def extract_fact_source_data():
    """
    Pulls raw events and current item states from OLTP.
    Joins StockEvent with InventoryItem to resolve the ProductID.
    """
    logger.info("Extracting raw inventory events from OLTP...")
    query = """
        SELECT
            StockEvent.StockEventID AS TransactionID,
            InventoryItem.ProductID,
            StockEvent.LocationID,
            StockEvent.[UserID],
            StockEvent.EventDate,
            StockEvent.OldQuantity,
            StockEvent.NewQuantity,
            StockEvent.EventType,
            InventoryItem.Quantity AS CurrentStockSnapshot
        FROM inventory.StockEvent
        LEFT JOIN inventory.InventoryItem ON StockEvent.InventoryItemID = InventoryItem.InventoryItemID
    """
    conn = get_oltp_connection()
    try:
        df = pd.read_sql(query, conn)
        logger.info(f"Successfully extracted {len(df)} inventory events.")
        return df
    except Exception as e:
        logger.error(f"Failed to extract fact data: {e}")
        raise
    finally:
        conn.close()

#3. Load
def load_fact_inventory(df_fact_inventory: pd.DataFrame):
    """
    Performs dimension lookups and incrementally inserts only new rows
    into Fact_Inventory_Transactions, skipping any TransactionID that
    already exists.
    """
    logger.info("Performing incremental load into Fact_Inventory_Transactions...")
    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")
        duck_conn.register("tmp_fact_inventory", df_fact_inventory)
        
        # Count existing rows before insert for accurate delta reporting
        before_count = duck_conn.execute(
            "SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions"
        ).fetchone()[0]

        # Register the raw data
        duck_conn.register("tmp_fact_inventory", df_fact_inventory)

        duck_conn.execute("""
            INSERT INTO dw.Fact_Inventory_Transactions (
                TransactionID, DateKey, ProductKey, LocationKey, UserKey,
                QuantityDelta, AbsoluteQuantity, CurrentStockSnapshot, EventType
            )
            SELECT 
                tmp_fact_inventory.TransactionID,
                Dim_Date.DateKey,
                Dim_Product.ProductKey,
                Dim_Location.LocationKey,
                Dim_User.UserKey,
                (tmp_fact_inventory.NewQuantity - tmp_fact_inventory.OldQuantity) AS QuantityDelta,
                tmp_fact_inventory.NewQuantity AS AbsoluteQuantity,
                tmp_fact_inventory.CurrentStockSnapshot,
                tmp_fact_inventory.EventType
            FROM tmp_fact_inventory
            JOIN dw.Dim_Date ON CAST(strftime(tmp_fact_inventory.EventDate, '%Y%m%d') AS INT) = dw.Dim_Date.DateKey
            JOIN dw.Dim_Product ON tmp_fact_inventory.ProductID = dw.Dim_Product.ProductID
            JOIN dw.Dim_Location ON tmp_fact_inventory.LocationID = dw.Dim_Location.LocationID
            JOIN dw.Dim_User ON tmp_fact_inventory.UserID = dw.Dim_User.UserID;
            WHERE NOT EXISTS (
                SELECT 1
                FROM dw.Fact_Inventory_Transactions f
                WHERE f.TransactionID = src.TransactionID
        """)
        
        duck_conn.execute("COMMIT;")
        
        after_count = duck_conn.execute(
            "SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions"
        ).fetchone()[0]
        new_rows = after_count - before_count
        logger.info(
            f"✅ Incremental load complete. "
            f"{new_rows} new rows inserted out of {len(df_fact_inventory)} source events."
        )
        
    except Exception as e:
        if duck_conn:
            duck_conn.execute("ROLLBACK;")
        logger.error(f"❌ Fact Table Load Failed: {e}")
        raise
    finally:
        duck_conn.close()

# -------------------------
# Orchestration
# -------------------------

def run_fact_inventory_etl():
    """Main entry point for Fact Table ETL."""
    try:
        df_source = extract_fact_source_data()
        if not df_source.empty:
            load_fact_inventory(df_source)
        else:
            logger.warning("No source data found to load into Fact table.")
    except Exception as e:
        logger.error(f"Fact Inventory ETL aborted: {e}")

if __name__ == "__main__":
    run_fact_inventory_etl()