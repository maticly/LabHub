import logging
import pandas as pd
from analytics.data.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Dim_Location] - %(message)s'
)
logger = logging.getLogger(__name__)

# -------------------------
# Extract
# -------------------------
def extract_locations():
    """
    Pulls location data from the OLTP database.
    Joins Site, Building, and Room to create a flattened location record.
    """
    query = """
        SELECT
            Location.LocationID,
            Location.SiteName,
            Location.Building,
            Location.RoomNumber,
            Location.StorageType
        FROM inventory.Location
    """
    conn = get_oltp_connection()
    try:
        df_locations = pd.read_sql(query, conn)
        logger.info(f"Successfully extracted {len(df_locations)} locations.")
        return df_locations
    except Exception as e:
        logger.error(f"Failed to extract locations: {e}")
        raise
    finally:
        conn.close()

# -------------------------
# Transform
# -------------------------
def transform_dim_location(df_locations: pd.DataFrame) -> pd.DataFrame:
    """
    Shapes OLTP location data into Dim_Location rows.
    """
    logger.info("Transforming location data...")
    dim_location_df = df_locations.copy()

    # Ensures SiteName and Building are capitalized
    dim_location_df['SiteName'] = dim_location_df['SiteName'].str.strip().str.title()
    dim_location_df['Building'] = dim_location_df['Building'].str.strip().str.title()

    dim_location_df = dim_location_df[["LocationID", "SiteName", "Building", "RoomNumber", "StorageType"]]
    return dim_location_df

# -------------------------
# Load
# -------------------------
def load_dim_location(dim_df: pd.DataFrame):
    """
    Loads transformed data into DuckDB Dim_Location.
    """
    logger.info("Loading data into DuckDB dw.Dim_Location...")
    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")
        duck_conn.execute("DELETE FROM dw.Dim_Location;")  # simple full reload for now
        duck_conn.register("tmp_dim_location", dim_df) # register pandas DataFrame as a DuckDB table
        duck_conn.execute("""
            INSERT INTO dw.Dim_Location (LocationID, SiteName, Building, RoomNumber, StorageType)
            SELECT LocationID, SiteName, Building, RoomNumber, StorageType
            FROM tmp_dim_location;
        """)
        duck_conn.execute("COMMIT;")
        logger.info(f"Load complete. {len(dim_df)} rows inserted.")
    except Exception as e:
        if duck_conn:
            duck_conn.execute("ROLLBACK;")
        logger.error(f"Load failed for Dim_Location: {e}")
        raise
    finally: 
        duck_conn.close()

# -------------------------
# Orchestration
# -------------------------
def run_dim_location_etl():
    """Main function to run the Location Dimension ETL."""
    try:
        raw_locations = extract_locations()
        dim_location_df = transform_dim_location(raw_locations)
        load_dim_location(dim_location_df)
        logger.info("✅ Dim_Location ETL completed successfully.")
    except Exception as e:
        logger.error(f"❌ Dim_Location ETL failed: {e}")

# --- Self-Testing Block ---
def test_location_load():
    """Verify data exists in the warehouse."""
    logger.info("🧪 Running Post-Load Test for Dim_Location...")
    duck_conn = get_warehouse_conn()
    try:
        count = duck_conn.execute("SELECT COUNT(*) FROM dw.Dim_Location").fetchone()[0]
        if count > 0:
            logger.info(f"✅ Test Passed: {count} records found in Dim_Location.")
        else:
            logger.warning("❌ Test Failed: Dim_Location is empty.")
    finally:
        duck_conn.close()

if __name__ == "__main__":
    run_dim_location_etl()
    test_location_load()