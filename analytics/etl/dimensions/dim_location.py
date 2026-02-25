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
def load_dim_location(duck_conn, dim_df: pd.DataFrame):
    """
    Loads transformed data into DuckDB Dim_Location.
    Incremental SCD Type 1 upsert into dw.Dim_Location.
    - New LocationIDs are inserted.
    - Existing LocationIDs have their attributes overwritten if anything changed.
    Transaction is managed by the pipeline.
    """
    logger.info("Upserting data into dw.Dim_Location...")
    duck_conn.register("tmp_dim_location", dim_df)

    #1. SCD1
    duck_conn.execute("""
        UPDATE dw.Dim_Location
        SET
            SiteName = tmp.SiteName,
            Building = tmp.Building,
            RoomNumber = tmp.RoomNumber,
            StorageType = tmp.StorageType
        FROM tmp_dim_location AS tmp
        WHERE dw.Dim_Location.LocationID = tmp.LocationID
          AND (
              dw.Dim_Location.SiteName IS DISTINCT FROM tmp.SiteName OR
              dw.Dim_Location.Building IS DISTINCT FROM tmp.Building OR
              dw.Dim_Location.RoomNumber IS DISTINCT FROM tmp.RoomNumber OR
              dw.Dim_Location.StorageType IS DISTINCT FROM tmp.StorageType
          );
    """)

    # 2. Insert new locations
    duck_conn.execute("""
        INSERT INTO dw.Dim_Location (LocationID, SiteName, Building, RoomNumber, StorageType)
        SELECT tmp.LocationID, tmp.SiteName, tmp.Building, tmp.RoomNumber, tmp.StorageType
        FROM tmp_dim_location AS tmp
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.Dim_Location
            WHERE dw.Dim_Location.LocationID = tmp.LocationID
        );
    """)

    logger.info(f"Dim_Location upsert complete. Source rows: {len(dim_df)}.")
# -------------------------
# Orchestration
# -------------------------
def run_dim_location_etl(duck_conn):
    """Orchestrates the Location Dimension ETL.
    Accepts a shared connection from the pipeline — does NOT open its own or manage transactions."""
    try:
        raw_locations = extract_locations()
        dim_location_df = transform_dim_location(raw_locations)
        load_dim_location(duck_conn, dim_location_df)
        logger.info("✅ Dim_Location ETL completed successfully.")
    except Exception as e:
        logger.error(f"❌ Dim_Location ETL failed: {e}")
        raise


if __name__ == "__main__":
    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")
        run_dim_location_etl(duck_conn)
        duck_conn.execute("COMMIT;")

        count = duck_conn.execute("SELECT COUNT(*) FROM dw.Dim_Location").fetchone()[0]
        logger.info(f"🧪 Post-load check: {count} records in Dim_Location.")
    except Exception as e:
        duck_conn.execute("ROLLBACK;")
        logger.error(f"Standalone run failed: {e}")
    finally:
        duck_conn.close()