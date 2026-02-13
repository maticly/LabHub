import logging
import pandas as pd
from analytics.data.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Dim_Date] - %(message)s'
)
logger = logging.getLogger(__name__)

# -------------------------
# Extract (Discovery)
# -------------------------
def extract_date_range():
    """
    Queries OLTP to find the actual date boundaries of the business data.
    """
    query = """
        SELECT 
            MIN(EventDate) AS MinDate,
            MAX(EventDate) AS MaxDate 
        FROM inventory.StockEvent;
    """
    conn = get_oltp_connection()
    min_date = None
    max_date = None

    try:
        df_dates = pd.read_sql(query, conn)
        min_date = df_dates['MinDate'].iloc[0]
        max_date = df_dates['MaxDate'].iloc[0]
    except Exception as e:
        logger.error(f"Error querying date range: {e}")
    finally:
        conn.close()

    # Fallback logic if table is empty or query fails
    if pd.isna(min_date) or pd.isna(max_date):
        logger.warning("No dates found in OLTP. Using default range (2025-01-01 to 2026-12-31).")
        return pd.Timestamp("2025-01-01"), pd.Timestamp("2026-12-31")
    
    logger.info(f"Date range discovered: {min_date} to {max_date}")
    return pd.to_datetime(min_date), pd.to_datetime(max_date)

# -------------------------
# Transform
# -------------------------
def transform_dim_date(min_date, max_date) -> pd.DataFrame:
    """
    Generates a continuous sequence of dates and calculates attributes.
    """
    logger.info(f"Generating date attributes for range: {min_date.date()} to {max_date.date()}")

    #generate date range
    date_range = pd.date_range(start=min_date, end=max_date, freq='D')
    dim_date_df = pd.DataFrame({"FullDate": date_range})

    #transformt to Dim_Date structure
    dim_date_df['DateKey'] = dim_date_df['FullDate'].dt.strftime('%Y%m%d').astype(int)
    dim_date_df['Day'] = dim_date_df['FullDate'].dt.day
    dim_date_df['Month'] = dim_date_df['FullDate'].dt.month
    dim_date_df['MonthName'] = dim_date_df['FullDate'].dt.month_name()
    dim_date_df['Quarter'] = dim_date_df['FullDate'].dt.quarter
    dim_date_df['Year'] = dim_date_df['FullDate'].dt.year
    dim_date_df['DayOfWeek'] = dim_date_df['FullDate'].dt.day_name()

    return dim_date_df

# -------------------------
# Load
# -------------------------

def load_dim_date(dim_date_df: pd.DataFrame):
    """
    Full reload of dw.Dim_Date.
    """
    logger.info(f"Loading {len(dim_date_df)} days into dw.Dim_Date...")
    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")
        duck_conn.execute("DELETE FROM dw.Dim_Date;")
        duck_conn.register("tmp_dim_date", dim_date_df)
        duck_conn.execute("""
            INSERT INTO dw.Dim_Date (DateKey, FullDate, Day, Month, MonthName, Quarter, Year, DayOfWeek)
            SELECT DateKey, FullDate, Day, Month, MonthName, Quarter, Year, DayOfWeek
            FROM tmp_dim_date;
        """)
        duck_conn.execute("COMMIT;")
        logger.info("Dim_Date load completed successfully.")
    except Exception as e:
        duck_conn.execute("ROLLBACK;")
        logger.error(f"Failed to load Dim_Date: {e}")
        raise
    finally:
        duck_conn.close()

# -------------------------
# Orchestration
# -------------------------
def run_dim_date_etl():
    """Main entry point for Date Dimension ETL."""
    try:
        min_d, max_d = extract_date_range()
        dim_date_df = transform_dim_date(min_d, max_d)
        load_dim_date(dim_date_df)
    except Exception as e:
        logger.error(f"Dim_Date ETL process failed: {e}")

if __name__ == "__main__":
    run_dim_date_etl()