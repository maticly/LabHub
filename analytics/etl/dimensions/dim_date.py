import logging
import pandas as pd
from analytics.data.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Dim_Date] - %(message)s'
)
logger = logging.getLogger(__name__)


# 1.Extract
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
        logger.warning("No dates found in OLTP. Using default range (2026-01-31 to 2026-01-07).")
        return pd.Timestamp("2026-01-31"), pd.Timestamp("2026-01-07")
    
    logger.info(f"Date range discovered: {min_date} to {max_date}")
    return pd.to_datetime(min_date), pd.to_datetime(max_date)

# -------------------------
# 2.Transform
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
# 3.Load
# -------------------------

def load_dim_date(duck_conn, dim_date_df: pd.DataFrame):
    """
    Incremental insert into dw.Dim_Date — only adds dates not already present.
    """
    logger.info(f"Inserting new dates from {len(dim_date_df)}-row range into dw.Dim_Date...")
    duck_conn.register("tmp_dim_date", dim_date_df)
    duck_conn.execute("""
        INSERT INTO dw.Dim_Date (DateKey, FullDate, Day, Month, MonthName, Quarter, Year, DayOfWeek)
        SELECT
            tmp_dim_date.DateKey,
            tmp_dim_date.FullDate,
            tmp_dim_date.Day,
            tmp_dim_date.Month,
            tmp_dim_date.MonthName,
            tmp_dim_date.Quarter,
            tmp_dim_date.Year,
            tmp_dim_date.DayOfWeek
        FROM tmp_dim_date
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.Dim_Date
            WHERE dw.Dim_Date.DateKey = tmp_dim_date.DateKey
        );
    """)
    logger.info("Dim_Date incremental load completed successfully.")

# -------------------------
# Orchestration
# -------------------------

def run_dim_date_etl(duck_conn):
    try:
        min_d, max_d = extract_date_range()
        dim_date_df = transform_dim_date(min_d, max_d)
        load_dim_date(duck_conn, dim_date_df)
        logger.info("✅ Dim_Date ETL completed successfully.")
    except Exception as e:
        logger.error(f"Dim_Date ETL process failed: {e}")
        raise

if __name__ == "__main__":
    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")
        run_dim_date_etl(duck_conn)
        duck_conn.execute("COMMIT;")

        count = duck_conn.execute("SELECT COUNT(*) FROM dw.Dim_Date").fetchone()[0]
        logger.info(f"🧪 Post-load check: {count} records in Dim_Date.")
    except Exception as e:
        duck_conn.execute("ROLLBACK;")
        logger.error(f"Standalone run failed: {e}")
    finally:
        duck_conn.close()