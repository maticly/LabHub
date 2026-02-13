import logging
import pandas as pd
from analytics.data.connect_db import get_oltp_connection, get_warehouse_conn

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Dim_User] - %(message)s'
)
logger = logging.getLogger(__name__)

# -------------------------
# Extract
# -------------------------
def extract_users():
    """
    Pulls user data from the OLTP database, joining Role and Department.
    """
    logger.info("Extracting user data from OLTP...")
    query = """
        SELECT
            [User].UserID,
            [User].FirstName + ' ' + [User].LastName AS UserName,
            UserRole.UserRoleName AS UserRole,
            Department.DepartmentName
        FROM core.[User]
        JOIN core.UserRole ON [User].UserRoleID = UserRole.UserRoleID
        JOIN core.Department ON [User].DepartmentID = Department.DepartmentID;
    """
    conn = get_oltp_connection()
    try:
        df_users = pd.read_sql(query, conn)
        logger.info(f"Successfully extracted {len(df_users)} users.")
        return df_users
    except Exception as e:
        logger.error(f"Failed to extract users: {e}")
        raise
    finally:
        conn.close()

# -------------------------
# Transform
# -------------------------
def transform_dim_user(df_users: pd.DataFrame) -> pd.DataFrame:
    """
    Shapes OLTP user data into Dim_User rows.
    """
    logger.info("Transforming user data...")
    dim_user_df = df_users.copy()
    
    # Cleans whitespace and ensures proper casing if needed
    dim_user_df['UserName'] = dim_user_df['UserName'].str.strip()
    
    # Enforces schema column order
    dim_user_df = dim_user_df[["UserID", "UserName", "UserRole", "DepartmentName"]]
    return dim_user_df

# -------------------------
# Load
# -------------------------
def load_dim_user(dim_df: pd.DataFrame):
    """
    Loads transformed data into DuckDB Dim_User.
    """
    logger.info("Loading data into DuckDB dw.Dim_User...")
    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")
        duck_conn.execute("DELETE FROM dw.Dim_User;") 
        duck_conn.register("tmp_dim_user", dim_df)
        duck_conn.execute("""
            INSERT INTO dw.Dim_User (UserID, UserName, UserRole, DepartmentName)
            SELECT UserID, UserName, UserRole, DepartmentName
            FROM tmp_dim_user;
        """)
        duck_conn.execute("COMMIT;")
        logger.info(f"Load complete. {len(dim_df)} rows inserted.")
    except Exception as e:
        if duck_conn:
            duck_conn.execute("ROLLBACK;")
        logger.error(f"Load failed for Dim_User: {e}")
        raise
    finally: 
        duck_conn.close()

# -------------------------
# Orchestration
# -------------------------
def run_dim_user_etl():
    """Main function to run the User Dimension ETL."""
    try:
        raw_users = extract_users()
        dim_user_df = transform_dim_user(raw_users)
        load_dim_user(dim_user_df)
        logger.info("✅ Dim_User ETL completed successfully.")
    except Exception as e:
        logger.error(f"❌ Dim_User ETL failed: {e}")

# --- Self-Testing Block ---
def test_user_load():
    """Verify data exists in the warehouse."""
    logger.info("🧪 Running Post-Load Test for Dim_User...")
    duck_conn = get_warehouse_conn()
    try:
        count = duck_conn.execute("SELECT COUNT(*) FROM dw.Dim_User").fetchone()[0]
        if count > 0:
            logger.info(f"✅ Test Passed: {count} records found in Dim_User.")
        else:
            logger.warning("❌ Test Failed: Dim_User is empty.")
    finally:
        duck_conn.close()

if __name__ == "__main__":
    run_dim_user_etl()
    test_user_load()