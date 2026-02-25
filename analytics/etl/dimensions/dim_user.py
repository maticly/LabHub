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
def load_dim_user(duck_conn, dim_df: pd.DataFrame):
    """
    Incremental SCD Type 1 upsert into dw.Dim_User.
    """
    logger.info("Upserting data into dw.Dim_User...")
    duck_conn.register("tmp_dim_user", dim_df)

    # Step 1: Update changed attributes on existing rows
    duck_conn.execute("""
        UPDATE dw.Dim_User
        SET
            UserName       = tmp_dim_user.UserName,
            UserRole       = tmp_dim_user.UserRole,
            DepartmentName = tmp_dim_user.DepartmentName
        FROM tmp_dim_user
        WHERE dw.Dim_User.UserID = tmp_dim_user.UserID
          AND (
              dw.Dim_User.UserName       IS DISTINCT FROM tmp_dim_user.UserName       OR
              dw.Dim_User.UserRole       IS DISTINCT FROM tmp_dim_user.UserRole       OR
              dw.Dim_User.DepartmentName IS DISTINCT FROM tmp_dim_user.DepartmentName
          );
    """)

    # Step 2: Insert new users
    duck_conn.execute("""
        INSERT INTO dw.Dim_User (UserID, UserName, UserRole, DepartmentName)
        SELECT
            tmp_dim_user.UserID,
            tmp_dim_user.UserName,
            tmp_dim_user.UserRole,
            tmp_dim_user.DepartmentName
        FROM tmp_dim_user
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.Dim_User
            WHERE dw.Dim_User.UserID = tmp_dim_user.UserID
        );
    """)

    logger.info(f"Dim_User upsert complete. Source rows: {len(dim_df)}.")


# 4. ORCHESTRATION
def run_dim_user_etl(duck_conn):
    """
    Orchestrates the User Dimension ETL.
    Accepts a shared connection — does NOT open its own or manage transactions.
    """
    try:
        raw_users = extract_users()
        dim_user_df = transform_dim_user(raw_users)
        load_dim_user(duck_conn, dim_user_df)
        logger.info("✅ Dim_User ETL completed successfully.")
    except Exception as e:
        logger.error(f"❌ Dim_User ETL failed: {e}")
        raise


# --- Standalone entry point ---
if __name__ == "__main__":
    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")
        run_dim_user_etl(duck_conn)
        duck_conn.execute("COMMIT;")

        count = duck_conn.execute("SELECT COUNT(*) FROM dw.Dim_User").fetchone()[0]
        logger.info(f"🧪 Post-load check: {count} records in Dim_User.")
    except Exception as e:
        duck_conn.execute("ROLLBACK;")
        logger.error(f"Standalone run failed: {e}")
    finally:
        duck_conn.close()