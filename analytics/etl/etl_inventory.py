"""
THIS IS THE ETL FILE FOR THE INVENTORY DOMAIN.

Extracts from OLTP (SQL Server),
transforms into Dim and Fact,
loads into DuckDB warehouse.
"""

import os
from pathlib import Path
import duckdb
import pyodbc
import pandas as pd
from analytics.data.connect_db import get_connection as get_oltp_connection

# -------------------------
# Config
# -------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE_DB = PROJECT_ROOT / "warehouse" / "warehouse.duckdb"

# -------------------------
# Extract
# -------------------------

#Dim_product
def extract_products():
    """
    Pulls product data from the OLTP database, joining to get category and unit names.
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
    finally:
        conn.close()
    return df_products

#Dim_user
def extract_user():
    """
    Pulls user data from the OLTP database.
    """
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
    finally:
        conn.close()
    return df_users

#Dim_location
def extract_location():
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
    finally:
        conn.close()
    return df_locations

#Dim_Date
def extract_data_range():
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
        print(f"❌ Error querying date range: {e}")
    finally:
        conn.close()

        if pd.isna(min_date) or pd.isna(max_date):
            print("⚠️ No dates found in inventory.StockEvent. Using default date range.")
            return pd.Timestamp("2025-01-01"), pd.Timestamp("2026-01-30")
    
        return pd.to_datetime(min_date), pd.to_datetime(max_date)

def extract_fact_source_data():
    """
    Pulls raw events and current item states from OLTP.
    """
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
        df_fact_source_data = pd.read_sql(query, conn)
    finally:
        conn.close()
    return df_fact_source_data
# -------------------------
# Transform
# -------------------------
def transform_dim_product(df_products: pd.DataFrame) -> pd.DataFrame:
    """
    Shapes OLTP product data into Dim_Product rows.
    For now select/rename columns and enforce order.
    Later we add deduping, SCD logic, etc.
    """
    dim_product_df = df_products.copy()

    dim_product_df = dim_product_df[["ProductID", "ProductName", "CategoryName", "UnitOfMeasure"]]
    return dim_product_df

def transform_dim_user(df_users: pd.DataFrame) -> pd.DataFrame:
    """
    Shapes OLTP user data into Dim_User rows.
    """
    dim_user_df = df_users.copy()

    dim_user_df = dim_user_df[["UserID", "UserName", "UserRole", "DepartmentName"]]
    return dim_user_df

def transform_dim_locations(df_locations: pd.DataFrame) -> pd.DataFrame:
    """
    Shapes OLTP location data into Dim_Location rows.
    """
    dim_location_df = df_locations.copy()
    dim_location_df = dim_location_df[["LocationID", "SiteName", "Building", "RoomNumber", "StorageType"]]
    return dim_location_df

#Dim_Date
# No date transformation - it is generated from a date range.

# -------------------------
# Load
# -------------------------
def load_dim_product(dim_df: pd.DataFrame):
    """
    Simple full reload of dw.Dim_Product.
    Later i'll make it incremental.
    """
    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")
        duck_conn.execute("DELETE FROM dw.Dim_Product;")  # simple full reload for now
        duck_conn.register("tmp_dim_product", dim_df) # register pandas DataFrame as a DuckDB table
        duck_conn.execute("""
            INSERT INTO dw.Dim_Product (ProductID, ProductName, CategoryName, UnitOfMeasure)
            SELECT ProductID, ProductName, CategoryName, UnitOfMeasure
            FROM tmp_dim_product;
        """)
        duck_conn.execute("COMMIT;")
        print(f"✅ Loaded {len(dim_df)} rows into dw.Dim_Product")
    except Exception as e:
        duck_conn.execute("ROLLBACK;")
        print("❌ Failed loading dw.Dim_Product:", e)
        raise
    finally: 
        duck_conn.close()

#Dim_user
def load_dim_user(dim_df: pd.DataFrame):
    """
    Simple full reload of dw.Dim_User.
    Later i'll make it incremental.
    """
    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")
        duck_conn.execute("DELETE FROM dw.Dim_User;")  # simple full reload for now
        duck_conn.register("tmp_dim_user", dim_df) # register pandas DataFrame as a DuckDB table
        duck_conn.execute("""
            INSERT INTO dw.Dim_User (UserID, UserName, UserRole, DepartmentName)
            SELECT UserID, UserName, UserRole, DepartmentName
            FROM tmp_dim_user;
        """)
        duck_conn.execute("COMMIT;")
        print(f"✅ Loaded {len(dim_df)} rows into dw.Dim_User")
    except Exception as e:
        duck_conn.execute("ROLLBACK;")
        print("❌ Failed loading dw.Dim_User:", e)
        raise
    finally: 
        duck_conn.close()

#Dim_locations
def load_dim_locations(dim_df: pd.DataFrame):
    """
    Simple full reload of dw.Dim_Locations.
    Later i'll make it incremental.
    """
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
        print(f"✅ Loaded {len(dim_df)} rows into dw.Dim_Location")
    except Exception as e:
        duck_conn.execute("ROLLBACK;")
        print("❌ Failed loading dw.Dim_Location:", e)
        raise
    finally: 
        duck_conn.close()

#Dim_date
def load_dim_date():
    """
    Automatically aligns Dim_Date with the OLTP transaction range.
    """
    min_date, max_date = extract_data_range()
    print(f"📅 Generating Date Dimension from {min_date.date()} to {max_date.date()}")

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
    
    #load into warehouse
    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")
        duck_conn.execute("DELETE FROM dw.Dim_Date;")  # simple full reload for now
        duck_conn.register("tmp_dim_date", dim_date_df) # register pandas DataFrame as a DuckDB table
        duck_conn.execute("""
            INSERT INTO dw.Dim_Date (DateKey, FullDate, Day, Month, MonthName, Quarter, Year, DayOfWeek)
            SELECT DateKey, FullDate, Day, Month, MonthName, Quarter, Year, DayOfWeek
            FROM tmp_dim_date;
        """)
        duck_conn.execute("COMMIT;")
        print(f"✅ Loaded {len(dim_date_df)} rows into dw.Dim_Date")
    except Exception as e:
        duck_conn.execute("ROLLBACK;")
        print("❌ Failed loading dw.Dim_Date:", e)
        raise
    finally: 
        duck_conn.close()

#fact_inventory_transactions
def load_fact_inventory(df_fact_inventory: pd.DataFrame):
    """
    Performs lookups against Warehouse Dimensions and calculates metrics.
    """
    duck_conn = get_warehouse_conn()
    try:
        duck_conn.execute("BEGIN TRANSACTION;")
        duck_conn.execute("DELETE FROM dw.Fact_Inventory_Transactions;")
        
        # Register the raw data
        duck_conn.register("tmp_fact_inventory", df_fact_inventory)

        # Transformation + Lookup + Load
        # QuantityDelta (New - Old) and use NewQuantity as AbsoluteQuantity
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
            JOIN dw.Dim_Date ON CAST(strftime(tmp_fact_inventory.EventDate, '%Y%m%d') AS INT) = Dim_Date.DateKey
            JOIN dw.Dim_Product ON tmp_fact_inventory.ProductID = Dim_Product.ProductID
            JOIN dw.Dim_Location ON tmp_fact_inventory.LocationID = Dim_Location.LocationID
            JOIN dw.Dim_User ON tmp_fact_inventory.UserID = Dim_User.UserID;
        """)
        
        duck_conn.execute("COMMIT;")
        print(f"✅ Loaded {len(df_fact_inventory)} rows into Fact_Inventory_Transactions")
    except Exception as e:
        duck_conn.execute("ROLLBACK;")
        print(f"❌ Fact Table Load Failed: {e}")
        raise
    finally:
        duck_conn.close()

# -------------------------
# Orchestration
# -------------------------
def run_inventory_etl():
    print("Starting Inventory ETL Pipeline")
    
    # PRODUCT DIMENSION
    products_df = extract_products()
    dim_product_df = transform_dim_product(products_df)
    load_dim_product(dim_product_df)

    # USER DIMENSION
    users_df = extract_user()
    dim_user_df = transform_dim_user(users_df)
    load_dim_user(dim_user_df)

    # USER DIMENSION
    locations_df = extract_location()
    dim_locations_df = transform_dim_locations(locations_df)
    load_dim_locations(dim_locations_df)

    # DATE DIMENSION
    load_dim_date()

    # FACT TABLE
    fact_source_data_df = extract_fact_source_data()
    load_fact_inventory(fact_source_data_df)

    print("✅ ETL run completed successfully.")


if __name__ == "__main__":
    run_inventory_etl()