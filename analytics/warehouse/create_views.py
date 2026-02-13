# --Current inventory--
import duckdb
from analytics.data.connect_db import get_warehouse_conn

def create_analytics_views():
    conn = get_warehouse_conn()
    
    # --- 1. Current Stock on Hand ---
    conn.execute("""
    CREATE OR REPLACE VIEW dw.v_current_inventory AS
    SELECT 
        Dim_Product.ProductName,
        Dim_Product.CategoryName,
        Dim_Location.SiteName,
        Dim_Location.Building,
        Dim_Location.RoomNumber,
        SUM(Fact_Inventory_Transactions.QuantityDelta) AS StockOnHand,
        Dim_Product.UnitOfMeasure
    FROM dw.Fact_Inventory_Transactions
    JOIN dw.Dim_Product ON Fact_Inventory_Transactions.ProductKey = Dim_Product.ProductKey
    JOIN dw.Dim_Location ON Fact_Inventory_Transactions.LocationKey = Dim_Location.LocationKey
    GROUP BY
        Dim_Product.ProductName, 
        Dim_Product.CategoryName, 
        Dim_Location.SiteName, 
        Dim_Location.Building, 
        Dim_Location.RoomNumber, 
        Dim_Product.UnitOfMeasure
    HAVING SUM(Fact_Inventory_Transactions.QuantityDelta) > 0;
    """)

    # --- 2. Monthly Usage Trends ---
    conn.execute("""
    CREATE OR REPLACE VIEW dw.v_monthly_usage AS
    SELECT 
        Dim_Date.Year,
        Dim_Date.Month,
        Dim_Date.MonthName,
        Dim_Product.CategoryName,
        ABS(SUM(CASE WHEN Fact_Inventory_Transactions.QuantityDelta < 0 THEN Fact_Inventory_Transactions.QuantityDelta ELSE 0 END)) AS TotalConsumed
    FROM dw.Fact_Inventory_Transactions
    JOIN dw.Dim_Date ON Fact_Inventory_Transactions.DateKey = Dim_Date.DateKey
    JOIN dw.Dim_Product ON Fact_Inventory_Transactions.ProductKey = Dim_Product.ProductKey
    GROUP BY 
        Dim_Date.Year, 
        Dim_Date.Month, 
        Dim_Date.MonthName, 
        Dim_Product.CategoryName
    ORDER BY Dim_Date.Year DESC, Dim_Date.Month DESC;
    """)

    # --- 3. User Activity Audit ---
    # Useful for a "Recent Activity" table in your UI
    conn.execute("""
    CREATE OR REPLACE VIEW dw.v_recent_activity AS
    SELECT 
        Dim_Date.FullDate,
        Dim_User.UserName,
        Dim_Product.ProductName,
        Fact_Inventory_Transactions.EventType,
        Fact_Inventory_Transactions.QuantityDelta,
        Dim_Location.SiteName
    FROM dw.Fact_Inventory_Transactions
    JOIN dw.Dim_Date ON Fact_Inventory_Transactions.DateKey = Dim_Date.DateKey
    JOIN dw.Dim_User ON Fact_Inventory_Transactions.UserKey = Dim_User.UserKey
    JOIN dw.Dim_Product ON Fact_Inventory_Transactions.ProductKey = Dim_Product.ProductKey
    JOIN dw.Dim_Location ON Fact_Inventory_Transactions.LocationKey = Dim_Location.LocationKey
    ORDER BY Dim_Date.FullDate DESC;
    """)

    print("✅ Analytics Views created in dw schema.")
    conn.close()

if __name__ == "__main__":
    create_analytics_views()