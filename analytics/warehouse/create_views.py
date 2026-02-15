# --Current inventory--
from multiprocessing.dummy import connection
import duckdb
from analytics.data.connect_db import get_warehouse_conn

def create_analytics_views():
    conn = get_warehouse_conn()

    try:
        #-- Base metrics view
        conn.execute("""
            CREATE OR REPLACE VIEW dw.v_inventory_metrics_base AS
            SELECT 
                ProductKey,
                LocationKey,
                UserKey,
                DateKey,
                -- central logic for metrics
                ABS(SUM(CASE WHEN QuantityDelta < 0 THEN QuantityDelta ELSE 0 END)) AS TotalQuantityConsumed,
                COUNT(CASE WHEN QuantityDelta < 0 THEN 1 END) AS TransactionCount,
                COUNT(CASE WHEN QuantityDelta > 0 THEN 1 END) AS ReplenishmentCount,
                SUM(QuantityDelta) AS NetStockChange
            FROM dw.Fact_Inventory_Transactions
            GROUP BY ProductKey, LocationKey, UserKey, DateKey;
            """)
        
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
            SUM(v_inventory_metrics_base.TotalQuantityConsumed) AS TotalQuantityConsumed,
        FROM dw.v_inventory_metrics_base
        JOIN dw.Dim_Date ON v_inventory_metrics_base.DateKey = Dim_Date.DateKey
        JOIN dw.Dim_Product ON v_inventory_metrics_base.ProductKey = Dim_Product.ProductKey
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

        # --- 4. Product Consumption Summary
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_consumption_summary AS
        SELECT 
            Dim_Product.ProductName,
            Dim_Product.CategoryName,
            Dim_Product.UnitOfMeasure,
            SUM(v_inventory_metrics_base.TotalQuantityConsumed) AS TotalQuantityConsumed,
            SUM(v_inventory_metrics_base.TransactionCount) AS TransactionCount,
        FROM dw.v_inventory_metrics_base
        JOIN dw.Dim_Product ON v_inventory_metrics_base.ProductKey = Dim_Product.ProductKey
        GROUP BY 
            Dim_Product.ProductName, 
            Dim_Product.CategoryName, 
            Dim_Product.UnitOfMeasure
        ORDER BY TotalQuantityConsumed DESC;
        """)

        # --- 5. Location Consumption Overview (Hotspots)
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_location_hotspots AS
        SELECT 
            Dim_Location.SiteName,
            Dim_Location.Building,
            Dim_Location.RoomNumber,
            SUM(v_inventory_metrics_base.TotalQuantityConsumed) AS TotalQuantityConsumed,
            SUM(v_inventory_metrics_base.TransactionCount) AS TransactionCount,
            -- Restocks (to identify replenishment frequency)
            SUM(v_inventory_metrics_base.ReplenishmentCount) AS ReplenishmentCount
        FROM dw.v_inventory_metrics_base
        JOIN dw.Dim_Location ON v_inventory_metrics_base.LocationKey = Dim_Location.LocationKey
        GROUP BY 
            Dim_Location.SiteName, 
            Dim_Location.Building, 
            Dim_Location.RoomNumber
        ORDER BY TotalQuantityConsumed DESC;
        """)

        # --- 6. Product x Location Matrix ---
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_product_location_matrix AS
        SELECT 
            Dim_Product.ProductName,
            Dim_Product.CategoryName,
            Dim_Location.SiteName,
            Dim_Location.Building,
            ABS(SUM(CASE WHEN Fact_Inventory_Transactions.QuantityDelta < 0 
                        THEN Fact_Inventory_Transactions.QuantityDelta 
                        ELSE 0 END)) AS QuantityConsumed,
            SUM(Fact_Inventory_Transactions.QuantityDelta) AS CurrentLocalStock
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_Product  ON Fact_Inventory_Transactions.ProductKey = dw.Dim_Product.ProductKey
        JOIN dw.Dim_Location ON Fact_Inventory_Transactions.LocationKey = dw.Dim_Location.LocationKey
        GROUP BY 
            Dim_Product.ProductName, 
            Dim_Product.CategoryName, 
            Dim_Location.SiteName, 
            Dim_Location.Building;
        """)

        # --- 7. User x Product Consumption (Accountability)   
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_user_product_consumption AS
        SELECT 
            Dim_User.UserName,
            Dim_User.UserRole,
            Dim_Product.ProductName,
            Dim_Product.CategoryName,
            ABS(SUM(CASE WHEN Fact_Inventory_Transactions.QuantityDelta < 0 
                        THEN Fact_Inventory_Transactions.QuantityDelta 
                        ELSE 0 END)) AS TotalQuantityConsumed,
            COUNT(Fact_Inventory_Transactions.TransactionID) AS TotalActions
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_User ON Fact_Inventory_Transactions.UserKey = dw.Dim_User.UserKey
        JOIN dw.Dim_Product ON Fact_Inventory_Transactions.ProductKey = dw.Dim_Product.ProductKey
        GROUP BY 
            Dim_User.UserName, 
            Dim_User.UserRole, 
            Dim_Product.ProductName, 
            Dim_Product.CategoryName
        ORDER BY TotalQuantityConsumed DESC;
        """)
        # --- 8. Daily Inventory Movement Log (The Audit Trail) 
        conn.execute("""
        CREATE OR REPLACE VIEW dw.v_movement_log AS
        SELECT 
            Dim_Date.FullDate,
            Dim_Product.ProductName,
            Dim_Location.SiteName,
            Dim_Location.Building,
            Dim_User.UserName,
            Fact_Inventory_Transactions.EventType,
            Fact_Inventory_Transactions.QuantityDelta,
            Fact_Inventory_Transactions.AbsoluteQuantity AS NewQuantity,
            Fact_Inventory_Transactions.CurrentStockSnapshot
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_Date ON Fact_Inventory_Transactions.DateKey = dw.Dim_Date.DateKey
        JOIN dw.Dim_Product ON Fact_Inventory_Transactions.ProductKey = dw.Dim_Product.ProductKey
        JOIN dw.Dim_Location ON Fact_Inventory_Transactions.LocationKey = dw.Dim_Location.LocationKey
        JOIN dw.Dim_User ON Fact_Inventory_Transactions.UserKey = dw.Dim_User.UserKey
        ORDER BY Dim_Date.FullDate DESC;
        """)
        
        print("✅ Analytics Views created in dw schema.")

    except Exception as e:
        print(f"❌ Error creating views: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    create_analytics_views()