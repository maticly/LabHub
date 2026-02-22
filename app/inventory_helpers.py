import streamlit as st
import duckdb
import plotly.express as px
from vector.search import semantic_search
import pandas as pd


@st.dialog("📦 Product Stock Details", width="large")
def show_stock_detail(product_id, db_path):
    conn = duckdb.connect(db_path)
    
    # Current Total and Product Info
    product_info = conn.execute("""
    SELECT 
        dp.ProductName,
        dp.CategoryName,
        COALESCE(SUM(v.CurrentStock),0) AS TotalStock
    FROM dw.v_product_distribution_detailed v
    JOIN dw.Dim_Product dp
        ON v.ProductName = dp.ProductName
    WHERE dp.ProductID = ?
    GROUP BY dp.ProductName, dp.CategoryName
""", [product_id]).fetchone()
    
    if not product_info:
        st.error("Product not found in warehouse.") 
        conn.close() 
        return
 
    st.header(product_info[0]) 
    st.subheader(f"Total Inventory: {int(product_info[2])} units") 
    st.divider() 

    st.write("### 📍 Location Breakdown")
    

    location_df = conn.execute("""
    WITH LatestState AS (
        SELECT 
            ProductKey,
            LocationKey,
            AbsoluteQuantity,
            ROW_NUMBER() OVER (
                PARTITION BY ProductKey, LocationKey
                ORDER BY DateKey DESC, TransactionID DESC
            ) AS r
        FROM dw.Fact_Inventory_Transactions
    )
    SELECT 
        dl.SiteName AS LocationPath,
        CAST(ls.AbsoluteQuantity AS INT) AS CurrentStock
    FROM LatestState ls
    JOIN dw.Dim_Product dp 
        ON ls.ProductKey = dp.ProductKey
    JOIN dw.Dim_Location dl
        ON ls.LocationKey = dl.LocationKey
    WHERE dp.ProductID = ?
      AND ls.r = 1
      AND ls.AbsoluteQuantity > 0
""", [product_id]).df()




    if location_df.empty:
        st.info("This product exists in the catalog but is not currently in stock at any location.")
    else:
        st.table(location_df)

    st.divider()
    
    description_row = conn.execute("""
        SELECT Description 
        FROM dw.Dim_Product WHERE ProductID = ?
    """, [product_id]).fetchone()

    description = description_row[0] if description_row else None
    
    st.write("### 🧪 Related Items")
    if description:
        related = semantic_search(description, n_results=4) 
        related = [r for r in related if r["id"] != str(product_id)] 
        related = related[:3]

        for r in related: 
            st.write(f"- {r['metadata']['name']}")
    else: 
        st.info("No description available for this product, so related item search is not possible.")
    
       
    conn.close()