import streamlit as st
import duckdb
import plotly.express as px
from vector.search import semantic_search


@st.dialog("📦 Product Stock Details", width="large")
def show_stock_detail(product_id, db_path):
    conn = duckdb.connect(db_path)
    
    # Current Total and Product Info
    product_info = conn.execute("""
        SELECT 
            dw.Dim_Product.ProductName, 
            dw.Dim_Product.CategoryName, 
            COALESCE(SUM(dw.Fact_Inventory_Transactions.AbsoluteQuantity), 0) as TotalStock
        FROM dw.Dim_Product
        JOIN dw.Fact_Inventory_Transactions ON dw.Dim_Product.ProductKey = dw.Fact_Inventory_Transactions.ProductKey
        WHERE dw.Dim_Product.ProductID = ?
        GROUP BY dw.Dim_Product.ProductName, dw.Dim_Product.CategoryName
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
        SELECT 
            CONCAT(dw.Dim_Location.SiteName) AS LocationPath,
            CAST(SUM(dw.Fact_Inventory_Transactions.AbsoluteQuantity) AS INTEGER) AS Stock
        FROM dw.Fact_Inventory_Transactions
        JOIN dw.Dim_Location ON dw.Fact_Inventory_Transactions.LocationKey = dw.Dim_Location.LocationKey
        JOIN dw.Dim_Product ON dw.Fact_Inventory_Transactions.ProductKey = dw.Dim_Product.ProductKey
        WHERE dw.Dim_Product.ProductID = ?
        GROUP BY 1
        HAVING Stock > 0
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