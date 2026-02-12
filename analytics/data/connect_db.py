"""
THIS IS THE OLTP CONNECTION FILE.
IT CONTAINS FUNCTIONS TO CONNECT TO THE SQL SERVER OLTP DATABASE AND THE DUCKDB WAREHOUSE.
IT ALSO HAS SOME TEST FUNCTIONS TO CHECK THE CONNECTIONS AND RUN SAMPLE QUERIES.
"""

import pyodbc
import pandas as pd

Driver = 'ODBC Driver 18 for SQL Server'
Server = r'TABLET-LTM0C509\SQLEXPRESS01'
Database = 'CS779_LabHub_final'

def get_connection(): 
    conn_str = ( 
        f"DRIVER={{{Driver}}};"
        f"SERVER={Server};"
        f"DATABASE={Database};"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes;"
    ) 
    return pyodbc.connect(conn_str)

def test_query():
    conn = get_connection()
    query = "SELECT TOP 5 * FROM core.UserRole;"  # change to a table that exists
    df = pd.read_sql(query, conn)
    print(df)
    conn.close()

if __name__ == "__main__":
    try:
        test_query()
        print("✅ Connection successful and query executed.")
    except Exception as e:
        print("❌ Connection failed.")
        print(e)

