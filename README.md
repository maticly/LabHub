# LabHub
sql db - holds CS779_LabHub_db file with CS779_LabHub_final SQL Server database.
    *that SQL db runs on TABLET-LTM0C509\SQLEXPRESS01 server.
    *to populate data into OLTP use sql db/CS779_LabHub_db/11_populate - place csv files in the GitHub folder as VSCode has permission to read only that folder.

analytics/data
    /generated_data_OLTP - data to populate tables 
    /connect_db.py - Python connection to OLTP
    /warehouse.duckdb - duckDB warehouse

analytics/etl
    /etl_inventory.py

analytics/warehouse
    /init_warehouse.py - initiates Duck DB connection. Please make sure that DB_PATH & SCHEMA_SQL_PATH are up-to-date and correctly defined
    /warehouse_schema.sql - the warehouse schema - this is where you make any changes to the OLAP schema
    /warehouse.duckdb - FIGURE IT OUT WHICH .duckdb is the correct one. This file seeps to be 'used by different service' - check which service uses it