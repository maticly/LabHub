import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path
import os

def run_dq_checks():
    """
    Runs a Data Quality audit.
    """
    # 1. PATHING
    script_path = Path(__file__).resolve() 
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    db_path = PROJECT_ROOT / 'warehouse' / 'warehouse.duckdb'
    
    # 2. VITAL DEBUG PRINT
    print(f"🔍 Searching for database at: {db_path}")

    if not db_path.exists():
        print(f"❌ ERROR: Database file NOT FOUND at {db_path}")
        return
    
    # Connect to the verified path
    conn = duckdb.connect(str(db_path))
    
    report = {
    "title": "🛡️ LabHub Warehouse Audit",
    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M'),
    "checks": [],
    "passed_all": True
    }

    
    def add_check(name, query, expected_zero=True, critical=False):
        try:
            val = conn.execute(query).fetchone()[0]
            passed = (val == 0) if expected_zero else (val > 0)
            status = "✅ PASS" if passed else ("❌ FAIL" if critical else "⚠️ WARN")
            
            if not passed and critical:
                report["passed_all"] = False

            report["checks"].append({
                "check_name": name,
                "value": val,
                "status": status
            })
        except Exception as e:
            report["checks"].append({"check_name": name, "status": "💥 ERROR", "error": str(e)})
            report["passed_all"] = False

    #  1. Integrity Checks 
    add_check("Negative Stock (Absolute)", "SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions WHERE AbsoluteQuantity < 0", critical=True)
    add_check("Orphaned Products", "SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions f LEFT JOIN dw.Dim_Product p ON f.ProductKey = p.ProductKey WHERE p.ProductKey IS NULL", critical=True)
    add_check("Orphaned Locations", "SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions f LEFT JOIN dw.Dim_Location l ON f.LocationKey = l.LocationKey WHERE l.LocationKey IS NULL", critical=True)
    
    # 2. Duplicate Checks
    add_check("Duplicate Transaction IDs", "SELECT COUNT(TransactionID) - COUNT(DISTINCT TransactionID) FROM dw.Fact_Inventory_Transactions", critical=True)

    # 3. Semantic
    add_check("Missing AI Descriptions", "SELECT COUNT(*) FROM dw.Dim_Product WHERE Description IS NULL OR length(trim(Description)) < 5", critical=False)

    # 4. Freshness 
    today_key = int(datetime.now().strftime('%Y%m%d'))
    add_check("Data is from Today", f"SELECT COUNT(*) FROM dw.Fact_Inventory_Transactions WHERE DateKey = {today_key}", expected_zero=False, critical=False)

    conn.close()
        
    return report

def print_dq_report(report):
    print(f"\n{'='*40}")
    print(f"WAREHOUSE AUDIT REPORT: {report['timestamp']}")
    print(f"{'='*40}")

    for check in report["checks"]:
        print(f"{check['status']} | {check['check_name']}: {check.get('value', 'N/A')}")
        
    print(f"{'='*40}")
    if report["passed_all"]:
        print("✅ STATUS: HEALTHY")
    else:
        print("🛑 STATUS: ISSUES DETECTED")


if __name__ == "__main__":
    report = run_dq_checks()
    print_dq_report(report)

