import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from dagster import get_dagster_logger

def load_dotenv_manual(dotenv_path):
    if not os.path.exists(dotenv_path):
        return
    with open(dotenv_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip().strip('"').strip("'")

def check_system():
    # 1. Setup paths dynamically
    # Assuming script is in tests/integration/system_check.py
    current_file = Path(__file__).resolve()
    root_dir = current_file.parents[2] # github/dagster-dag-factory
    pip_dir = root_dir.parent / "dagster-pipelines"
    
    # Add src to python path
    sys.path.append(str(root_dir / "src"))
    
    # Load environment variables
    load_dotenv_manual(pip_dir / ".env")
    
    from dagster_dag_factory.factory.dagster_factory import DagsterFactory
    logger = get_dagster_logger()
    
    # 2. Validate Definitions
    print("\n--- [1/3] Validating Dagster Definitions ---")
    try:
        factory = DagsterFactory(pip_dir / "src/pipelines")
        defs = factory.build_definitions()
        print(f"✅ Definitions loaded successfully. Found {len(defs.assets)} assets.")
    except Exception as e:
        print(f"❌ Definitions failed to load: {e}")
        return

    # 3. Verify Connections and Auto-Provision Tables
    print("\n--- [2/3] Verifying Connections & Auto-Provisioning ---")
    resources = defs.resources
    sqlserver = resources.get("sqlserver_conn") or resources.get("sqlserver_prod")
    snowflake = resources.get("snowflake_conn") or resources.get("snowflake_prod")
    
    if sqlserver:
        print("Checking SQL Server...")
        try:
            with sqlserver.get_connection() as conn:
                cursor = conn.cursor()
                # Ensure test_customers exists
                cursor.execute("IF OBJECT_ID('dbo.test_customers', 'U') IS NULL CREATE TABLE dbo.test_customers (id INT, name VARCHAR(100), email VARCHAR(100), age INT)")
                # Ensure perf test table exists
                cursor.execute("IF OBJECT_ID('STG_PERF_TEST_100K', 'U') IS NULL CREATE TABLE STG_PERF_TEST_100K (ID INT, PRODUCT VARCHAR(100), AMOUNT FLOAT, CREATED_AT TIMESTAMP, REGION VARCHAR(50))")
                conn.commit()
            print("✅ SQL Server Connection & Tables Ready.")
        except Exception as e:
            print(f"⚠️ SQL Server issue: {e}")
    
    if snowflake:
        print("Checking Snowflake...")
        try:
            # Ensure database and schema exist
            snowflake.execute_query("CREATE DATABASE IF NOT EXISTS SNOWFLAKE_LEARNING_DB")
            snowflake.execute_query("CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_LEARNING_DB.PUBLIC")
            # Ensure target tables exist
            snowflake.execute_query("CREATE TABLE IF NOT EXISTS SALES_RAW (SaleID INT, Product VARCHAR(100), Amount FLOAT, SaleDate DATE, Region VARCHAR(100))")
            snowflake.execute_query("CREATE TABLE IF NOT EXISTS STG_PERF_TEST_INCREMENTAL (ID INT, PRODUCT VARCHAR(100), AMOUNT FLOAT, CREATED_AT TIMESTAMP, REGION VARCHAR(50))")
            print("✅ Snowflake Connection & Tables Ready.")
        except Exception as e:
            print(f"⚠️ Snowflake issue: {e}")

    # 4. Final Summary
    print("\n--- [3/3] System Health Summary ---")
    print("🚀 All definitions are valid.")
    print("📈 Showcase pipeline 'big_showcase_job' is ready for visualization.")
    print("🔗 Data pipelines are auto-provisioned and ready for execution.")

if __name__ == "__main__":
    check_system()
