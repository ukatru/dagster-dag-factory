import os
import sys
import time
import io
from pathlib import Path

# Setup paths
current_file = Path(__file__).resolve()
root_dir = current_file.parents[2] # github/dagster-dag-factory
pip_dir = root_dir.parent / "dagster-pipelines"
sys.path.append(str(root_dir / "src"))

def load_dotenv_manual(path):
    if not os.path.exists(path): return
    with open(path) as f:
        for line in f:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                os.environ[k.strip()] = v.strip().strip("\"").strip("'")

load_dotenv_manual(pip_dir / ".env")

from dagster_dag_factory.resources.sqlserver import SQLServerResource
from dagster_dag_factory.resources.sftp import SFTPResource

def generate_sql_data():
    print("--- Generating SQL Server Stress Data (500k rows, 25 cols) ---")
    sqlserver = SQLServerResource(
        host=os.environ.get("SQLSERVER_HOST"),
        user=os.environ.get("SQLSERVER_USERNAME"),
        password=os.environ.get("SQLSERVER_PASSWORD"),
        database=os.environ.get("SQLSERVER_DATABASE"),
    )
    
    table_name = "STRESS_TEST_WIDE"
    
    # Construct CREATE TABLE with 25 columns
    cols = ["ID INT PRIMARY KEY"]
    for i in range(1, 25):
        cols.append(f"COL_{i} VARCHAR(100)")
    create_sql = f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}; CREATE TABLE {table_name} ({', '.join(cols)})"
    
    # Generate data using a T-SQL loop for speed
    generate_sql = f"""
    SET NOCOUNT ON;
    DECLARE @counter INT = 1;
    WHILE @counter <= 500000
    BEGIN
        INSERT INTO {table_name} (ID, COL_1, COL_2, COL_3, COL_4, COL_5, COL_6, COL_7, COL_8, COL_9, COL_10, COL_11, COL_12, COL_13, COL_14, COL_15, COL_16, COL_17, COL_18, COL_19, COL_20, COL_21, COL_22, COL_23, COL_24)
        VALUES (@counter, 
                'VAL_'+CAST(@counter AS VARCHAR), 'VAL_'+CAST(@counter AS VARCHAR), 'VAL_'+CAST(@counter AS VARCHAR), 
                'VAL_'+CAST(@counter AS VARCHAR), 'VAL_'+CAST(@counter AS VARCHAR), 'VAL_'+CAST(@counter AS VARCHAR),
                'VAL_'+CAST(@counter AS VARCHAR), 'VAL_'+CAST(@counter AS VARCHAR), 'VAL_'+CAST(@counter AS VARCHAR),
                'VAL_'+CAST(@counter AS VARCHAR), 'VAL_'+CAST(@counter AS VARCHAR), 'VAL_'+CAST(@counter AS VARCHAR),
                'VAL_'+CAST(@counter AS VARCHAR), 'VAL_'+CAST(@counter AS VARCHAR), 'VAL_'+CAST(@counter AS VARCHAR),
                'VAL_'+CAST(@counter AS VARCHAR), 'VAL_'+CAST(@counter AS VARCHAR), 'VAL_'+CAST(@counter AS VARCHAR),
                'VAL_'+CAST(@counter AS VARCHAR), 'VAL_'+CAST(@counter AS VARCHAR), 'VAL_'+CAST(@counter AS VARCHAR),
                'VAL_'+CAST(@counter AS VARCHAR), 'VAL_'+CAST(@counter AS VARCHAR), 'VAL_'+CAST(@counter AS VARCHAR));
        SET @counter = @counter + 1;
        
        -- Batch commit every 10k rows
        IF @counter % 10000 = 0
        BEGIN
            PRINT 'Inserted ' + CAST(@counter AS VARCHAR) + ' rows...';
        END
    END
    """
    
    with sqlserver.get_connection() as conn:
        cursor = conn.cursor()
        print(f"Creating table {table_name}...")
        cursor.execute(create_sql)
        conn.commit()
        
        print("Starting data generation on server (this may take a minute)...")
        # T-SQL loop might be slow if we do it row by row like above. 
        # Better: use a cross join trick to generate many rows fast.
    
    # Optimized generation
    optimized_sql = f"""
    INSERT INTO {table_name} (ID, COL_1, COL_2, COL_3, COL_4, COL_5, COL_6, COL_7, COL_8, COL_9, COL_10, COL_11, COL_12, COL_13, COL_14, COL_15, COL_16, COL_17, COL_18, COL_19, COL_20, COL_21, COL_22, COL_23, COL_24)
    SELECT TOP 500000 
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
        'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V'
    FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c;
    """
    
    with sqlserver.get_connection() as conn:
        cursor = conn.cursor()
        print("Generating 500k rows using cross join...")
        cursor.execute(optimized_sql)
        conn.commit()
        print(f"✅ SQL Data Ready in {table_name}")

def generate_sftp_data():
    print("\n--- Generating SFTP Stress Data (10 files) ---")
    sftp_res = SFTPResource(
        host=os.environ.get("SFTP_HOST"),
        username=os.environ.get("SFTP_USERNAME"),
        password=os.environ.get("SFTP_PASSWORD"),
        port=int(os.environ.get("SFTP_PORT", 22)),
    )
    
    path = "/home/ukatru/data/stress_test"
    
    with sftp_res.get_client() as sftp:
        try:
            sftp.mkdir(path)
        except:
            pass
        
        for i in range(1, 11):
            filename = f"stress_file_{i}.csv"
            content = "id,name,value\n" + "\n".join([f"{j},item_{j},val_{j}" for j in range(1000)])
            print(f"Creating {filename} in SFTP...")
            sftp.putfo(io.BytesIO(content.encode()), os.path.join(path, filename))
    print(f"✅ SFTP Data Ready in {path}")

if __name__ == "__main__":
    generate_sql_data()
    generate_sftp_data()
