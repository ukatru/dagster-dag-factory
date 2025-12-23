import sys
import os

# Add src to path
sys.path.append("/home/ukatru/github/dagster-dag-factory/src")

try:
    print("Verifying imports...")
    import dagster_dag_factory
    import dagster_dag_factory.configs.base
    import dagster_dag_factory.configs.s3
    import dagster_dag_factory.configs.sftp
    import dagster_dag_factory.configs.snowflake
    import dagster_dag_factory.configs.sqlserver
    import dagster_dag_factory.configs.compression
    
    import dagster_dag_factory.resources.base
    import dagster_dag_factory.resources.s3
    import dagster_dag_factory.resources.sftp
    import dagster_dag_factory.resources.snowflake
    import dagster_dag_factory.resources.sqlserver
    
    import dagster_dag_factory.operators.sftp_s3
    import dagster_dag_factory.operators.sqlserver_s3
    import dagster_dag_factory.operators.s3_snowflake
    import dagster_dag_factory.factory.base_operator
    
    print("SUCCESS: All modules imported successfully with absolute paths.")
except ImportError as e:
    print(f"FAILURE: {e}")
    sys.exit(1)
except Exception as e:
    print(f"FAILURE: {e}")
    sys.exit(1)
