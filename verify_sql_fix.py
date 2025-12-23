import sys
from unittest.mock import MagicMock

# Mock dagster and other deps if needed, but we can just use the classes
sys.path.append("/home/ukatru/github/dagster-dag-factory/src")

from dagster_dag_factory.configs.sqlserver import SQLServerConfig
from dagster_dag_factory.resources.sqlserver import SQLServerResource

def test_sqlserver_config_aliasing():
    print("Testing SQLServerConfig field aliasing...")
    
    # Check 'query' works
    conf_query = SQLServerConfig(connection="conn", query="SELECT 1")
    assert conf_query.query == "SELECT 1"
    print("SUCCESS: 'query' field works")
    
    # Check 'sql' alias works
    conf_sql = SQLServerConfig(connection="conn", sql="SELECT 1")
    assert conf_sql.query == "SELECT 1"
    assert conf_sql.sql == "SELECT 1"
    print("SUCCESS: 'sql' field aliases to 'query'")

def test_sqlserver_resource_extract():
    print("Testing SQLServerResource.extract...")
    
    # Mock pyodbc/connection
    resource = SQLServerResource(
        host="localhost",
        database="db",
        user="user",
        password="pwd"
    )
    
    # Mock execute_query to avoid real connection
    resource.execute_query = MagicMock(return_value=[{"a": 1}])
    
    conf = SQLServerConfig(connection="conn", sql="SELECT * FROM test")
    result = resource.extract(conf)
    
    resource.execute_query.assert_called_with(sql="SELECT * FROM test")
    assert result == [{"a": 1}]
    print("SUCCESS: SQLServerResource.extract called execute_query with correct string")

if __name__ == "__main__":
    try:
        test_sqlserver_config_aliasing()
        test_sqlserver_resource_extract()
        print("\nAll tests passed!")
    except Exception as e:
        print(f"\nFAILURE: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
