import unittest
from unittest.mock import MagicMock
from dagster import AssetKey, AssetCheckExecutionContext, AssetCheckResult
from dagster_dag_factory.operators.base_operator import BaseOperator
from dagster_dag_factory.operators.sqlserver_s3 import SqlServerS3Operator

class TestUnifiedObservationCheck(unittest.TestCase):
    def setUp(self):
        # We use a concrete implementation that inherits BaseOperator's generic check logic
        class GenericOperator(BaseOperator):
            def execute(self, *args, **kwargs): pass
            
        self.operator = GenericOperator()
        self.context = MagicMock(spec=AssetCheckExecutionContext)
        self.context.asset_key = AssetKey("test_asset")
        self.context.instance = MagicMock()

    def mock_materialization(self, metadata):
        class MockMetadataValue:
            def __init__(self, value):
                self.value = value

        wrapped_metadata = {k: MockMetadataValue(v) for k, v in metadata.items()}
        
        event = MagicMock()
        event.asset_materialization.metadata = wrapped_metadata
        self.context.instance.get_latest_materialization_event.return_value = event

    def get_meta_val(self, metadata, key):
        val = metadata[key]
        if hasattr(val, "value"):
            return val.value
        if hasattr(val, "text"):
            return val.text
        return val

    def test_generic_observation_pass(self):
        self.mock_materialization({
            "rows_extracted": 1000,
            "rows_written": 1000
        })
        
        config = {
            "type": "observation_diff",
            "source_key": "rows_extracted",
            "target_key": "rows_written",
            "threshold": 0,
            "_asset_key": AssetKey("test_asset")
        }
        
        result = self.operator.execute_check(self.context, config)
        self.assertTrue(result.passed)
        self.assertEqual(self.get_meta_val(result.metadata, "status"), "PASS")

    def test_sql_server_specialized_check(self):
        # Testing the specialized override in SqlServerS3Operator
        sql_op = SqlServerS3Operator()
        
        config = {
            "type": "sql_server_check",
            "query": "SELECT 100",
            "connection": "sql_conn",
            "threshold": 50
        }
        
        # Mock resource
        mock_resource = MagicMock()
        mock_resource.execute_query.return_value = [{"count": 100}]
        setattr(self.context.resources, "sql_conn", mock_resource)
        
        result = sql_op.execute_check(self.context, config)
        self.assertTrue(result.passed)
        self.assertEqual(self.get_meta_val(result.metadata, "value"), 100)

    def test_fallback_to_generic(self):
        # SqlServerS3Operator should still support observation_diff via super()
        sql_op = SqlServerS3Operator()
        self.mock_materialization({"rows": 10, "cols": 10})
        
        config = {
            "type": "observation_diff",
            "source_key": "rows",
            "target_key": "cols",
            "_asset_key": AssetKey("test_asset")
        }
        
        result = sql_op.execute_check(self.context, config)
        self.assertTrue(result.passed)

if __name__ == "__main__":
    unittest.main()
