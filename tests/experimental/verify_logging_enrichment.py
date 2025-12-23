import unittest
from unittest.mock import MagicMock
import json
from dagster_dag_factory.factory.base_operator import BaseOperator
from dagster_dag_factory.configs.s3 import S3Config
from dagster_dag_factory.resources.s3 import S3Resource

class MockOperator(BaseOperator):
    def execute(self, context, source_config, target_config):
        return {}

class TestLoggingDecoupling(unittest.TestCase):
    def test_log_configs_with_connection_details(self):
        # 1. Setup Resource (Masked)
        s3_resource = S3Resource(
            access_key="AKIASECRET",
            secret_key="SECRET_KEY_123",
            region_name="us-west-2",
            endpoint_url="http://localhost:9000"
        )
        
        # 2. Setup Context with Resource
        context = MagicMock()
        context.resources = MagicMock()
        setattr(context.resources, "s3_prod", s3_resource)
        
        # 3. Setup Config
        s3_config = S3Config(
            connection="s3_prod",
            bucket_name="my-bucket",
            key="test.csv"
        )
        
        # 4. Mock Operator
        op = MockOperator()
        
        # 5. Capture logs
        log_messages = []
        context.log.info.side_effect = lambda msg: log_messages.append(msg)
        
        # 6. Run log_configs
        op.log_configs(context, {}, s3_config)
        
        # 7. Verify
        self.assertEqual(len(log_messages), 2)
        target_log = log_messages[1]
        self.assertIn("Target Configuration:", target_log)
        
        # Parse JSON
        json_data = json.loads(target_log.replace("Target Configuration:\n", ""))
        self.assertEqual(json_data["connection"], "s3_prod")
        self.assertEqual(json_data["bucket_name"], "my-bucket")
        
        # Verify connection details presence
        self.assertIn("connection_details", json_data)
        details = json_data["connection_details"]
        self.assertEqual(details["region_name"], "us-west-2")
        self.assertEqual(details["access_key"], "******")
        self.assertEqual(details["secret_key"], "******")
        
        print("Verified Log Structure:")
        print(target_log)

if __name__ == "__main__":
    unittest.main()
