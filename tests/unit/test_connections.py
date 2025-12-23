import os
import unittest
from pathlib import Path
from dagster_dag_factory.factory.resource_factory import ResourceFactory
from dagster_dag_factory.resources.s3 import S3Resource
from dagster_dag_factory.resources.sqlserver import SQLServerResource

class TestConnectionLoading(unittest.TestCase):
    def setUp(self):
        self.conn_dir = Path("/home/ukatru/github/dagster-pipelines/src/pipelines/connections")
        # Ensure ENV=dev for test
        os.environ["ENV"] = "dev"
        # Dummy env vars for resource init
        os.environ["SQL_PASSWORD"] = "testpass"
        os.environ["AWS_ACCESS_KEY_ID"] = "testkey"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testsecret"

    def test_hierarchical_loading(self):
        resources = ResourceFactory.load_resources_from_dir(self.conn_dir)
        
        # Check if s3_prod is loaded and overridden from dev
        self.assertIn("s3_prod", resources)
        s3 = resources["s3_prod"]
        self.assertIsInstance(s3, S3Resource)
        # common has us-east-1, dev has us-west-2
        self.assertEqual(s3.region_name, "us-west-2")
        
        # Check if sqlserver_prod is loaded from dev
        self.assertIn("sqlserver_prod", resources)
        sql = resources["sqlserver_prod"]
        self.assertIsInstance(sql, SQLServerResource)
        self.assertEqual(sql.host, "winnuc002")
        self.assertEqual(sql.database, "DG_PLAY")

    def test_prod_loading(self):
        # Switch to prod
        os.environ["ENV"] = "prod"
        resources = ResourceFactory.load_resources_from_dir(self.conn_dir)
        
        # In prod, s3_prod should have us-east-1 (from common) 
        # unless prod.yaml overrides it. Current prod.yaml has us-west-2.
        self.assertIn("s3_prod", resources)
        s3 = resources["s3_prod"]
        self.assertEqual(s3.region_name, "us-west-2") # From prod.yaml
        
        # sqlserver_prod in prod.yaml has winnuc002
        sql = resources["sqlserver_prod"]
        self.assertEqual(sql.host, "winnuc002")

if __name__ == "__main__":
    unittest.main()
