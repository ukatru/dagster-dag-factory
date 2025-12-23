import os
import unittest
from pathlib import Path
from dagster import EnvVar
from dagster_dag_factory.factory.asset_factory import AssetFactory
from dagster_dag_factory.factory.helpers.rendering import render_config
from dagster_dag_factory.factory.helpers.env_accessor import EnvVarAccessor

class TestVariableRendering(unittest.TestCase):
    def setUp(self):
        self.base_dir = Path("/home/ukatru/github/dagster-pipelines/src/pipelines")
        # Ensure ENV=dev for test
        os.environ["ENV"] = "dev"
        self.factory = AssetFactory(self.base_dir)

    def test_variable_loading(self):
        # Check common vars
        self.assertEqual(self.factory.env_vars.get("PROJECT_NAME"), "Dagster ETL Platform")
        # Check dev overrides/additions
        self.assertEqual(self.factory.env_vars.get("FIN_SFTP_PATH"), "/home/ukatru/data")
        self.assertEqual(self.factory.env_vars.get("ENV_NAME"), "development")

    def test_rendering(self):
        # Sample configuration with vars and env
        config = {
            "source": {
                "path": "{{vars.FIN_SFTP_PATH}}/incoming",
                "project": "{{vars.PROJECT_NAME}}"
            },
            "target": {
                "description": "Running in {{vars.ENV_NAME}}",
                "secret": "{{env.MY_SECRET}}"
            },
            "other": "Literal Value",
            "missing": "{{vars.NON_EXISTENT}}"
        }
        
        # Test vars
        template_vars = {
            "vars": self.factory.env_vars,
            "env": EnvVarAccessor()
        }
        
        rendered = render_config(config, template_vars)
        
        self.assertEqual(rendered["source"]["path"], "/home/ukatru/data/incoming")
        self.assertEqual(rendered["source"]["project"], "Dagster ETL Platform")
        self.assertEqual(rendered["target"]["description"], "Running in development")
        # Full match should return EnvVar object
        self.assertIsInstance(rendered["target"]["secret"], EnvVar)
        self.assertEqual(rendered["target"]["secret"].env_var_name, "MY_SECRET")
        
        self.assertEqual(rendered["other"], "Literal Value")
        # Should remain literal if not found
        self.assertEqual(rendered["missing"], "{{vars.NON_EXISTENT}}")

    def test_nested_rendering(self):
        # test partition + vars rendering
        config = "Path: {{vars.FIN_SFTP_PATH}}/date={{partition_key}}"
        template_vars = {
            "vars": self.factory.env_vars,
            "partition_key": "2023-10-27"
        }
        rendered = render_config(config, template_vars)
        self.assertEqual(rendered, "Path: /home/ukatru/data/date=2023-10-27")

if __name__ == "__main__":
    unittest.main()
