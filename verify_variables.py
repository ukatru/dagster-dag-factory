import os
import unittest
from pathlib import Path
from dagster_dag_factory.factory.asset_factory import AssetFactory
from dagster_dag_factory.factory.helpers.rendering import render_config

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
        # Sample configuration with env vars
        config = {
            "source": {
                "path": "{{env.FIN_SFTP_PATH}}/incoming",
                "project": "{{env.PROJECT_NAME}}"
            },
            "target": {
                "description": "Running in {{env.ENV_NAME}}"
            },
            "other": "Literal Value",
            "missing": "{{env.NON_EXISTENT}}"
        }
        
        # Test vars
        template_vars = {
            "env": self.factory.env_vars
        }
        
        rendered = render_config(config, template_vars)
        
        self.assertEqual(rendered["source"]["path"], "/home/ukatru/data/incoming")
        self.assertEqual(rendered["source"]["project"], "Dagster ETL Platform")
        self.assertEqual(rendered["target"]["description"], "Running in development")
        self.assertEqual(rendered["other"], "Literal Value")
        # Should remain literal if not found
        self.assertEqual(rendered["missing"], "{{env.NON_EXISTENT}}")

    def test_nested_rendering(self):
        # test partition + env rendering
        config = "Path: {{env.FIN_SFTP_PATH}}/date={{partition_key}}"
        template_vars = {
            "env": self.factory.env_vars,
            "partition_key": "2023-10-27"
        }
        rendered = render_config(config, template_vars)
        self.assertEqual(rendered, "Path: /home/ukatru/data/date=2023-10-27")

if __name__ == "__main__":
    unittest.main()
