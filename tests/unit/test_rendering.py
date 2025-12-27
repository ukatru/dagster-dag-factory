import os
import unittest
from pathlib import Path
from dagster import EnvVar
from dagster_dag_factory.factory.asset_factory import AssetFactory
from dagster_dag_factory.factory.helpers.rendering import render_config
from dagster_dag_factory.factory.helpers.env_accessor import EnvVarAccessor
from dagster_dag_factory.factory.helpers.dynamic import Dynamic

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

    def test_dynamic_object(self):
        # Test dot notation for dicts
        data = {
            "top": "value",
            "nested": {
                "key": "secret",
                "deep": {
                    "val": 123
                }
            }
        }
        obj = Dynamic(data)
        self.assertEqual(obj.top, "value")
        self.assertEqual(obj.nested.key, "secret")
        self.assertEqual(obj.nested.deep.val, 123)

    def test_rendering_with_dynamic_vars(self):
        # Sample configuration with vars and env
        config = {
            "source": {
                "path": "{{vars.FIN_SFTP_PATH}}/incoming",
                "project": "{{vars.PROJECT_NAME}}"
            },
            "target": {
                "description": "Running in {{vars.ENV_NAME}}",
                "secret": "{{env.MY_SECRET}}"
            }
        }
        
        # In the real system, factory.env_vars is wrapped in Dynamic
        template_vars = {
            "vars": Dynamic(self.factory.env_vars),
            "env": EnvVarAccessor()
        }
        
        rendered = render_config(config, template_vars)
        
        self.assertEqual(rendered["source"]["path"], "/home/ukatru/data/incoming")
        self.assertEqual(rendered["source"]["project"], "Dagster ETL Platform")
        self.assertEqual(rendered["target"]["description"], "Running in development")
        # Full match should return EnvVar object
        self.assertIsInstance(rendered["target"]["secret"], EnvVar)

    def test_nested_rendering_dot_notation(self):
        # Test a deeper nested var structure
        dists = {
            "s3": {
                "buckets": {
                    "raw": "my-raw-bucket"
                }
            }
        }
        template_vars = {
            "vars": Dynamic(dists)
        }
        config = "Bucket: {{vars.s3.buckets.raw}}"
        rendered = render_config(config, template_vars)
        self.assertEqual(rendered, "Bucket: my-raw-bucket")

    def test_nested_model_item_rendering(self):
        from dagster_dag_factory.configs.s3 import S3Config
        from dagster_dag_factory.factory.helpers.rendering import render_config
        from dagster_dag_factory.models.file_info import FileInfo
        
        info = FileInfo(
            file_name="customer_data.csv",
            full_file_path="/src/raw/customer_data.csv",
            root_path="/src",
            file_size=5000,
            modified_ts=1671234567.0
        )
        
        # Verify Framework properties
        self.assertEqual(info.file_path, "raw/customer_data.csv")
        self.assertEqual(info.path, "raw")
        self.assertEqual(info.name, "customer_data")
        self.assertEqual(info.ext, ".csv")
        
        source_model = S3Config(
            connection="s3_src",
            bucket_name="src-bucket",
            key="raw/"
        )
        source_model.item = info
        
        template_vars = {"source": source_model}
        
        # Test interpolation with new properties
        target_conf = {
            "key": "archive/{{source.item.path}}/{{source.item.name}}_backup{{source.item.ext}}"
        }
        
        rendered = render_config(target_conf, template_vars)
        self.assertEqual(rendered["key"], "archive/raw/customer_data_backup.csv")

    def test_strict_routing_no_backward_compat(self):
        # test that top-level 'item' is NOT available if not explicitly provided
        from dagster_dag_factory.models.file_info import FileInfo
        
        info = FileInfo(
            file_name="test.csv",
            full_file_path="/test.csv",
            root_path="/",
            file_size=1,
            modified_ts=1.0
        )
        
        template_vars = {
            "source": Dynamic({"item": info})
        }
        
        config = "File: {{item.file_name}}"
        rendered = render_config(config, template_vars)
        # Should stay literal as {{item.file_name}} is not reachable at top level
        self.assertEqual(rendered, "File: {{item.file_name}}")
        
        config_nested = "File: {{source.item.file_name}}"
        rendered_nested = render_config(config_nested, template_vars)
        self.assertEqual(rendered_nested, "File: test.csv")

if __name__ == "__main__":
    unittest.main()
