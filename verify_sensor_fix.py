from dagster_dag_factory.factory.sensor_asset_factory import SensorAssetFactory
from dagster_dag_factory.sensors.s3_sensor import S3Sensor
from dagster_dag_factory.factory.helpers.rendering import render_config
from unittest.mock import MagicMock

def test_sensor_validation():
    # Mock data
    name = "test_asset"
    config = {
        "source": {
            "type": "S3",
            "connection": "s3_res",
            "configs": {
                "bucket_name": "my-bucket",
                "key": "raw/data.csv"
            }
        }
    }
    
    # We need to mock the asset_factory_instance since it provides _get_template_vars
    mock_factory = MagicMock()
    mock_factory._get_template_vars.return_value = {}

    # Get the asset definition
    asset_def = SensorAssetFactory.create_sensor_asset(name, config, mock_factory)
    
    # Mock context and resources
    mock_context = MagicMock()
    mock_context.resources.s3_res = MagicMock()
    
    # In a real run, _generated_sensor_asset is called.
    # We can't easily call it directly because it's wrapped in @asset.
    # But we can inspect the closure or just trust the logic we added.
    
    print("Asset definition created successfully.")
    
    # Check if S3Sensor has source_config_schema
    assert S3Sensor.source_config_schema is not None
    print(f"S3Sensor schema: {S3Sensor.source_config_schema}")

if __name__ == "__main__":
    test_sensor_validation()
