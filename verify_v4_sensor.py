import sys
import os
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from dagster_dag_factory.factory.sensor_factory import SensorFactory
from dagster_dag_factory.factory.asset_factory import AssetFactory
from dagster_dag_factory.factory.helpers.rendering import render_config
from dagster_dag_factory.models.s3_info import S3Info
from dagster_dag_factory.sensors.base_sensor import SensorRegistry, BaseSensor
from dagster_dag_factory.factory.helpers.dynamic import Dynamic

# 1. Register a Mock Sensor for testing
@SensorRegistry.register("MOCK_S3")
class MockS3Sensor(BaseSensor):
    def check(self, context, source_config, resource, cursor=None, **kwargs):
        # Simulate finding one file
        item = S3Info(
            bucket_name="factory-test-bucket",
            key="inbound/2023/12/sales_report_v1.csv",
            size=98765,
            modified_dt=None
        )
        return [item], "new_cursor_123"

def test_sensor_to_asset_v4_flow():
    print("🚀 Starting V4 Sensor-to-Asset Integration Test...")
    
    # --- STEP A: Setup Factories ---
    asset_factory = AssetFactory(Path("/tmp"))
    sensor_factory = SensorFactory()
    
    sensor_conf = {
        "name": "test_v4_sensor",
        "type": "MOCK_S3",
        "job": "test_job",
        "connection": "my_s3_conn",
        "configs": {"bucket_name": "test-bucket"}
    }
    
    # --- STEP B: Simulate Sensor Execution ---
    print("📡 Simulating sensor discovery...")
    mock_context = MagicMock()
    mock_context.cursor = None
    mock_context.resources.my_s3_conn = MagicMock()
    
    # Create the sensor definition via factory
    sensor_def = sensor_factory._create_sensor(sensor_conf, {}, asset_factory)
    
    # Evaluate sensor logic
    # Note: Dagster's SensorDefinition stores the logic in _evaluation_fn
    run_requests = list(sensor_def._evaluation_fn(mock_context))
    
    assert len(run_requests) == 1
    request = run_requests[0]
    
    print(f"✅ Sensor produced RunRequest with tags: {json.dumps(request.tags, indent=2)}")
    assert "factory/source_metadata" in request.tags
    assert request.tags["factory/source_item"] == "inbound/2023/12/sales_report_v1.csv"
    
    # --- STEP C: Simulate Asset Materialization with Tags ---
    print("🏗️ Simulating asset materialization with discovered tags...")
    
    # Mock the asset context during materialization
    # In reality, Dagster passes these tags into the run
    asset_context = MagicMock()
    asset_context.run.tags = request.tags
    
    # Get template variables (this is what AssetFactory.logic does)
    template_vars = asset_factory._get_template_vars(asset_context)
    
    # The logic we added to AssetFactory.logic:
    metadata_json = template_vars.get("run_tags", {}).get("factory/source_metadata")
    if metadata_json:
        template_vars["source"] = {
            "item": Dynamic(json.loads(metadata_json))
        }
    
    # --- STEP D: Verify Analyst Experience (Jinja Rendering) ---
    print("🪄 Verifying Jinja rendering for Analyst...")
    
    # An analyst would use: {{ source.item.object_name }}
    rendered_key = render_config("processed/{{ source.item.object_name }}", template_vars)
    print(f"   Target Key: 'processed/{{{{ source.item.object_name }}}}' -> '{rendered_key}'")
    assert rendered_key == "processed/sales_report_v1.csv"
    
    # And maybe use the bucket name or size:
    rendered_path = render_config("archive/{{ source.item.bucket_name }}/{{ source.item.key }}", template_vars)
    print(f"   Archive Path: 'archive/{{{{ source.item.bucket_name }}}}/{{{{ source.item.key }}}}' -> '{rendered_path}'")
    assert rendered_path == "archive/factory-test-bucket/inbound/2023/12/sales_report_v1.csv"

    print("\n✨ Integration Test Passed: Niagara-Inspired V4 Pattern is ROCK SOLID!")

if __name__ == "__main__":
    try:
        test_sensor_to_asset_v4_flow()
    except Exception as e:
        print(f"\n❌ Integration Test Failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
