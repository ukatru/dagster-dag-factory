import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path.cwd() / "src"))

from dagster_dag_factory.configs.s3 import S3Config
from dagster_dag_factory.configs.csv import CsvConfig
from dagster_dag_factory.factory.helpers.rendering import render_config_model

def test_template_fields():
    # 1. Setup S3 config with mixed templates
    config = S3Config(
        connection="aws_default",
        bucket_name="{{ vars.bucket }}", # Whitelisted
        key="data/{{ source.item.name }}", # Whitelisted
        object_type="CSV", # NOT whitelisted (Enum/Literal)
        csv_options=CsvConfig(
            delimiter="{{ vars.delim }}" # Whitelisted in nested model
        )
    )

    template_vars = {
        "vars": {"bucket": "my-real-bucket", "delim": "|"},
        "source": {"item": {"name": "sales.csv"}}
    }

    # 2. Render
    rendered = render_config_model(config, template_vars)

    # 3. Assertions
    print(f"Bucket: {rendered.bucket_name}")
    print(f"Key: {rendered.key}")
    print(f"Delimiter: {rendered.csv_options.delimiter}")
    
    assert rendered.bucket_name == "my-real-bucket"
    assert rendered.key == "data/sales.csv"
    assert rendered.csv_options.delimiter == "|"
    
    print("\n✅ Template Fields whitelisting works correctly!")

if __name__ == "__main__":
    test_template_fields()
