from dagster_dag_factory.configs.s3 import S3Config
from dagster_dag_factory.configs.sqlserver import SQLServerConfig
import json

def test_masking():
    s3_conf = S3Config(
        connection="s3_prod",
        key="raw/sales/data.csv",
        object_type="CSV"
    )
    
    print("--- S3Config Masked Output ---")
    print(s3_conf.to_masked_json())
    
    masked_dict = s3_conf.to_masked_dict()
    if masked_dict.get("key") == "raw/sales/data.csv":
        print("SUCCESS: S3 key is NOT masked.")
    else:
        print(f"FAILURE: S3 key is still masked: {masked_dict.get('key')}")

    # Check if a truly sensitive field is still masked (using a hypothetical secret)
    # The BaseConfigModel masks anything containing "secret", "password", etc.
    class SecretConfig(S3Config):
        my_secret_token: str = "super_secret"

    secret_conf = SecretConfig(
        connection="s3_prod",
        key="test",
        my_secret_token="extremely_sensitive"
    )
    
    print("\n--- SecretConfig Masked Output ---")
    print(secret_conf.to_masked_json())
    
    if secret_conf.to_masked_dict().get("my_secret_token") == "******":
        print("SUCCESS: Secret token IS masked.")
    else:
        print("FAILURE: Secret token is NOT masked.")

if __name__ == "__main__":
    test_masking()
