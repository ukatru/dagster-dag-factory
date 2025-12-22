from typing import Optional, Any, List, ClassVar
import boto3
import os
import uuid
from pydantic import Field
import io
from .base import BaseConfigurableResource

class S3Resource(BaseConfigurableResource):
    """
    Dagster resource for S3 operations with enterprise authentication support.
    Supports explicit keys, profiles, and EKS Web Identity (IRSA).
    """
    mask_fields: ClassVar[List[str]] = BaseConfigurableResource.mask_fields + ["access_key", "secret_key", "session_token"]
    
    bucket_name: str = Field(description="Default bucket name")
    
    # Auth: Explicit Keys
    access_key: Optional[str] = Field(default=None, description="AWS Access Key ID")
    secret_key: Optional[str] = Field(default=None, description="AWS Secret Access Key")
    session_token: Optional[str] = Field(default=None, description="AWS Session Token")
    
    # Auth: Role Assumption & IRSA
    assume_role_arn: Optional[str] = Field(default=None, description="ARN of role to assume")
    aws_web_identity_token_file: Optional[str] = Field(default=None, description="Path to web identity token file (IRSA)")
    assume_role_session_name: Optional[str] = Field(default=None, description="Session name for assumed role")
    external_id: Optional[str] = Field(default=None, description="External ID for cross-account role assumption")
    
    # Configuration
    region_name: str = Field(default="us-east-1", description="AWS Region")
    endpoint_url: Optional[str] = Field(default=None, description="Custom endpoint URL (e.g. for MinIO)")
    profile_name: Optional[str] = Field(default=None, description="AWS Profile name")
    use_unsigned_session: bool = Field(default=False, description="Use unsigned session")
    verify: bool = Field(default=True, description="Verify SSL certificates")

    def get_session(self) -> boto3.Session:
        """
        Creates a boto3 Session with priority:
        1. Explicit Keys
        2. Profile
        3. Web Identity (IRSA) / Role Assumption
        4. Default Chain (Env vars, Instance Profile)
        """
        session_kwargs = {
            "region_name": self.region_name,
            "profile_name": self.profile_name
        }
        
        if self.access_key and self.secret_key:
            session_kwargs.update({
                "aws_access_key_id": self.access_key,
                "aws_secret_access_key": self.secret_key,
                "aws_session_token": self.session_token
            })
            
        session = boto3.Session(**session_kwargs)

        if self.assume_role_arn:
            sts_client = session.client('sts')
            role_session_name = self.assume_role_session_name or f"dagster-{uuid.uuid4()}"
            
            web_identity_token = None
            if self.aws_web_identity_token_file:
                with open(self.aws_web_identity_token_file, 'r') as f:
                    web_identity_token = f.read()
            elif os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE") and not self.access_key:
                with open(os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"], 'r') as f:
                    web_identity_token = f.read()

            if web_identity_token:
                sts_response = sts_client.assume_role_with_web_identity(
                    RoleArn=self.assume_role_arn,
                    RoleSessionName=role_session_name,
                    WebIdentityToken=web_identity_token
                )
            else:
                assume_kwargs = {
                    "RoleArn": self.assume_role_arn,
                    "RoleSessionName": role_session_name
                }
                if self.external_id:
                    assume_kwargs["ExternalId"] = self.external_id
                    
                sts_response = sts_client.assume_role(**assume_kwargs)

            credentials = sts_response['Credentials']
            return boto3.Session(
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken'],
                region_name=self.region_name
            )

        return session

    def get_client(self) -> Any:
        session = self.get_session()
        config = None
        if self.use_unsigned_session:
            from botocore import UNSIGNED
            from botocore.config import Config
            config = Config(signature_version=UNSIGNED)

        return session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            verify=self.verify,
            config=config
        )

    def write_csv(self, key: str, data: list, headers: list = None) -> None:
        if not data:
            return
        import pandas as pd
        df = pd.DataFrame(data)
        if headers:
            df = df[headers]
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        self.get_client().put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=csv_buffer.getvalue()
        )

    def write_parquet(self, key: str, data: list) -> None:
        if not data:
            return
        import pandas as pd
        df = pd.DataFrame(data)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        self.get_client().put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=parquet_buffer.getvalue()
        )

    def read_csv_sample(self, key: str, nrows: int = 10, delimiter: str = ",") -> Any:
        import pandas as pd
        response = self.get_client().get_object(Bucket=self.bucket_name, Key=key, Range='bytes=0-10240')
        return pd.read_csv(response["Body"], nrows=nrows, sep=delimiter)
