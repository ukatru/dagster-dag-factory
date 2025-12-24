import os
import uuid
from typing import Any, ClassVar, List, Optional

import boto3
from pydantic import Field
from dagster_dag_factory.resources.base import BaseConfigurableResource


class AWSResource(BaseConfigurableResource):
    """
    Generic AWS connection resource providing a boto3 session.
    Supports explicit keys, role assumption, and default credential chain.
    """

    mask_fields: ClassVar[List[str]] = BaseConfigurableResource.mask_fields + [
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
    ]

    # Auth: Explicit Keys (with aliases for backward compatibility)
    aws_access_key_id: Optional[str] = Field(
        default=None, alias="access_key", description="AWS Access Key ID"
    )
    aws_secret_access_key: Optional[str] = Field(
        default=None, alias="secret_key", description="AWS Secret Access Key"
    )
    aws_session_token: Optional[str] = Field(
        default=None, alias="session_token", description="AWS Session Token"
    )

    # Auth: Role Assumption
    assume_role_arn: Optional[str] = Field(
        default=None, description="ARN of role to assume"
    )
    aws_web_identity_token_file: Optional[str] = Field(
        default=None, description="Path to web identity token file (IRSA)"
    )
    assume_role_session_name: Optional[str] = Field(
        default=None, description="Session name for assumed role"
    )
    external_id: Optional[str] = Field(
        default=None, description="External ID for cross-account role assumption"
    )

    # Configuration
    region_name: str = Field(default="us-east-1", description="AWS Region")
    endpoint_url: Optional[str] = Field(default=None, description="Custom endpoint URL")
    profile_name: Optional[str] = Field(default=None, description="AWS Profile name")
    verify: bool = Field(default=True, description="Verify SSL certificates")

    def get_session(self) -> boto3.Session:
        """
        Creates a boto3 Session with priority handling and environment fallbacks.
        Supports IRSA (IAM Roles for Service Accounts) automatically.
        """
        # 1. Resolve Region (Fallback to AWS_REGION if not in config)
        region = (
            self.resolve("region_name")
            or os.environ.get("AWS_REGION")
            or os.environ.get("AWS_DEFAULT_REGION")
        )

        session_kwargs = {
            "region_name": region,
            "profile_name": self.resolve("profile_name"),
        }

        # 2. Resolve Keys (Fallback to standard AWS env vars if not in config)
        access_key = self.resolve("aws_access_key_id") or os.environ.get(
            "AWS_ACCESS_KEY_ID"
        )
        secret_key = self.resolve("aws_secret_access_key") or os.environ.get(
            "AWS_SECRET_ACCESS_KEY"
        )
        session_token = self.resolve("aws_session_token") or os.environ.get(
            "AWS_SESSION_TOKEN"
        )

        if access_key and secret_key:
            session_kwargs.update(
                {
                    "aws_access_key_id": access_key,
                    "aws_secret_access_key": secret_key,
                    "aws_session_token": session_token,
                }
            )

        session = boto3.Session(**session_kwargs)

        # 3. Handle Role Assumption (Fallback to AWS_ROLE_ARN for IRSA)
        assume_role_arn = self.resolve("assume_role_arn") or os.environ.get(
            "AWS_ROLE_ARN"
        )

        if assume_role_arn:
            sts_kwargs = {}
            endpoint_url = self.resolve("endpoint_url")
            if endpoint_url:
                sts_kwargs["endpoint_url"] = endpoint_url

            sts_client = session.client("sts", **sts_kwargs)
            role_session_name = (
                self.resolve("assume_role_session_name")
                or os.environ.get("AWS_ROLE_SESSION_NAME")
                or f"dagster-{uuid.uuid4()}"
            )

            web_identity_file = self.resolve(
                "aws_web_identity_token_file"
            ) or os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE")

            if web_identity_file and os.path.exists(web_identity_file):
                with open(web_identity_file, "r") as f:
                    web_identity_token = f.read()
                sts_response = sts_client.assume_role_with_web_identity(
                    RoleArn=assume_role_arn,
                    RoleSessionName=role_session_name,
                    WebIdentityToken=web_identity_token,
                )
            else:
                # Standard role assumption
                assume_kwargs = {
                    "RoleArn": assume_role_arn,
                    "RoleSessionName": role_session_name,
                }
                external_id = self.resolve("external_id") or os.environ.get(
                    "AWS_EXTERNAL_ID"
                )
                if external_id:
                    assume_kwargs["ExternalId"] = external_id

                sts_response = sts_client.assume_role(**assume_kwargs)

            credentials = sts_response["Credentials"]
            return boto3.Session(
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
                region_name=region,
            )

        return session

    def get_client(self, service_name: str, **kwargs) -> Any:
        """Generic client getter for any AWS service."""
        session = self.get_session()
        client_kwargs = {
            "verify": self.resolve("verify"),
        }
        endpoint_url = self.resolve("endpoint_url")
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url

        client_kwargs.update(kwargs)
        return session.client(service_name, **client_kwargs)
