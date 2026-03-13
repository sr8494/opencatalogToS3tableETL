"""Configuration management for the Iceberg demo project."""

import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv


@dataclass
class AWSConfig:
    """AWS-related configuration."""
    profile: str
    region: str
    account_id: str
    s3_bucket_daily: str  # For Goal 1 (Snowflake Open Catalog)
    s3_bucket_hourly: str  # For Goal 2 (AWS Glue)
    s3_tables_bucket_name: str  # S3 Tables bucket (alternative for Goal 2)
    s3_tables_bucket_arn: str   # Full ARN for S3 Tables bucket
    iam_role_arn: str  # For Snowflake Open Catalog
    glue_iam_role_arn: str  # For Glue and LakeFormation
    external_id: str

    @classmethod
    def from_env(cls) -> 'AWSConfig':
        """Load AWS config from environment variables."""
        return cls(
            profile=cls._get_required_env('AWS_PROFILE'),
            region=cls._get_required_env('AWS_REGION'),
            account_id=cls._get_required_env('AWS_ACCOUNT_ID'),
            s3_bucket_daily=cls._get_required_env('S3_BUCKET_DAILY'),
            s3_bucket_hourly=cls._get_optional_env('S3_BUCKET_HOURLY', ''),  # Optional, only for regular S3
            s3_tables_bucket_name=cls._get_required_env('S3_TABLES_BUCKET_NAME'),
            s3_tables_bucket_arn=cls._get_required_env('S3_TABLES_BUCKET_ARN'),
            iam_role_arn=cls._get_required_env('IAM_ROLE_ARN'),
            glue_iam_role_arn=cls._get_required_env('GLUE_IAM_ROLE_ARN'),
            external_id=cls._get_required_env('EXTERNAL_ID'),
        )

    @staticmethod
    def _get_required_env(key: str) -> str:
        """Get required environment variable or raise error."""
        value = os.getenv(key)
        if not value:
            raise ValueError(f"Required environment variable '{key}' is not set")
        return value

    @staticmethod
    def _get_optional_env(key: str, default: str = '') -> str:
        """Get optional environment variable or return default."""
        return os.getenv(key, default)


@dataclass
class SnowflakeConfig:
    """Snowflake Open Catalog configuration (OAuth)."""
    account: str
    client_id: str
    client_secret: str
    catalog_name: str
    catalog_uri: str

    @classmethod
    def from_env(cls) -> 'SnowflakeConfig':
        """Load Snowflake config from environment variables."""
        return cls(
            account=cls._get_required_env('SNOWFLAKE_ACCOUNT'),
            client_id=cls._get_required_env('SNOWFLAKE_CLIENT_ID'),
            client_secret=cls._get_required_env('SNOWFLAKE_CLIENT_SECRET'),
            catalog_name=cls._get_required_env('SNOWFLAKE_CATALOG_NAME'),
            catalog_uri=cls._get_required_env('SNOWFLAKE_CATALOG_URI'),
        )

    @staticmethod
    def _get_required_env(key: str) -> str:
        """Get required environment variable or raise error."""
        value = os.getenv(key)
        if not value:
            raise ValueError(f"Required environment variable '{key}' is not set")
        return value


@dataclass
class TableConfig:
    """Table naming and location configuration."""
    daily_namespace: str
    daily_table_name: str
    hourly_database: str
    hourly_table_name: str
    state_file_path: str

    @classmethod
    def from_env(cls) -> 'TableConfig':
        """Load table config from environment variables."""
        return cls(
            daily_namespace=cls._get_required_env('DAILY_TABLE_NAMESPACE'),
            daily_table_name=cls._get_required_env('DAILY_TABLE_NAME'),
            hourly_database=cls._get_required_env('HOURLY_TABLE_DATABASE'),
            hourly_table_name=cls._get_required_env('HOURLY_TABLE_NAME'),
            state_file_path=cls._get_required_env('STATE_FILE_PATH'),
        )

    @staticmethod
    def _get_required_env(key: str) -> str:
        """Get required environment variable or raise error."""
        value = os.getenv(key)
        if not value:
            raise ValueError(f"Required environment variable '{key}' is not set")
        return value


@dataclass
class Config:
    """Main configuration container."""
    aws: AWSConfig
    snowflake: SnowflakeConfig
    tables: TableConfig

    @classmethod
    def load(cls, env_file: Optional[str] = '.env') -> 'Config':
        """
        Load configuration from environment.

        Args:
            env_file: Path to .env file (default: '.env')

        Returns:
            Config object with all settings loaded

        Raises:
            ValueError: If required environment variables are missing
        """
        # Load .env file if it exists
        if env_file and os.path.exists(env_file):
            load_dotenv(env_file)

        try:
            return cls(
                aws=AWSConfig.from_env(),
                snowflake=SnowflakeConfig.from_env(),
                tables=TableConfig.from_env(),
            )
        except ValueError as e:
            raise ValueError(
                f"Configuration error: {e}\n"
                "Please ensure all required variables are set in .env file. "
                "See .env.template for reference."
            ) from e
