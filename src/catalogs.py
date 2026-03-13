"""Catalog connection factory for Snowflake Open Catalog and AWS Glue."""

from pyiceberg.catalog import Catalog, load_catalog
from src.config import Config


def get_snowflake_catalog(config: Config) -> Catalog:
    """
    Get PyIceberg catalog for Snowflake Open Catalog using OAuth.

    Args:
        config: Configuration object

    Returns:
        Configured PyIceberg catalog for Snowflake

    Raises:
        Exception: If catalog connection fails
    """
    try:
        catalog = load_catalog(
            config.snowflake.catalog_name,
            **{
                'type': 'rest',
                'uri': config.snowflake.catalog_uri,
                'credential': f'{config.snowflake.client_id}:{config.snowflake.client_secret}',
                'warehouse': config.snowflake.catalog_name,
                'scope': 'PRINCIPAL_ROLE:ALL',
                'header.X-Iceberg-Access-Delegation': 'vended-credentials',
                'token-refresh-enabled': 'true',
            }
        )
        return catalog
    except Exception as e:
        raise Exception(
            f"Failed to connect to Snowflake Open Catalog: {e}\n"
            "Troubleshooting:\n"
            "  1. Verify SNOWFLAKE_CATALOG_URI is correct\n"
            "  2. Check SNOWFLAKE_CLIENT_ID and SNOWFLAKE_CLIENT_SECRET\n"
            "  3. Ensure the catalog exists in Snowflake Open Catalog\n"
            "  4. Verify the OAuth connection has access to the catalog\n"
            "  5. Check that scope 'PRINCIPAL_ROLE:ALL' is valid for your connection"
        ) from e


def get_glue_catalog(config: Config) -> Catalog:
    """
    Get PyIceberg catalog for AWS Glue (default catalog).

    Args:
        config: Configuration object

    Returns:
        Configured PyIceberg catalog for AWS Glue

    Raises:
        Exception: If catalog connection fails
    """
    try:
        catalog = load_catalog(
            'glue',
            **{
                'type': 'glue',
                'glue.region': config.aws.region,
                's3.region': config.aws.region,
            }
        )
        return catalog
    except Exception as e:
        raise Exception(
            f"Failed to connect to AWS Glue catalog: {e}\n"
            "Troubleshooting:\n"
            "  1. Verify AWS credentials are configured (profile: {config.aws.profile})\n"
            "  2. Check AWS_REGION is correct\n"
            "  3. Ensure IAM permissions for Glue and S3\n"
            "  4. Run setup/aws_setup.py to create Glue database"
        ) from e


def get_s3tables_catalog(config: Config) -> Catalog:
    """
    Get PyIceberg catalog for AWS S3 Tables via Glue Iceberg REST endpoint.

    This uses the AWS Glue Iceberg REST API to access S3 Tables. When you write
    data through this endpoint, PyIceberg creates proper Iceberg metadata.

    Args:
        config: Configuration object

    Returns:
        Configured PyIceberg catalog for S3 Tables

    Raises:
        Exception: If catalog connection fails
    """
    try:
        # Extract bucket name from ARN
        # ARN format: arn:aws:s3tables:region:account:bucket/bucket-name
        bucket_name = config.aws.s3_tables_bucket_arn.split('/')[-1]

        catalog = load_catalog(
            's3tablescatalog',
            **{
                'type': 'rest',
                'uri': f'https://glue.{config.aws.region}.amazonaws.com/iceberg',
                'warehouse': f'{config.aws.account_id}:s3tablescatalog/{bucket_name}',  # Must be prefixed with account ID
                'rest.sigv4-enabled': 'true',
                'rest.signing-name': 'glue',
                'rest.signing-region': config.aws.region,
            }
        )
        return catalog
    except Exception as e:
        raise Exception(
            f"Failed to connect to S3 Tables catalog via Glue REST endpoint: {e}\n"
            "Troubleshooting:\n"
            "  1. Enable AWS analytics integration for S3 Tables in AWS Console\n"
            "     (S3 → Table buckets → Integration with AWS analytics services)\n"
            "  2. Verify S3 Tables bucket exists: {config.aws.s3_tables_bucket_name}\n"
            "  3. Check IAM permissions for glue:* and s3tables:* actions\n"
            "  4. Ensure AWS credentials are configured properly\n"
            "  5. Verify the Glue Iceberg REST endpoint is accessible"
        ) from e
