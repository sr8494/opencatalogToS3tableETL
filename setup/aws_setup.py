"""AWS infrastructure setup for Glue database and LakeFormation."""

import sys
import boto3
from botocore.exceptions import ClientError

# Add parent directory to path for imports
sys.path.insert(0, '/Users/sharajag/workspace/icelake')

from src.config import Config


def create_glue_database(glue_client, database_name: str, s3_location: str) -> bool:
    """
    Create Glue database if it doesn't exist.

    Args:
        glue_client: Boto3 Glue client
        database_name: Name of the database to create
        s3_location: S3 location for the database

    Returns:
        True if created, False if already exists
    """
    try:
        glue_client.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'Iceberg hourly partitioned events table database',
                'LocationUri': s3_location,
            }
        )
        print(f"✓ Created Glue database: {database_name}")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'AlreadyExistsException':
            print(f"✓ Glue database already exists: {database_name}")
            return False
        else:
            raise


def register_s3_location(lakeformation_client, s3_location: str, role_arn: str) -> None:
    """
    Register S3 location with LakeFormation.

    Args:
        lakeformation_client: Boto3 LakeFormation client
        s3_location: S3 location to register
        role_arn: IAM role ARN for LakeFormation
    """
    try:
        lakeformation_client.register_resource(
            ResourceArn=s3_location,
            UseServiceLinkedRole=False,
            RoleArn=role_arn,
        )
        print(f"✓ Registered S3 location with LakeFormation: {s3_location}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'AlreadyExistsException':
            print(f"✓ S3 location already registered with LakeFormation: {s3_location}")
        else:
            print(f"Warning: Could not register S3 location: {e}")


def grant_database_permissions(
    lakeformation_client,
    database_name: str,
    principal_arn: str
) -> None:
    """
    Grant LakeFormation permissions on database.

    Args:
        lakeformation_client: Boto3 LakeFormation client
        database_name: Name of the database
        principal_arn: IAM principal ARN to grant permissions to
    """
    try:
        lakeformation_client.grant_permissions(
            Principal={'DataLakePrincipalIdentifier': principal_arn},
            Resource={'Database': {'Name': database_name}},
            Permissions=['CREATE_TABLE', 'ALTER', 'DROP', 'DESCRIBE'],
        )
        print(f"✓ Granted database permissions to: {principal_arn}")
    except ClientError as e:
        if 'already exists' in str(e).lower() or 'duplicate' in str(e).lower():
            print(f"✓ Database permissions already granted")
        else:
            print(f"Warning: Could not grant database permissions: {e}")


def setup_aws_infrastructure(config: Config) -> None:
    """
    Set up AWS infrastructure for the project.

    Args:
        config: Configuration object
    """
    print("=" * 60)
    print("AWS Infrastructure Setup")
    print("=" * 60)

    # Create boto3 clients
    session = boto3.Session(profile_name=config.aws.profile, region_name=config.aws.region)
    glue_client = session.client('glue')
    lakeformation_client = session.client('lakeformation')

    database_name = config.tables.hourly_database
    s3_location = f"s3://{config.aws.s3_bucket_hourly}/{database_name}/"

    print(f"\nDatabase: {database_name}")
    print(f"S3 Location: {s3_location}")
    print(f"IAM Role (Glue): {config.aws.glue_iam_role_arn}")
    print()

    # Step 1: Create Glue database
    print("Step 1: Creating Glue database...")
    create_glue_database(glue_client, database_name, s3_location)

    # Step 2: Register S3 location with LakeFormation
    print("\nStep 2: Registering S3 location with LakeFormation...")
    register_s3_location(
        lakeformation_client,
        f"arn:aws:s3:::{config.aws.s3_bucket_hourly}/{database_name}/",
        config.aws.glue_iam_role_arn
    )

    # Step 3: Grant LakeFormation permissions
    print("\nStep 3: Granting LakeFormation permissions...")
    grant_database_permissions(
        lakeformation_client,
        database_name,
        config.aws.glue_iam_role_arn
    )

    print("\n" + "=" * 60)
    print("AWS Infrastructure Setup Complete!")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Run: python setup/verify_connections.py")
    print("  2. Run: python goal1_snowflake_daily.py")
    print("  3. Run: python goal2_s3tables_hourly.py")


def main():
    """Main entry point."""
    try:
        config = Config.load()
        setup_aws_infrastructure(config)
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
