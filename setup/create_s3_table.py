#!/usr/bin/env python
"""
Create Iceberg table in S3 Tables using AWS API.
After creation, PyIceberg can read/write via Glue catalog.
"""

import sys
import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, '/Users/sharajag/workspace/icelake')
from src.config import Config


def create_table_in_s3tables(config: Config):
    """Create table in S3 Tables using AWS API."""

    session = boto3.Session(
        profile_name=config.aws.profile,
        region_name=config.aws.region
    )
    s3tables_client = session.client('s3tables')

    table_bucket_arn = config.aws.s3_tables_bucket_arn
    namespace = config.tables.hourly_database
    table_name = config.tables.hourly_table_name

    print("=" * 70)
    print("Creating Table in S3 Tables")
    print("=" * 70)
    print(f"\nTable Bucket ARN: {table_bucket_arn}")
    print(f"Namespace: {namespace}")
    print(f"Table Name: {table_name}")

    # Step 1: Check if namespace exists, create if not
    # S3 Tables API namespace parameter format:
    # - get_namespace: expects string
    # - create_namespace: expects list (supports hierarchical namespaces)
    print(f"\nStep 1: Checking namespace...")
    try:
        response = s3tables_client.get_namespace(
            tableBucketARN=table_bucket_arn,
            namespace=namespace  # String for get_namespace
        )
        print(f"✓ Namespace exists: {namespace}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NotFoundException':
            print(f"  Creating namespace: {namespace}")
            try:
                s3tables_client.create_namespace(
                    tableBucketARN=table_bucket_arn,
                    namespace=[namespace]  # List for create_namespace
                )
                print(f"✓ Created namespace: {namespace}")
            except ClientError as create_error:
                print(f"❌ Failed to create namespace: {create_error}")
                sys.exit(1)
        else:
            print(f"❌ Error checking namespace: {e}")
            sys.exit(1)

    # Step 2: Check if table exists
    print(f"\nStep 2: Checking if table exists...")
    try:
        response = s3tables_client.get_table_metadata_location(
            tableBucketARN=table_bucket_arn,
            namespace=namespace,  # String for get_table_metadata_location
            name=table_name
        )
        print(f"✓ Table already exists: {namespace}.{table_name}")
        print(f"  Metadata location: {response.get('metadataLocation', 'N/A')}")
        print("\nTable is ready for PyIceberg to use!")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'NotFoundException':
            print(f"  Table does not exist, creating...")
        else:
            print(f"❌ Error checking table: {e}")
            sys.exit(1)

    # Step 3: Create table
    print(f"\nStep 3: Creating table in S3 Tables...")
    try:
        response = s3tables_client.create_table(
            tableBucketARN=table_bucket_arn,
            namespace=namespace,  # String for create_table
            name=table_name,
            format='ICEBERG'
        )

        print(f"✓ Created table: {namespace}.{table_name}")
        print(f"  Table ARN: {response.get('tableARN', 'N/A')}")

        # Get metadata location
        try:
            meta_response = s3tables_client.get_table_metadata_location(
                tableBucketARN=table_bucket_arn,
                namespace=namespace,  # String for get_table_metadata_location
                name=table_name
            )
            print(f"  Metadata location: {meta_response.get('metadataLocation', 'N/A')}")
        except:
            pass

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'ConflictException' or 'AlreadyExists' in str(e):
            # Table already exists - idempotent operation
            print(f"✓ Table already exists: {namespace}.{table_name}")
        else:
            print(f"❌ Failed to create table: {e}")
            sys.exit(1)

    print("\n" + "=" * 70)
    print("Table Created Successfully!")
    print("=" * 70)
    print("\nNext steps:")
    print("  1. Verify in Glue Data Catalog:")
    print(f"     aws glue get-table --catalog-id s3tablescatalog --database-name {namespace} --name {table_name}")
    print("  2. Run Goal 2 to write data:")
    print("     python goal2_s3tables_hourly.py")

    return True


def main():
    """Main entry point."""
    try:
        config = Config.load()
        create_table_in_s3tables(config)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
