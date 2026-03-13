"""Verify connectivity to AWS and Snowflake Open Catalog."""

import sys
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Add parent directory to path for imports
sys.path.insert(0, '/Users/sharajag/workspace/icelake')

from src.config import Config
from src.catalogs import get_snowflake_catalog, get_glue_catalog


def verify_aws_connectivity(config: Config) -> bool:
    """
    Verify AWS connectivity and permissions.

    Args:
        config: Configuration object

    Returns:
        True if all checks pass, False otherwise
    """
    print("\n" + "=" * 60)
    print("AWS Connectivity Tests")
    print("=" * 60)

    success = True
    session = boto3.Session(profile_name=config.aws.profile, region_name=config.aws.region)

    # Test 1: STS (verify credentials)
    print("\n1. Testing AWS credentials (STS)...")
    try:
        sts = session.client('sts')
        identity = sts.get_caller_identity()
        print(f"   ✓ Authenticated as: {identity['Arn']}")
        print(f"   ✓ Account: {identity['Account']}")
    except (ClientError, NoCredentialsError) as e:
        print(f"   ❌ Failed: {e}")
        success = False

    # Test 2: S3 bucket access
    print("\n2. Testing S3 bucket access...")
    try:
        s3 = session.client('s3')

        # Test daily bucket
        s3.head_bucket(Bucket=config.aws.s3_bucket_daily)
        print(f"   ✓ Can access daily bucket: {config.aws.s3_bucket_daily}")
        response = s3.list_objects_v2(Bucket=config.aws.s3_bucket_daily, MaxKeys=1)
        print(f"   ✓ Can list objects in daily bucket")

        # Test hourly bucket
        s3.head_bucket(Bucket=config.aws.s3_bucket_hourly)
        print(f"   ✓ Can access hourly bucket: {config.aws.s3_bucket_hourly}")
        response = s3.list_objects_v2(Bucket=config.aws.s3_bucket_hourly, MaxKeys=1)
        print(f"   ✓ Can list objects in hourly bucket")
    except ClientError as e:
        print(f"   ❌ Failed: {e}")
        success = False

    # Test 3: Glue access
    print("\n3. Testing AWS Glue access...")
    try:
        glue = session.client('glue')
        databases = glue.get_databases()
        print(f"   ✓ Can access Glue (found {len(databases['DatabaseList'])} databases)")
    except ClientError as e:
        print(f"   ❌ Failed: {e}")
        success = False

    # Test 4: LakeFormation access
    print("\n4. Testing AWS LakeFormation access...")
    try:
        lakeformation = session.client('lakeformation')
        resources = lakeformation.list_resources()
        print(f"   ✓ Can access LakeFormation (found {len(resources.get('ResourceInfoList', []))} resources)")
    except ClientError as e:
        print(f"   ❌ Failed: {e}")
        success = False

    return success


def verify_snowflake_connectivity(config: Config) -> bool:
    """
    Verify Snowflake Open Catalog connectivity.

    Args:
        config: Configuration object

    Returns:
        True if check passes, False otherwise
    """
    print("\n" + "=" * 60)
    print("Snowflake Open Catalog Connectivity Tests")
    print("=" * 60)

    print("\n1. Testing Snowflake Open Catalog connection...")
    try:
        catalog = get_snowflake_catalog(config)
        namespaces = list(catalog.list_namespaces())
        print(f"   ✓ Connected to Snowflake Open Catalog")
        print(f"   ✓ Found {len(namespaces)} namespaces")
        if namespaces:
            print(f"   Namespaces: {', '.join([str(ns) for ns in namespaces[:5]])}")
        return True
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False


def verify_glue_catalog(config: Config) -> bool:
    """
    Verify AWS Glue catalog connectivity via PyIceberg.

    Args:
        config: Configuration object

    Returns:
        True if check passes, False otherwise
    """
    print("\n" + "=" * 60)
    print("AWS Glue Catalog (PyIceberg) Connectivity Tests")
    print("=" * 60)

    print("\n1. Testing Glue catalog via PyIceberg...")
    try:
        catalog = get_glue_catalog(config)
        namespaces = list(catalog.list_namespaces())
        print(f"   ✓ Connected to Glue catalog via PyIceberg")
        print(f"   ✓ Found {len(namespaces)} namespaces")
        if namespaces:
            print(f"   Namespaces: {', '.join([str(ns) for ns in namespaces[:5]])}")
        return True
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False


def main():
    """Main entry point."""
    try:
        config = Config.load()

        # Run all verification tests
        aws_ok = verify_aws_connectivity(config)
        snowflake_ok = verify_snowflake_connectivity(config)
        glue_ok = verify_glue_catalog(config)

        # Summary
        print("\n" + "=" * 60)
        print("Verification Summary")
        print("=" * 60)
        print(f"AWS Connectivity: {'✓ PASS' if aws_ok else '❌ FAIL'}")
        print(f"Snowflake Open Catalog: {'✓ PASS' if snowflake_ok else '❌ FAIL'}")
        print(f"Glue Catalog (PyIceberg): {'✓ PASS' if glue_ok else '❌ FAIL'}")

        if aws_ok and snowflake_ok and glue_ok:
            print("\n✓ All connectivity tests passed!")
            sys.exit(0)
        else:
            print("\n❌ Some connectivity tests failed. Please check configuration.")
            sys.exit(1)

    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
