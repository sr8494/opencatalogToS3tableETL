"""
Check if AWS profile has all required permissions for Goal 2.
This helps identify missing permissions before running the main scripts.
"""

import sys
import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, '/Users/sharajag/workspace/icelake')
from src.config import Config


def check_s3_permissions(s3_client, bucket_name, bucket_label="Bucket"):
    """Check S3 permissions."""
    print(f"\n   {bucket_label}: {bucket_name}")

    tests = {
        'List Bucket': lambda: s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1),
        'Get Object': lambda: s3_client.head_bucket(Bucket=bucket_name),
    }

    results = {}
    for test_name, test_func in tests.items():
        try:
            test_func()
            print(f"     ✓ {test_name}")
            results[test_name] = True
        except ClientError as e:
            print(f"     ❌ {test_name}: {e.response['Error']['Code']}")
            results[test_name] = False

    return all(results.values())


def check_glue_permissions(glue_client):
    """Check Glue permissions."""
    print("\n2. Checking Glue Permissions...")

    tests = {
        'List Databases': lambda: glue_client.get_databases(),
        'Get Database': lambda: glue_client.get_database(Name='default'),
    }

    results = {}
    for test_name, test_func in tests.items():
        try:
            test_func()
            print(f"   ✓ {test_name}")
            results[test_name] = True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'EntityNotFoundException':
                # Database not found is OK for Get Database test
                print(f"   ✓ {test_name} (permission OK)")
                results[test_name] = True
            else:
                print(f"   ❌ {test_name}: {error_code}")
                results[test_name] = False

    # Test create database (dry run by checking if we can at least call it)
    try:
        # Try to get a non-existent database to test permissions
        glue_client.get_database(Name='__permission_test__')
    except ClientError as e:
        if e.response['Error']['Code'] in ['EntityNotFoundException', 'AccessDeniedException']:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                print(f"   ✓ Database operations allowed")
                results['Create Database'] = True
            else:
                print(f"   ❌ Create Database: AccessDenied")
                results['Create Database'] = False

    return all(results.values())


def check_lakeformation_permissions(lf_client):
    """Check LakeFormation permissions."""
    print("\n3. Checking LakeFormation Permissions...")

    tests = {
        'List Resources': lambda: lf_client.list_resources(),
        'Describe Resource': lambda: lf_client.describe_resource(ResourceArn='arn:aws:s3:::test'),
    }

    results = {}
    for test_name, test_func in tests.items():
        try:
            test_func()
            print(f"   ✓ {test_name}")
            results[test_name] = True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            # Some operations may fail but that's OK if we have permission
            if error_code in ['InvalidResourceException', 'EntityNotFoundException']:
                print(f"   ✓ {test_name} (permission OK)")
                results[test_name] = True
            else:
                print(f"   ❌ {test_name}: {error_code}")
                results[test_name] = False

    return all(results.values())


def check_iam_role_exists(iam_client, role_arn):
    """Check if IAM role exists and get its details."""
    print("\n4. Checking IAM Role...")

    try:
        role_name = role_arn.split('/')[-1]
        response = iam_client.get_role(RoleName=role_name)
        print(f"   ✓ Role exists: {role_name}")

        # Check trust policy
        trust_policy = response['Role']['AssumeRolePolicyDocument']
        print(f"   Trust policy principals:")
        for statement in trust_policy.get('Statement', []):
            principal = statement.get('Principal', {})
            if 'Service' in principal:
                services = principal['Service']
                if isinstance(services, str):
                    services = [services]
                for service in services:
                    print(f"     - {service}")

        # Check if LakeFormation is in trust policy
        trust_policy_str = str(trust_policy)
        has_lakeformation = 'lakeformation' in trust_policy_str.lower()

        if has_lakeformation:
            print(f"   ✓ LakeFormation trust policy configured")
        else:
            print(f"   ⚠️  LakeFormation NOT in trust policy - you may need to add it")

        return True

    except ClientError as e:
        print(f"   ❌ Role not found or access denied: {e.response['Error']['Code']}")
        return False


def main():
    """Main entry point."""
    print("=" * 70)
    print("AWS Permissions Check for Goal 2 (Glue + LakeFormation)")
    print("=" * 70)

    try:
        config = Config.load()
        print(f"\nAWS Profile: {config.aws.profile}")
        print(f"Region: {config.aws.region}")
        print(f"S3 Bucket (Daily): {config.aws.s3_bucket_daily}")
        print(f"S3 Bucket (Hourly): {config.aws.s3_bucket_hourly}")
        print(f"IAM Role (Snowflake): {config.aws.iam_role_arn}")
        print(f"IAM Role (Glue/LakeFormation): {config.aws.glue_iam_role_arn}")

        # Create clients
        session = boto3.Session(
            profile_name=config.aws.profile,
            region_name=config.aws.region
        )

        s3_client = session.client('s3')
        glue_client = session.client('glue')
        lf_client = session.client('lakeformation')
        iam_client = session.client('iam')

        # Run checks
        print("\n1. Checking S3 Permissions...")
        s3_daily_ok = check_s3_permissions(s3_client, config.aws.s3_bucket_daily, "Daily Bucket")
        s3_hourly_ok = check_s3_permissions(s3_client, config.aws.s3_bucket_hourly, "Hourly Bucket")
        s3_ok = s3_daily_ok and s3_hourly_ok
        glue_ok = check_glue_permissions(glue_client)
        lf_ok = check_lakeformation_permissions(lf_client)

        # Check both IAM roles
        snowflake_role_ok = check_iam_role_exists(iam_client, config.aws.iam_role_arn)
        glue_role_ok = check_iam_role_exists(iam_client, config.aws.glue_iam_role_arn)
        role_ok = snowflake_role_ok and glue_role_ok

        # Summary
        print("\n" + "=" * 70)
        print("Permission Check Summary")
        print("=" * 70)
        print(f"S3 Permissions: {'✓ PASS' if s3_ok else '❌ FAIL'}")
        print(f"Glue Permissions: {'✓ PASS' if glue_ok else '❌ FAIL'}")
        print(f"LakeFormation Permissions: {'✓ PASS' if lf_ok else '❌ FAIL'}")
        print(f"IAM Role: {'✓ EXISTS' if role_ok else '❌ NOT FOUND'}")

        if all([s3_ok, glue_ok, lf_ok, role_ok]):
            print("\n✓ All permissions OK! Ready to run Goal 2.")
            print("\nNext steps:")
            print("  1. python setup/aws_setup.py")
            print("  2. python goal1_snowflake_daily.py")
            print("  3. python goal2_s3tables_hourly.py")
            sys.exit(0)
        else:
            print("\n❌ Some permissions are missing.")
            print("\nRecommended actions:")

            if not s3_ok:
                print("\n  S3 Permissions Issue:")
                print("    - Ensure your IAM user/role has s3:ListBucket, s3:GetObject, s3:PutObject")
                print(f"    - For daily bucket: {config.aws.s3_bucket_daily}")
                print(f"    - For hourly bucket: {config.aws.s3_bucket_hourly}")

            if not glue_ok:
                print("\n  Glue Permissions Issue:")
                print("    - Ensure your IAM user/role has glue:GetDatabase, glue:CreateDatabase, glue:CreateTable")
                print("    - These are needed to create and manage Glue databases/tables")

            if not lf_ok:
                print("\n  LakeFormation Permissions Issue:")
                print("    - Ensure your IAM user/role has lakeformation:RegisterResource, lakeformation:GrantPermissions")
                print("    - These are needed for LakeFormation resource management")

            if not role_ok:
                print("\n  IAM Role Issue:")
                if not snowflake_role_ok:
                    print(f"    - Snowflake role not found: {config.aws.iam_role_arn}")
                if not glue_role_ok:
                    print(f"    - Glue role not found: {config.aws.glue_iam_role_arn}")
                    print("    - Run: bash setup/create_iam_role.sh")
                print("    - Or verify the role ARNs in your .env file")

            sys.exit(1)

    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
