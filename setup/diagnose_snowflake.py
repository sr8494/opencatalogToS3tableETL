"""
Diagnostic script for Snowflake Open Catalog connectivity.
Tests different authentication methods to find the right one.
"""

import sys
import os

sys.path.insert(0, '/Users/sharajag/workspace/icelake')

from src.config import Config
from pyiceberg.catalog import load_catalog


def test_basic_auth(config):
    """Test with basic auth (username:password)."""
    print("\n1. Testing Basic Auth (username:password)...")
    try:
        catalog = load_catalog(
            config.snowflake.catalog_name,
            **{
                'type': 'rest',
                'uri': config.snowflake.catalog_uri,
                'credential': f'{config.snowflake.user}:{config.snowflake.password}',
                'warehouse': f's3://{config.aws.s3_bucket_daily}',
            }
        )
        namespaces = list(catalog.list_namespaces())
        print(f"   ✓ Success! Found {len(namespaces)} namespaces")
        return True
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False


def test_token_auth(config):
    """Test with token-based auth."""
    print("\n2. Testing Token Auth...")
    try:
        catalog = load_catalog(
            config.snowflake.catalog_name,
            **{
                'type': 'rest',
                'uri': config.snowflake.catalog_uri,
                'token': f'{config.snowflake.user}:{config.snowflake.password}',
                'warehouse': f's3://{config.aws.s3_bucket_daily}',
            }
        )
        namespaces = list(catalog.list_namespaces())
        print(f"   ✓ Success! Found {len(namespaces)} namespaces")
        return True
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False


def test_oauth_auth(config):
    """Test with OAuth client credentials."""
    print("\n3. Testing OAuth Client Credentials...")
    try:
        catalog = load_catalog(
            config.snowflake.catalog_name,
            **{
                'type': 'rest',
                'uri': config.snowflake.catalog_uri,
                'oauth2-server-uri': f"{config.snowflake.catalog_uri.replace('/polaris/api/catalog', '')}/oauth/token",
                'credential': f'{config.snowflake.user}:{config.snowflake.password}',
                'scope': 'PRINCIPAL_ROLE:ALL',
                'warehouse': f's3://{config.aws.s3_bucket_daily}',
            }
        )
        namespaces = list(catalog.list_namespaces())
        print(f"   ✓ Success! Found {len(namespaces)} namespaces")
        return True
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False


def test_header_auth(config):
    """Test with header-based auth."""
    print("\n4. Testing Header Auth...")
    try:
        catalog = load_catalog(
            config.snowflake.catalog_name,
            **{
                'type': 'rest',
                'uri': config.snowflake.catalog_uri,
                'header.Authorization': f'Basic {config.snowflake.user}:{config.snowflake.password}',
                'warehouse': f's3://{config.aws.s3_bucket_daily}',
            }
        )
        namespaces = list(catalog.list_namespaces())
        print(f"   ✓ Success! Found {len(namespaces)} namespaces")
        return True
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False


def test_snowflake_connector(config):
    """Test using Snowflake Python connector directly."""
    print("\n5. Testing Snowflake Connector (Direct)...")
    try:
        import snowflake.connector

        conn = snowflake.connector.connect(
            account=config.snowflake.account,
            user=config.snowflake.user,
            password=config.snowflake.password,
            warehouse=config.snowflake.warehouse,
        )

        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()
        print(f"   ✓ Connected! Snowflake version: {version[0]}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False


def check_catalog_in_ui(config):
    """Provide manual steps to check catalog."""
    print("\n6. Manual Verification Steps...")
    print("   Please verify in Snowflake Open Catalog UI:")
    print(f"   1. Log in to: {config.snowflake.catalog_uri.replace('/polaris/api/catalog', '')}")
    print(f"   2. Look for catalog: {config.snowflake.catalog_name}")
    print("   3. Check if catalog has 'REST API' access enabled")
    print("   4. Verify your user has permissions to the catalog")
    print()


def main():
    """Main diagnostic entry point."""
    print("=" * 70)
    print("Snowflake Open Catalog Diagnostic Tool")
    print("=" * 70)

    try:
        config = Config.load()

        print("\nConfiguration:")
        print(f"  Account: {config.snowflake.account}")
        print(f"  User: {config.snowflake.user}")
        print(f"  Catalog Name: {config.snowflake.catalog_name}")
        print(f"  Catalog URI: {config.snowflake.catalog_uri}")
        print(f"  Warehouse (S3): {config.aws.s3_bucket_daily}")

        # Run all tests
        results = []
        results.append(('Basic Auth', test_basic_auth(config)))
        results.append(('Token Auth', test_token_auth(config)))
        results.append(('OAuth', test_oauth_auth(config)))
        results.append(('Header Auth', test_header_auth(config)))
        results.append(('Snowflake Connector', test_snowflake_connector(config)))

        # Check catalog manually
        check_catalog_in_ui(config)

        # Summary
        print("\n" + "=" * 70)
        print("Test Results Summary")
        print("=" * 70)

        for name, result in results:
            status = "✓ PASS" if result else "❌ FAIL"
            print(f"{name:25} {status}")

        successful = [name for name, result in results if result]

        if successful:
            print(f"\n✓ Working authentication method: {successful[0]}")
            print("\nUpdate src/catalogs.py to use this method.")
        else:
            print("\n❌ No authentication method worked.")
            print("\nPossible issues:")
            print("  1. Catalog doesn't exist or name is wrong")
            print("  2. User doesn't have permissions")
            print("  3. Snowflake Open Catalog requires different auth")
            print("  4. Catalog URI is incorrect")
            print("\nNext steps:")
            print("  1. Log in to Snowflake Open Catalog UI")
            print("  2. Verify catalog exists and you have access")
            print("  3. Check catalog API documentation")
            print("  4. Consider using Snowflake connector instead of REST")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
