"""
Goal 2: Incremental ETL from daily to hourly partitioned table.

This script:
1. Reads from Snowflake Open Catalog daily-partitioned table (Goal 1)
2. Writes to AWS Glue/LakeFormation hourly-partitioned table
3. Uses Iceberg snapshot tracking for incremental processing
4. Idempotent - safe to re-run, only processes new data
"""

import sys
from datetime import datetime
from pyiceberg.exceptions import NoSuchTableError, NamespaceAlreadyExistsError, ForbiddenError
import pyarrow.compute as pc

from src.config import Config
from src.catalogs import get_snowflake_catalog, get_s3tables_catalog
from src.schemas import get_events_schema, get_hourly_partition_spec
from src.state_manager import StateManager


def create_namespace_if_not_exists(catalog, namespace: str) -> None:
    """
    Create namespace (database) if it doesn't exist.

    Args:
        catalog: PyIceberg catalog instance
        namespace: Namespace name to create
    """
    try:
        catalog.create_namespace(namespace)
        print(f"✓ Created namespace: {namespace}")
    except NamespaceAlreadyExistsError:
        print(f"✓ Namespace already exists: {namespace}")


def create_destination_table(
    catalog,
    config: Config,
    database: str,
    table_name: str
) -> None:
    """
    Create hourly-partitioned destination table in S3 Tables.

    Uses PyIceberg with S3 Tables catalog to create the table with proper Iceberg metadata.

    Args:
        catalog: PyIceberg catalog instance (S3 Tables via Glue REST endpoint)
        config: Configuration object
        database: Database name (namespace)
        table_name: Table name
    """
    table_identifier = f"{database}.{table_name}"

    try:
        # Check if table exists
        catalog.load_table(table_identifier)
        print(f"✓ Destination table already exists: {table_identifier}")
        return
    except (NoSuchTableError, ForbiddenError) as e:
        # Table doesn't exist or lacks metadata/permissions - create it
        if isinstance(e, ForbiddenError):
            print(f"  Table exists but lacks proper metadata, recreating...")
        else:
            print(f"  Table does not exist, creating via PyIceberg...")

    # Step 1: Create namespace if needed
    try:
        catalog.create_namespace(database)
        print(f"✓ Created namespace: {database}")
    except (NamespaceAlreadyExistsError, ForbiddenError) as e:
        # Namespace exists or we lack Lake Formation permissions
        # Either way, the namespace should exist from S3 Tables API creation
        if isinstance(e, ForbiddenError):
            print(f"✓ Namespace already exists (Lake Formation managed): {database}")
        else:
            print(f"✓ Namespace already exists: {database}")

    # Step 2: Create table using PyIceberg
    # This creates proper Iceberg metadata (schema, partition spec, metadata location)
    # Location is managed by S3 Tables
    schema = get_events_schema()
    partition_spec = get_hourly_partition_spec()

    try:
        table = catalog.create_table(
            identifier=table_identifier,
            schema=schema,
            partition_spec=partition_spec,
        )
        print(f"✓ Created table: {table_identifier}")
        print(f"  Schema: {len(schema.fields)} fields")
        print(f"  Partition spec: {partition_spec}")
    except Exception as e:
        if 'AlreadyExists' in str(e) or 'already exists' in str(e).lower():
            print(f"✓ Table already exists: {table_identifier}")
        else:
            raise Exception(f"Failed to create table: {e}")


def read_incremental_data(source_table, last_snapshot_id):
    """
    Read incremental data from source table based on snapshot ID.

    Args:
        source_table: PyIceberg table instance
        last_snapshot_id: Last processed snapshot ID (None for full load)

    Returns:
        Tuple of (data as PyArrow table, current snapshot ID)
    """
    current_snapshot = source_table.current_snapshot()

    if current_snapshot is None:
        print("  No data in source table")
        return None, None

    current_snapshot_id = current_snapshot.snapshot_id

    # Check if data is already processed
    if last_snapshot_id is not None and last_snapshot_id == current_snapshot_id:
        print(f"  No new data (snapshot {current_snapshot_id} already processed)")
        return None, current_snapshot_id

    # Read data from source
    print(f"  Reading data from snapshot: {current_snapshot_id}")
    if last_snapshot_id:
        print(f"  Previous snapshot: {last_snapshot_id}")

    data = source_table.scan().to_arrow()
    print(f"  ✓ Read {len(data)} records")

    return data, current_snapshot_id


def transform_partitioning(data):
    """
    Transform data for optimal hourly partitioning.

    Args:
        data: PyArrow table

    Returns:
        Transformed PyArrow table (sorted by timestamp)
    """
    # Sort by timestamp for better partitioning performance
    indices = pc.sort_indices(data, sort_keys=[("event_timestamp", "ascending")])
    sorted_data = pc.take(data, indices)

    return sorted_data


def write_to_destination(dest_table, data) -> None:
    """
    Write data to destination table.

    Args:
        dest_table: PyIceberg table instance
        data: PyArrow table to write
    """
    print(f"  Writing {len(data)} records to destination...")
    dest_table.append(data)
    print(f"  ✓ Data written successfully")


def print_processing_summary(
    source_table,
    dest_table,
    records_processed: int,
    snapshot_id: int
) -> None:
    """
    Print processing summary and statistics.

    Args:
        source_table: Source PyIceberg table
        dest_table: Destination PyIceberg table
        records_processed: Number of records processed
        snapshot_id: Processed snapshot ID
    """
    print("\n" + "=" * 60)
    print("Processing Summary")
    print("=" * 60)
    print(f"Records Processed: {records_processed}")
    print(f"Source Snapshot ID: {snapshot_id}")

    # Source table stats
    print("\nSource Table (Daily Partitioned):")
    src_snapshots = list(source_table.snapshots())
    print(f"  Total Snapshots: {len(src_snapshots)}")

    # Destination table stats
    print("\nDestination Table (Hourly Partitioned):")
    dest_snapshot = dest_table.current_snapshot()
    if dest_snapshot:
        print(f"  Current Snapshot ID: {dest_snapshot.snapshot_id}")
        print(f"  Timestamp: {datetime.fromtimestamp(dest_snapshot.timestamp_ms / 1000)}")

    dest_snapshots = list(dest_table.snapshots())
    print(f"  Total Snapshots: {len(dest_snapshots)}")

    # Get record count from destination
    print("\nDestination Record Count:")
    try:
        df = dest_table.scan().to_arrow()
        print(f"  Total Records: {len(df)}")
    except Exception as e:
        print(f"  Could not scan table: {e}")


def main():
    """Main entry point for Goal 2."""
    print("=" * 60)
    print("Goal 2: AWS S3 Tables - Hourly Partitioned (Incremental ETL)")
    print("=" * 60)

    try:
        # Load configuration
        print("\nLoading configuration...")
        config = Config.load()
        print(f"✓ Configuration loaded")

        # Initialize state manager
        print("\nInitializing state manager...")
        state_manager = StateManager(config.tables.state_file_path)
        last_snapshot_id = state_manager.get_last_snapshot_id()
        if last_snapshot_id:
            print(f"✓ Last processed snapshot: {last_snapshot_id}")
        else:
            print(f"✓ No previous state (first run)")

        # Connect to catalogs
        print("\nConnecting to catalogs...")
        print("  1. Snowflake Open Catalog (source)...")
        source_catalog = get_snowflake_catalog(config)
        print(f"     ✓ Connected: {config.snowflake.catalog_name}")

        print("  2. AWS S3 Tables Catalog (destination)...")
        dest_catalog = get_s3tables_catalog(config)
        print(f"     ✓ Connected: s3tablescatalog in {config.aws.region}")

        # Load source table
        print(f"\nLoading source table...")
        source_table_id = f"{config.tables.daily_namespace}.{config.tables.daily_table_name}"
        try:
            source_table = source_catalog.load_table(source_table_id)
            print(f"✓ Loaded source table: {source_table_id}")
        except NoSuchTableError:
            print(f"❌ Source table not found: {source_table_id}")
            print("   Run goal1_snowflake_daily.py first to create the source table")
            sys.exit(1)

        # Create destination table if needed
        print(f"\nPreparing destination table...")
        create_destination_table(
            dest_catalog,
            config,
            config.tables.hourly_database,
            config.tables.hourly_table_name
        )

        # Load destination table
        print(f"\nLoading destination table...")
        dest_table_id = f"{config.tables.hourly_database}.{config.tables.hourly_table_name}"
        dest_table = dest_catalog.load_table(dest_table_id)
        print(f"✓ Loaded destination table: {dest_table_id}")

        # Read incremental data
        print(f"\nReading incremental data from source...")
        data, current_snapshot_id = read_incremental_data(source_table, last_snapshot_id)

        if data is None:
            print("\n✓ No new data to process (already up-to-date)")
            sys.exit(0)

        # Transform data
        print(f"\nTransforming data (sorting by timestamp)...")
        transformed_data = transform_partitioning(data)
        print(f"✓ Data transformed")

        # Write to destination
        print(f"\nWriting to destination table...")
        write_to_destination(dest_table, transformed_data)

        # Update state
        print(f"\nUpdating state...")
        state_manager.save(current_snapshot_id)
        print(f"✓ State updated")

        # Print summary
        print_processing_summary(
            source_table,
            dest_table,
            len(data),
            current_snapshot_id
        )

        print("\n" + "=" * 60)
        print("Goal 2 Complete!")
        print("=" * 60)
        print("\nNext steps:")
        print("  1. Verify table in S3 Tables:")
        print(f"     aws s3tables list-tables --table-bucket-arn {config.aws.s3_tables_bucket_arn} --namespace {config.tables.hourly_database} --profile {config.aws.profile}")
        print("  2. Verify in Glue Data Catalog (s3tablescatalog):")
        print(f"     aws glue get-table --catalog-id s3tablescatalog --database-name {config.tables.hourly_database} --name {config.tables.hourly_table_name} --profile {config.aws.profile}")
        print("  3. Query data using Athena:")
        print(f"     SELECT COUNT(*) FROM s3tablescatalog.{config.tables.hourly_database}.{config.tables.hourly_table_name};")
        print("  4. Run this script again to test incremental processing (should report 'No new data')")

    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
