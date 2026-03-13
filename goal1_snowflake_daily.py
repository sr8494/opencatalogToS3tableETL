"""
Goal 1: Create daily-partitioned Iceberg table using Snowflake Open Catalog.

This script:
1. Creates a namespace (database) in Snowflake Open Catalog
2. Creates an Iceberg table with daily partitioning
3. Generates sample event data for the past 7 days
4. Ingests data into the table
"""

import sys
from datetime import datetime, timedelta
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

from src.config import Config
from src.catalogs import get_snowflake_catalog
from src.schemas import get_events_schema, get_daily_partition_spec
from src.data_generator import generate_sample_events


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


def create_table_if_not_exists(
    catalog,
    namespace: str,
    table_name: str,
    location: str
) -> None:
    """
    Create Iceberg table with daily partitioning if it doesn't exist.

    Args:
        catalog: PyIceberg catalog instance
        namespace: Namespace name
        table_name: Table name
        location: S3 location for table data
    """
    table_identifier = f"{namespace}.{table_name}"

    try:
        # Check if table exists
        catalog.load_table(table_identifier)
        print(f"✓ Table already exists: {table_identifier}")
    except NoSuchTableError:
        # Create table
        schema = get_events_schema()
        partition_spec = get_daily_partition_spec()

        catalog.create_table(
            identifier=table_identifier,
            schema=schema,
            location=location,
            partition_spec=partition_spec,
        )
        print(f"✓ Created table: {table_identifier}")
        print(f"  Schema: {len(schema.fields)} fields")
        print(f"  Partition spec: {partition_spec}")
        print(f"  Location: {location}")


def ingest_daily_data(
    table,
    start_date: datetime,
    num_days: int,
    events_per_day: int
) -> int:
    """
    Generate and ingest sample data for multiple days.

    Args:
        table: PyIceberg table instance
        start_date: Start date for data generation
        num_days: Number of days to generate data for
        events_per_day: Number of events per day

    Returns:
        Total number of events ingested
    """
    total_events = 0

    print(f"\nIngesting data for {num_days} days ({events_per_day} events/day)...")

    for day_offset in range(num_days):
        current_date = start_date + timedelta(days=day_offset)
        next_date = current_date + timedelta(days=1)

        # Generate events for this day
        events = generate_sample_events(
            num_events=events_per_day,
            start_date=current_date,
            end_date=next_date
        )

        # Append to table
        table.append(events)
        total_events += len(events)

        print(f"  ✓ Day {day_offset + 1}/{num_days}: {current_date.date()} - {len(events)} events")

    return total_events


def print_table_stats(table) -> None:
    """
    Print table statistics.

    Args:
        table: PyIceberg table instance
    """
    print("\n" + "=" * 60)
    print("Table Statistics")
    print("=" * 60)

    # Get current snapshot
    current_snapshot = table.current_snapshot()
    if current_snapshot:
        print(f"Current Snapshot ID: {current_snapshot.snapshot_id}")
        print(f"Timestamp: {datetime.fromtimestamp(current_snapshot.timestamp_ms / 1000)}")

    # Count snapshots
    snapshots = list(table.snapshots())
    print(f"Total Snapshots: {len(snapshots)}")

    # Scan table to get record count
    print("\nScanning table for record count...")
    try:
        df = table.scan().to_arrow()
        print(f"Total Records: {len(df)}")
        print(f"\nSample records:")
        print(df.to_pandas().head(3).to_string())
    except Exception as e:
        print(f"Could not scan table: {e}")


def main():
    """Main entry point for Goal 1."""
    print("=" * 60)
    print("Goal 1: Snowflake Open Catalog - Daily Partitioned Table")
    print("=" * 60)

    try:
        # Load configuration
        print("\nLoading configuration...")
        config = Config.load()
        print(f"✓ Configuration loaded")
        print(f"  S3 Bucket (Daily): {config.aws.s3_bucket_daily}")
        print(f"  Namespace: {config.tables.daily_namespace}")
        print(f"  Table: {config.tables.daily_table_name}")

        # Get Snowflake catalog
        print("\nConnecting to Snowflake Open Catalog...")
        catalog = get_snowflake_catalog(config)
        print(f"✓ Connected to catalog: {config.snowflake.catalog_name}")

        # Create namespace
        print(f"\nCreating namespace: {config.tables.daily_namespace}...")
        create_namespace_if_not_exists(catalog, config.tables.daily_namespace)

        # Create table
        print(f"\nCreating table: {config.tables.daily_table_name}...")
        table_location = f"s3://{config.aws.s3_bucket_daily}/{config.tables.daily_namespace}/{config.tables.daily_table_name}"
        create_table_if_not_exists(
            catalog,
            config.tables.daily_namespace,
            config.tables.daily_table_name,
            table_location
        )

        # Load table
        table_identifier = f"{config.tables.daily_namespace}.{config.tables.daily_table_name}"
        table = catalog.load_table(table_identifier)

        # Ingest data for the past 7 days
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=7)

        total_events = ingest_daily_data(
            table=table,
            start_date=start_date,
            num_days=7,
            events_per_day=1000
        )

        print(f"\n✓ Successfully ingested {total_events} events")

        # Print statistics
        print_table_stats(table)

        print("\n" + "=" * 60)
        print("Goal 1 Complete!")
        print("=" * 60)
        print("\nNext steps:")
        print("  1. Verify data in Snowflake Open Catalog UI")
        print("  2. Check S3 bucket for partitioned data")
        print(f"     aws s3 ls s3://{config.aws.s3_bucket_daily}/{config.tables.daily_namespace}/{config.tables.daily_table_name}/ --recursive --profile {config.aws.profile}")
        print("  3. Run Goal 2: python goal2_s3tables_hourly.py")

    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
