"""Schema and partition specification definitions for Iceberg tables."""

from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    TimestampType,
    NestedField,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, HourTransform


def get_events_schema() -> Schema:
    """
    Get the schema for the events table.

    Returns:
        Schema with event_id, event_timestamp, event_type, user_id, and data fields
    """
    return Schema(
        NestedField(field_id=1, name="event_id", field_type=StringType(), required=True),
        NestedField(field_id=2, name="event_timestamp", field_type=TimestampType(), required=True),
        NestedField(field_id=3, name="event_type", field_type=StringType(), required=True),
        NestedField(field_id=4, name="user_id", field_type=StringType(), required=True),
        NestedField(field_id=5, name="data", field_type=StringType(), required=False),
    )


def get_daily_partition_spec() -> PartitionSpec:
    """
    Get partition spec for daily partitioning.

    Returns:
        PartitionSpec that partitions by day(event_timestamp)
    """
    return PartitionSpec(
        PartitionField(
            source_id=2,  # event_timestamp field ID
            field_id=1000,
            transform=DayTransform(),
            name='event_date'
        )
    )


def get_hourly_partition_spec() -> PartitionSpec:
    """
    Get partition spec for hourly partitioning.

    Returns:
        PartitionSpec that partitions by hour(event_timestamp)
    """
    return PartitionSpec(
        PartitionField(
            source_id=2,  # event_timestamp field ID
            field_id=1000,
            transform=HourTransform(),
            name='event_hour'
        )
    )
