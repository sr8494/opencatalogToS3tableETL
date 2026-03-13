"""Sample event data generation for testing and demo purposes."""

import json
import random
from datetime import datetime, timedelta
from typing import Optional
import pyarrow as pa
from faker import Faker


def generate_sample_events(
    num_events: int,
    start_date: datetime,
    end_date: Optional[datetime] = None
) -> pa.Table:
    """
    Generate sample event data with realistic attributes.

    Args:
        num_events: Number of events to generate
        start_date: Start of the time range
        end_date: End of the time range (default: start_date + 1 day)

    Returns:
        PyArrow table with event data matching the events schema
    """
    if end_date is None:
        end_date = start_date + timedelta(days=1)

    fake = Faker()
    Faker.seed(42)  # For reproducibility

    event_types = ['page_view', 'click', 'purchase', 'signup']

    # Generate data
    event_ids = []
    event_timestamps = []
    event_types_list = []
    user_ids = []
    data_list = []

    time_range_seconds = int((end_date - start_date).total_seconds())

    for _ in range(num_events):
        # Generate random timestamp within range
        random_seconds = random.randint(0, time_range_seconds)
        timestamp = start_date + timedelta(seconds=random_seconds)

        # Generate event data
        event_id = fake.uuid4()
        event_type = random.choice(event_types)
        user_id = f"user_{random.randint(1000, 9999)}"

        # Generate event-specific data as JSON
        event_data = {
            'page_url': fake.url() if event_type in ['page_view', 'click'] else None,
            'product_id': f"prod_{random.randint(100, 999)}" if event_type == 'purchase' else None,
            'amount': round(random.uniform(10, 1000), 2) if event_type == 'purchase' else None,
            'referrer': fake.url() if random.random() > 0.5 else None,
            'user_agent': fake.user_agent(),
        }
        # Remove None values
        event_data = {k: v for k, v in event_data.items() if v is not None}

        event_ids.append(event_id)
        event_timestamps.append(timestamp)
        event_types_list.append(event_type)
        user_ids.append(user_id)
        data_list.append(json.dumps(event_data))

    # Create PyArrow schema with correct nullability
    # Match the Iceberg schema: required fields are non-nullable
    schema = pa.schema([
        pa.field('event_id', pa.string(), nullable=False),
        pa.field('event_timestamp', pa.timestamp('us'), nullable=False),
        pa.field('event_type', pa.string(), nullable=False),
        pa.field('user_id', pa.string(), nullable=False),
        pa.field('data', pa.string(), nullable=True),  # Only this field is optional
    ])

    # Create PyArrow table with explicit schema
    table = pa.table({
        'event_id': pa.array(event_ids, type=pa.string()),
        'event_timestamp': pa.array(event_timestamps, type=pa.timestamp('us')),
        'event_type': pa.array(event_types_list, type=pa.string()),
        'user_id': pa.array(user_ids, type=pa.string()),
        'data': pa.array(data_list, type=pa.string()),
    }, schema=schema)

    return table
