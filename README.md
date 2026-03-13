# Apache Iceberg Cross-Catalog ETL Demo

A production-ready demonstration of Apache Iceberg table management across multiple catalogs with incremental ETL and partition transformation.

## Overview

This project showcases:
- **Cross-catalog data movement** between Snowflake Open Catalog and AWS S3 Tables
- **Partition transformation** from daily to hourly granularity
- **Incremental ETL** using Iceberg's snapshot-based change tracking
- **Idempotent processing** suitable for production scheduling (cron/Airflow)
- **ACID guarantees** leveraging Iceberg's transactional capabilities

## Project Goals

### Goal 1: Daily-Partitioned Source Table (Snowflake Open Catalog)
- Create Iceberg table with daily partitioning
- Store data in AWS S3
- Managed by Snowflake Open Catalog (Polaris)
- Generate sample event data for testing

### Goal 2: Hourly-Partitioned Destination Table (AWS S3 Tables)
- Create Iceberg table with hourly partitioning
- Integrate with AWS Glue Data Catalog and Lake Formation
- Read from Goal 1 table incrementally
- Transform partition granularity (daily → hourly)
- Track processing state for idempotent execution

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Snowflake Open Catalog (Polaris)                      │
│  ┌─────────────────────────────────┐                   │
│  │  demo_db.events_daily           │                   │
│  │  Partition: day(event_timestamp)│                   │
│  │  Storage: S3                    │                   │
│  └─────────────────────────────────┘                   │
└─────────────────────────────────────────────────────────┘
                        │
                        │ Incremental ETL
                        │ (Snapshot-based)
                        ↓
┌─────────────────────────────────────────────────────────┐
│  AWS S3 Tables + Lake Formation                         │
│  ┌───────────────────────────────────┐                 │
│  │  icelake_hourly_db.events_hourly  │                 │
│  │  Partition: hour(event_timestamp) │                 │
│  │  Storage: S3 Tables               │                 │
│  └───────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────┘
```

## Table Schema

All tables share a common event schema:

```
event_id          string      (required)  - UUID identifier
event_timestamp   timestamp   (required)  - Event occurrence time
event_type        string      (required)  - Event category (page_view, click, purchase, signup)
user_id           string      (required)  - User identifier
data              string      (optional)  - JSON metadata
```

**Partition Evolution:**
- **Source**: `day(event_timestamp)` → 1 partition per day
- **Destination**: `hour(event_timestamp)` → 24 partitions per day

## Technology Stack

- **Python 3.11+** with type hints
- **PyIceberg 0.7.1** - Apache Iceberg Python SDK
- **PyArrow 15.0+** - Efficient columnar data processing
- **Boto3** - AWS SDK (S3, Glue, Lake Formation, S3 Tables)
- **Snowflake Connector** - OAuth-based catalog access
- **Faker** - Realistic test data generation

## Prerequisites

### AWS Requirements
- AWS account with programmatic access
- IAM role for Snowflake Open Catalog (with S3 access)
- IAM role for Glue + Lake Formation
- S3 bucket for daily-partitioned data
- S3 Tables bucket for hourly-partitioned data
- Lake Formation permissions configured

### Snowflake Requirements
- Snowflake Open Catalog account
- OAuth client credentials (client ID + secret)
- Catalog created in Polaris UI

### Local Requirements
- Python 3.11 or higher
- AWS CLI configured with appropriate profile
- Virtual environment support

## Quick Start

### 1. Clone and Setup Environment

```bash
git clone <your-repo-url>
cd icelake

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment Variables

```bash
# Copy template
cp .env.template .env

# Edit with your credentials
nano .env  # or your preferred editor
```

**Required Configuration:**

```bash
# AWS
AWS_PROFILE=your-aws-profile
AWS_REGION=us-east-2
AWS_ACCOUNT_ID=123456789012
S3_BUCKET_DAILY=your-bucket-daily
S3_TABLES_BUCKET_NAME=your-s3-tables-bucket
S3_TABLES_BUCKET_ARN=arn:aws:s3tables:us-east-2:123456789012:bucket/your-bucket
IAM_ROLE_ARN=arn:aws:iam::123456789012:role/your-snowflake-role
GLUE_IAM_ROLE_ARN=arn:aws:iam::123456789012:role/your-glue-role
EXTERNAL_ID=your-external-id

# Snowflake Open Catalog
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_CLIENT_ID=your_client_id
SNOWFLAKE_CLIENT_SECRET=your_client_secret
SNOWFLAKE_CATALOG_NAME=your-catalog
SNOWFLAKE_CATALOG_URI=https://your-locator.region.aws.snowflakecomputing.com/polaris/api/catalog

# Tables
DAILY_TABLE_NAMESPACE=demo_db
DAILY_TABLE_NAME=events_daily
HOURLY_TABLE_DATABASE=icelake_hourly_db
HOURLY_TABLE_NAME=events_hourly
STATE_FILE_PATH=state/etl_state.json
```

### 3. Create AWS Infrastructure

#### 3a. Create S3 Tables Bucket (One-time Setup)

**This is required before running Goal 2.** The S3 Tables bucket is the container for hourly-partitioned tables.

```bash
# Option 1: Use the provided script (reads from .env)
export $(grep -v '^#' .env | xargs)
bash setup/create_s3_tables_bucket.sh

# Option 2: Manual creation via AWS CLI
aws s3tables create-table-bucket \
  --name your-s3-tables-bucket \
  --region us-east-2 \
  --profile your-profile

# Option 3: Use AWS Console
# Navigate to S3 → Table buckets → Create table bucket
```

**Important**: Save the bucket ARN output and update `.env`:
```bash
S3_TABLES_BUCKET_ARN=arn:aws:s3tables:us-east-2:123456789012:bucket/your-bucket
```

#### 3b. Enable S3 Tables Analytics Integration

For Glue catalog integration and Athena queries:

1. Go to AWS Console → S3 → Table buckets
2. Find "Integration with AWS analytics services"
3. Click "Enable integration"
4. Wait 1-2 minutes for setup to complete

This creates the `s3tablescatalog` in Glue Data Catalog.

#### 3c. Configure Lake Formation Permissions

Grant permissions for your IAM user/role:

```bash
# Grant database creation permission
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::123456789012:role/your-role \
  --permissions CREATE_DATABASE \
  --resource '{"Catalog":{}}' \
  --region us-east-2

# Grant permissions on the database
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::123456789012:role/your-role \
  --permissions ALL \
  --resource '{"Database":{"CatalogId":"s3tablescatalog","Name":"icelake_hourly_db"}}' \
  --region us-east-2
```

### 4. Verify Connectivity

```bash
python setup/verify_connections.py
```

This validates:
- ✅ AWS credentials and permissions
- ✅ S3 bucket access
- ✅ Snowflake Open Catalog connectivity
- ✅ Glue catalog access
- ✅ Lake Formation permissions

### 5. Run Goal 1 - Create Source Table

```bash
python goal1_snowflake_daily.py
```

**What it does:**
1. Connects to Snowflake Open Catalog via OAuth
2. Creates namespace: `demo_db`
3. Creates table: `events_daily` with daily partitions
4. Generates 7 days × 1000 events of sample data
5. Writes to S3 via Snowflake catalog

**Expected Output:**
```
============================================================
Goal 1: Snowflake Open Catalog - Daily Partitioned Table
============================================================

✓ Created namespace: demo_db
✓ Created table: demo_db.events_daily
  Schema: 5 fields
  Partition spec: day(event_timestamp)

Processing day 1/7: 2026-03-06
✓ Ingested 1000 events for 2026-03-06
...

Goal 1 Complete! Total events ingested: 7000
```

### 6. Run Goal 2 - Incremental ETL

```bash
python goal2_s3tables_hourly.py
```

**What it does:**
1. Loads last processed snapshot ID (none on first run)
2. Connects to both catalogs (Snowflake + S3 Tables)
3. Checks source table for new data
4. If new data exists:
   - Reads from source
   - Sorts by timestamp
   - Writes to destination with hourly partitions
   - Saves snapshot ID to state file
5. If no new data: exits gracefully

**Expected Output:**
```
============================================================
Goal 2: AWS S3 Tables - Hourly Partitioned (Incremental ETL)
============================================================

✓ No previous state (first run)
✓ Connected to catalogs
✓ Loaded source table: demo_db.events_daily
✓ Created destination table: icelake_hourly_db.events_hourly

Reading incremental data...
  ✓ Read 7000 records

Transforming data...
✓ Data sorted by timestamp

Writing to destination...
  ✓ Data written successfully

✓ State updated

============================================================
Processing Summary
============================================================
Records Processed: 7000
Source Snapshot ID: 4516887802303547849

Destination Table (Hourly Partitioned):
  Current Snapshot ID: 8923445566778899000
  Total Snapshots: 1
  Total Records: 7000

Goal 2 Complete!
```

### 7. Test Idempotency

```bash
# Run Goal 2 again - should skip processing
python goal2_s3tables_hourly.py
```

**Expected Output:**
```
✓ No new data (snapshot 4516887802303547849 already processed)
```

## Data Flow

```
┌──────────────────────────────────────────────────────────────┐
│ 1. Load State                                                │
│    last_snapshot_id = state_manager.get_last_snapshot_id()   │
└──────────────────────────────────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│ 2. Connect to Catalogs                                       │
│    - Snowflake Open Catalog (REST, OAuth)                    │
│    - AWS S3 Tables (REST, SigV4, Lake Formation)             │
└──────────────────────────────────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│ 3. Load Source & Destination Tables                         │
│    source_table = snowflake_catalog.load_table(...)          │
│    dest_table = s3tables_catalog.load_table(...)             │
└──────────────────────────────────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│ 4. Check for New Data                                        │
│    current_snapshot = source_table.current_snapshot()        │
│    if current_snapshot_id == last_snapshot_id:               │
│        exit(0)  # No new data                                │
└──────────────────────────────────────────────────────────────┘
                           ↓
                    [New Data Found]
                           ↓
┌──────────────────────────────────────────────────────────────┐
│ 5. Read & Transform                                          │
│    data = source_table.scan().to_arrow()                     │
│    sorted_data = sort_by(event_timestamp)                    │
└──────────────────────────────────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│ 6. Write to Destination                                      │
│    dest_table.append(sorted_data)                            │
│    → Creates hourly partitions                               │
│    → Generates new snapshot                                  │
└──────────────────────────────────────────────────────────────┘
                           ↓
┌──────────────────────────────────────────────────────────────┐
│ 7. Update State                                              │
│    state_manager.save(current_snapshot_id)                   │
└──────────────────────────────────────────────────────────────┘
```

## Project Structure

```
icelake/
├── README.md                   # This file
├── .env.template               # Configuration template
├── .gitignore                  # Git ignore rules
├── requirements.txt            # Python dependencies
│
├── src/                        # Core modules
│   ├── __init__.py
│   ├── config.py               # Environment variable management
│   ├── catalogs.py             # Catalog connection factories
│   ├── schemas.py              # Iceberg schemas & partition specs
│   ├── data_generator.py      # Sample data generation
│   └── state_manager.py       # Snapshot tracking
│
├── goal1_snowflake_daily.py    # Goal 1: Create daily table
├── goal2_s3tables_hourly.py    # Goal 2: Incremental ETL
│
├── setup/                      # Infrastructure setup
│   ├── __init__.py
│   ├── aws_setup.py            # Glue DB + Lake Formation
│   ├── verify_connections.py  # Connectivity tests
│   ├── create_s3_tables_bucket.sh
│   └── create_s3_table.py
│
├── state/                      # State persistence
│   ├── .gitkeep
│   └── etl_state.json          # Last processed snapshot (git-ignored)
│
└── docs/                       # Documentation
    ├── IAM_SETUP_GUIDE.md
    ├── SNOWFLAKE_OAUTH_SETUP.md
    ├── S3_TABLES_INTEGRATION_GUIDE.md
    └── ...
```

## State Management

The project uses **Iceberg snapshot IDs** for incremental processing:

### State File: `state/etl_state.json`

```json
{
  "snapshot_id": 4516887802303547849,
  "timestamp": "2026-03-13T10:30:15.123456"
}
```

### How It Works

1. **First Run**: `last_snapshot_id = None` → processes all data
2. **Subsequent Runs**: Compares `current_snapshot_id` vs `last_snapshot_id`
   - **IDs match** → No new data, exits quickly
   - **IDs differ** → New data detected, processes incrementally
3. **After Processing**: Saves new `snapshot_id` to state file

### Reset State

```bash
# Delete state file
rm state/etl_state.json

# Or use Python
python -c "from src.state_manager import StateManager; StateManager('state/etl_state.json').clear()"
```

## Production Scheduling

Goal 2 is designed for automated execution:

### Cron Example

```bash
# Run every hour
0 * * * * cd /path/to/icelake && ./venv/bin/python goal2_s3tables_hourly.py >> logs/etl.log 2>&1
```

### Airflow DAG Example

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'iceberg_hourly_etl',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
)

etl_task = BashOperator(
    task_id='run_goal2',
    bash_command='cd /path/to/icelake && source venv/bin/activate && python goal2_s3tables_hourly.py',
    dag=dag,
)
```

**Why it works for scheduling:**
- ✅ **Idempotent**: Safe to re-run, skips if no new data
- ✅ **Fast no-op**: Quick exit when no changes detected
- ✅ **Stateful**: Never processes same data twice
- ✅ **Exit codes**: 0 = success, 1 = error
- ✅ **Structured logging**: Easy to monitor

## Querying Data

### AWS Athena

```sql
-- Query with automatic partition pruning
SELECT
    event_type,
    COUNT(*) as count,
    MIN(event_timestamp) as first_event,
    MAX(event_timestamp) as last_event
FROM s3tablescatalog.icelake_hourly_db.events_hourly
WHERE event_timestamp >= TIMESTAMP '2026-03-01 00:00:00'
  AND event_timestamp < TIMESTAMP '2026-03-02 00:00:00'
GROUP BY event_type;
```

### PyIceberg (Python)

```python
from src.config import Config
from src.catalogs import get_s3tables_catalog

config = Config.load()
catalog = get_s3tables_catalog(config)
table = catalog.load_table('icelake_hourly_db.events_hourly')

# Scan with filters
df = table.scan(
    row_filter="event_type == 'purchase'"
).to_pandas()

print(df.describe())
```

## Key Features

### Iceberg Advantages
- **ACID Transactions**: Atomic commits, consistent reads
- **Time Travel**: Query historical snapshots
- **Schema Evolution**: Add/modify columns without rewrites
- **Partition Evolution**: Change partitioning without data migration
- **Hidden Partitioning**: Automatic partition pruning

### Design Patterns
- **Snapshot-based CDC**: No complex change tracking required
- **Idempotent Processing**: Safe for retries and scheduling
- **Cross-catalog ETL**: Bridge different data platforms
- **Partition Transformation**: Optimize for query patterns

## Troubleshooting

### AWS Issues

**Problem**: Access denied errors
**Solution**: Check IAM permissions for S3, Glue, Lake Formation, S3 Tables

**Problem**: S3 Tables bucket not found
**Solution**: Ensure AWS analytics integration is enabled for S3 Tables

**Problem**: Lake Formation permissions error
**Solution**: Grant required permissions via Lake Formation console

### Snowflake Issues

**Problem**: OAuth authentication failed
**Solution**: Verify client ID and secret in `.env`

**Problem**: Catalog not found
**Solution**: Check catalog exists in Polaris UI, verify URI format

### General Issues

**Problem**: "Source table not found"
**Solution**: Run Goal 1 first to create the source table

**Problem**: Import errors
**Solution**: Ensure virtual environment is activated and dependencies installed

**Problem**: State file corrupted
**Solution**: Delete `state/etl_state.json` and re-run

## Testing

```bash
# Test full flow
python goal1_snowflake_daily.py           # Creates source data
python goal2_s3tables_hourly.py           # First run: processes all
python goal2_s3tables_hourly.py           # Second run: no new data

# Test incremental processing
python goal1_snowflake_daily.py           # Add more data
python goal2_s3tables_hourly.py           # Processes only new data
```

## Advanced Topics

- **Compaction**: Manage small files with Iceberg maintenance
- **Metadata optimization**: Periodic cleanup of old snapshots
- **Multi-table pipelines**: Extend to handle multiple table pairs
- **Monitoring**: Add CloudWatch metrics and alerts
- **Schema evolution**: Handle schema changes gracefully
- **Data quality**: Add validation before writing

## Resources

- [Apache Iceberg](https://iceberg.apache.org/)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
- [Snowflake Open Catalog](https://docs.snowflake.com/en/guides-overview-open-catalog)
- [AWS S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets.html)
- [AWS Glue + Iceberg](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)

## Contributing

This is a demonstration project. Feel free to:
- Fork and experiment
- Submit issues for bugs or questions
- Propose enhancements via pull requests

## License

MIT License - See LICENSE file for details

---

**Built with** Apache Iceberg • PyIceberg • AWS S3 Tables • Snowflake Open Catalog
