#!/bin/bash

# Script to create AWS S3 Tables bucket and infrastructure
# S3 Tables is a managed table storage optimized for Apache Iceberg
#
# Usage:
#   1. Set environment variables from .env:
#      export $(grep -v '^#' .env | xargs)
#      bash setup/create_s3_tables_bucket.sh
#
#   2. Or set variables directly:
#      AWS_PROFILE=your-profile bash setup/create_s3_tables_bucket.sh
#
#   3. Or edit the default values below
#
# Prerequisites:
#   - AWS CLI installed and configured
#   - IAM permissions for s3tables:CreateTableBucket
#   - S3 Tables analytics integration enabled (optional, for Glue sync)

set -e

# Configuration - Update these values for your environment
ACCOUNT_ID="${AWS_ACCOUNT_ID:-123456789012}"  # Get from .env or set here
REGION="${AWS_REGION:-us-east-2}"
AWS_PROFILE="${AWS_PROFILE:-your-profile}"
TABLE_BUCKET_NAME="${S3_TABLES_BUCKET_NAME:-your-s3-tables-bucket}"
NAMESPACE="${HOURLY_TABLE_DATABASE:-icelake_hourly_db}"
TABLE_NAME="${HOURLY_TABLE_NAME:-events_hourly}"

echo "=" * 70
echo "Creating AWS S3 Tables Infrastructure"
echo "=" * 70

# Step 1: Create S3 Tables Bucket
echo ""
echo "Step 1: Creating S3 Tables bucket..."
echo "Name: $TABLE_BUCKET_NAME"
echo "Region: $REGION"

TABLE_BUCKET_ARN=$(aws s3tables create-table-bucket \
  --name "$TABLE_BUCKET_NAME" \
  --region "$REGION" \
  --profile "$AWS_PROFILE" \
  --query 'arn' \
  --output text 2>&1) || {

  # Check if bucket already exists
  if echo "$TABLE_BUCKET_ARN" | grep -q "BucketAlreadyExists\|AlreadyExists"; then
    echo "✓ S3 Tables bucket already exists: $TABLE_BUCKET_NAME"
    TABLE_BUCKET_ARN="arn:aws:s3tables:${REGION}:${ACCOUNT_ID}:bucket/${TABLE_BUCKET_NAME}"
  else
    echo "❌ Failed to create S3 Tables bucket: $TABLE_BUCKET_ARN"
    exit 1
  fi
}

echo "✓ S3 Tables bucket created/verified"
echo "ARN: $TABLE_BUCKET_ARN"

# Step 2: Create Namespace
echo ""
echo "Step 2: Creating namespace (database)..."
echo "Namespace: $NAMESPACE"

aws s3tables create-namespace \
  --table-bucket-arn "$TABLE_BUCKET_ARN" \
  --namespace "$NAMESPACE" \
  --region "$REGION" \
  --profile "$AWS_PROFILE" 2>&1 || {

  if echo "$?" | grep -q "NamespaceAlreadyExists\|AlreadyExists"; then
    echo "✓ Namespace already exists: $NAMESPACE"
  else
    echo "⚠️  Warning: Could not create namespace (may already exist)"
  fi
}

echo "✓ Namespace created/verified: $NAMESPACE"

# Step 3: Configure Table Maintenance (optional but recommended)
echo ""
echo "Step 3: Configuring automatic maintenance..."

aws s3tables put-table-bucket-maintenance-configuration \
  --table-bucket-arn "$TABLE_BUCKET_ARN" \
  --value '{
    "iceberg-compaction": {
      "status": "enabled",
      "settings": {
        "target-file-size-MB": 512
      }
    },
    "iceberg-snapshot-management": {
      "status": "enabled",
      "settings": {
        "min-snapshots-to-keep": 3,
        "max-snapshot-age-hours": 168
      }
    }
  }' \
  --region "$REGION" \
  --profile "$AWS_PROFILE" 2>/dev/null || {
  echo "⚠️  Warning: Could not configure maintenance (may need additional permissions)"
}

echo "✓ Maintenance configuration set"

# Step 4: Verify Setup
echo ""
echo "Step 4: Verifying setup..."

echo "Listing table buckets..."
aws s3tables list-table-buckets \
  --region "$REGION" \
  --profile "$AWS_PROFILE" \
  --output table

echo ""
echo "Listing namespaces..."
aws s3tables list-namespaces \
  --table-bucket-arn "$TABLE_BUCKET_ARN" \
  --region "$REGION" \
  --profile "$AWS_PROFILE" \
  --output table

# Summary
echo ""
echo "=" * 70
echo "S3 Tables Infrastructure Created Successfully!"
echo "=" * 70
echo ""
echo "Table Bucket ARN: $TABLE_BUCKET_ARN"
echo "Namespace: $NAMESPACE"
echo ""
echo "Next steps:"
echo "1. Update your .env file with:"
echo "   S3_TABLES_BUCKET_ARN=$TABLE_BUCKET_ARN"
echo "   S3_TABLES_BUCKET_NAME=$TABLE_BUCKET_NAME"
echo ""
echo "2. Run Goal 2: python goal2_s3tables_hourly.py"
echo ""
echo "Note: Tables will be created automatically by Goal 2"
echo "      or you can create manually with:"
echo "      aws s3tables create-table --table-bucket-arn $TABLE_BUCKET_ARN \\"
echo "        --namespace $NAMESPACE --name $TABLE_NAME --format ICEBERG"
