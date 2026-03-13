#!/bin/bash

# Script to create IAM role for AWS Glue and LakeFormation
#
# Usage:
#   1. Set environment variables from .env:
#      export $(grep -v '^#' .env | xargs)
#      bash setup/create_iam_role.sh
#
#   2. Or set variables directly:
#      AWS_PROFILE=your-profile bash setup/create_iam_role.sh
#
#   3. Or edit the default values below
#
# Prerequisites:
#   - AWS CLI installed and configured
#   - IAM permissions to create roles and policies

set -e

# Configuration - Update these values for your environment
ACCOUNT_ID="${AWS_ACCOUNT_ID:-123456789012}"
ROLE_NAME="${GLUE_IAM_ROLE_NAME:-icelake-glue-lakeformation-role}"
BUCKET_NAME_DAILY="${S3_BUCKET_DAILY:-your-bucket-daily}"
BUCKET_NAME_HOURLY="${S3_BUCKET_HOURLY:-your-bucket-hourly}"
AWS_PROFILE="${AWS_PROFILE:-your-profile}"

echo "Creating IAM role for Glue and LakeFormation..."

# Create trust policy
cat > /tmp/trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "lakeformation.amazonaws.com",
          "glue.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create the role
aws iam create-role \
  --role-name "$ROLE_NAME" \
  --assume-role-policy-document file:///tmp/trust-policy.json \
  --description "Role for Glue and LakeFormation to access S3 for Iceberg tables" \
  --profile "$AWS_PROFILE"

echo "✓ Created role: $ROLE_NAME"

# Create permissions policy
cat > /tmp/permissions-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3ReadWriteDailyBucket",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::${BUCKET_NAME_DAILY}/*"
    },
    {
      "Sid": "S3ListDailyBucket",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::${BUCKET_NAME_DAILY}"
    },
    {
      "Sid": "S3ReadWriteHourlyBucket",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::${BUCKET_NAME_HOURLY}/*"
    },
    {
      "Sid": "S3ListHourlyBucket",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::${BUCKET_NAME_HOURLY}"
    },
    {
      "Sid": "GluePermissions",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:CreateTable",
        "glue:UpdateTable"
      ],
      "Resource": "*"
    }
  ]
}
EOF

# Attach inline policy
aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name "${ROLE_NAME}-policy" \
  --policy-document file:///tmp/permissions-policy.json \
  --profile "$AWS_PROFILE"

echo "✓ Attached permissions policy"

# Get role ARN
ROLE_ARN=$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text --profile "$AWS_PROFILE")

echo ""
echo "=========================================="
echo "IAM Role Created Successfully!"
echo "=========================================="
echo "Role Name: $ROLE_NAME"
echo "Role ARN: $ROLE_ARN"
echo ""
echo "Next steps:"
echo "1. Update your .env file with:"
echo "   GLUE_IAM_ROLE_ARN=$ROLE_ARN"
echo ""
echo "2. Or keep using the same variable and update:"
echo "   IAM_ROLE_ARN=$ROLE_ARN"
echo ""
echo "3. Run: python setup/aws_setup.py"

# Cleanup temp files
rm /tmp/trust-policy.json /tmp/permissions-policy.json
