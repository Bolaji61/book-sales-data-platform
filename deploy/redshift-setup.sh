#!/bin/bash
# Redshift Setup Script for Book Sales Platform
# Creates Redshift cluster and necessary AWS resources

set -e

echo "Setting up Redshift cluster for Book Sales Platform..."

if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo "AWS CLI not configured. Please run 'aws configure' first."
    exit 1
fi

REGION="us-east-2"
PROJECT_NAME="book-sales-platform"
CLUSTER_IDENTIFIER="${PROJECT_NAME}-redshift"
NODE_TYPE="ra3.xlplus"
NUM_NODES=2
DB_NAME="book_sales"
DB_USER="admin"
DB_PASSWORD="BookSales2024!"
S3_BUCKET="${PROJECT_NAME}-data-$(openssl rand -hex 4)"

echo "Configuration:"
echo "Region: $REGION"
echo "Cluster: $CLUSTER_IDENTIFIER"
echo "Node Type: $NODE_TYPE"
echo "Number of Nodes: $NUM_NODES"
echo "S3 Bucket: $S3_BUCKET"

echo "Creating S3 bucket for data..."
aws s3 mb s3://$S3_BUCKET --region $REGION
echo "S3 bucket created: $S3_BUCKET"

echo "Creating IAM role for Redshift..."
cat > /tmp/redshift-trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "redshift.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

aws iam create-role \
    --role-name "${PROJECT_NAME}-redshift-role" \
    --assume-role-policy-document file:///tmp/redshift-trust-policy.json || echo "Role already exists"

aws iam attach-role-policy \
    --role-name "${PROJECT_NAME}-redshift-role" \
    --policy-arn "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"

aws iam attach-role-policy \
    --role-name "${PROJECT_NAME}-redshift-role" \
    --policy-arn "arn:aws:iam::aws:policy/AmazonRedshiftFullAccess"

REDSHIFT_ROLE_ARN=$(aws iam get-role \
    --role-name "${PROJECT_NAME}-redshift-role" \
    --query "Role.Arn" \
    --output text)

echo "IAM role created: $REDSHIFT_ROLE_ARN"

echo "Creating security group for Redshift..."
VPC_ID=$(aws ec2 describe-vpcs --filters "Name=is-default,Values=true" --query "Vpcs[0].VpcId" --output text)
SECURITY_GROUP_ID=$(aws ec2 create-security-group \
    --group-name "${PROJECT_NAME}-redshift-sg" \
    --description "Security group for Redshift cluster" \
    --vpc-id $VPC_ID \
    --query "GroupId" \
    --output text)

aws ec2 authorize-security-group-ingress \
    --group-id $SECURITY_GROUP_ID \
    --protocol tcp \
    --port 5439 \
    --cidr 0.0.0.0/0

echo "Security group created: $SECURITY_GROUP_ID"

echo "Creating Redshift cluster..."
aws redshift create-cluster \
    --cluster-identifier $CLUSTER_IDENTIFIER \
    --node-type $NODE_TYPE \
    --number-of-nodes $NUM_NODES \
    --master-username $DB_USER \
    --master-user-password $DB_PASSWORD \
    --db-name $DB_NAME \
    --cluster-type multi-node \
    --vpc-security-group-ids $SECURITY_GROUP_ID \
    --iam-roles $REDSHIFT_ROLE_ARN \
    --region $REGION

echo "Waiting for Redshift cluster to be available..."
aws redshift wait cluster-available --cluster-identifier $CLUSTER_IDENTIFIER --region $REGION

CLUSTER_ENDPOINT=$(aws redshift describe-clusters \
    --cluster-identifier $CLUSTER_IDENTIFIER \
    --query "Clusters[0].Endpoint.Address" \
    --output text \
    --region $REGION)

CLUSTER_PORT=$(aws redshift describe-clusters \
    --cluster-identifier $CLUSTER_IDENTIFIER \
    --query "Clusters[0].Endpoint.Port" \
    --output text \
    --region $REGION)

echo "Redshift cluster created successfully!"
echo "Endpoint: $CLUSTER_ENDPOINT"
echo "Port: $CLUSTER_PORT"


echo "Creating environment configuration..."
cat > .env.production << EOF
# Environment variables for Book Sales Data Platform


# Redshift Configuration
REDSHIFT_CLUSTER=$CLUSTER_IDENTIFIER
REDSHIFT_ENDPOINT=$CLUSTER_ENDPOINT
REDSHIFT_PORT=$CLUSTER_PORT
REDSHIFT_DB=$DB_NAME
REDSHIFT_USER=$DB_USER
REDSHIFT_PASSWORD=$DB_PASSWORD
REDSHIFT_ROLE_ARN=$REDSHIFT_ROLE_ARN

# S3 Configuration
S3_BUCKET_NAME=$S3_BUCKET
S3_PREFIX=raw-data

# Data Source Configuration
DATA_SOURCE_TYPE=s3
DATA_DIR=data

# AWS Configuration
AWS_REGION=$REGION
AWS_ACCESS_KEY_ID=\${AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY=\${AWS_SECRET_ACCESS_KEY}


# Alerting Configuration
SNS_TOPIC_ARN=\${SNS_TOPIC_ARN}
EOF