#!/bin/bash

# Cleanup script for Redshift resources
# Use this if you need to start over

set -e

echo "Cleaning up Redshift resources..."

PROJECT_NAME="book-sales-platform"
CLUSTER_IDENTIFIER="${PROJECT_NAME}-redshift"

if aws redshift describe-clusters --cluster-identifier $CLUSTER_IDENTIFIER --region us-east-2 >/dev/null 2>&1; then
    echo "Deleting Redshift cluster..."
    aws redshift delete-cluster \
        --cluster-identifier $CLUSTER_IDENTIFIER \
        --skip-final-cluster-snapshot \
        --region us-east-2
    
    echo "Waiting for cluster deletion..."
    aws redshift wait cluster-deleted --cluster-identifier $CLUSTER_IDENTIFIER --region us-east-2
    echo "Cluster deleted"
else
    echo "No cluster to delete"
fi

SECURITY_GROUP_ID=$(aws ec2 describe-security-groups \
    --group-names "${PROJECT_NAME}-redshift-sg" \
    --query "SecurityGroups[0].GroupId" \
    --output text 2>/dev/null || echo "None")

if [ "$SECURITY_GROUP_ID" != "None" ]; then
    echo "Deleting security group..."
    aws ec2 delete-security-group --group-id $SECURITY_GROUP_ID
    echo "Security group deleted"
else
    echo "No security group to delete"
fi

if aws iam get-role --role-name "${PROJECT_NAME}-redshift-role" >/dev/null 2>&1; then
    echo "Cleaning up IAM role..."
    
    aws iam detach-role-policy \
        --role-name "${PROJECT_NAME}-redshift-role" \
        --policy-arn "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess" 2>/dev/null || true
    
    aws iam detach-role-policy \
        --role-name "${PROJECT_NAME}-redshift-role" \
        --policy-arn "arn:aws:iam::aws:policy/AmazonRedshiftFullAccess" 2>/dev/null || true
    
    aws iam delete-role --role-name "${PROJECT_NAME}-redshift-role"
    echo "IAM role deleted"
else
    echo "No IAM role to delete"
fi

echo "Cleanup completed!"
