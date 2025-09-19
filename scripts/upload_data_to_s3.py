"""
Upload data files to S3 for production deployment
"""

import argparse
from pathlib import Path

import boto3

from logger import get_logger, log_error, log_info, log_success, log_warning


def upload_data_to_s3(bucket_name: str, data_dir: str = "data", prefix: str = "data"):
    """Upload CSV files to S3 bucket for Redshift processing"""
    logger = get_logger("s3_upload")

    s3_client = boto3.client("s3")

    # files to upload with their S3 keys
    files_to_upload = {
        "users.csv": f"{prefix}/users.csv",
        "transactions.csv": f"{prefix}/transactions.csv",
        "books.csv": f"{prefix}/books.csv",
    }

    log_info(f"Uploading data files to S3 bucket: {bucket_name}", logger)

    uploaded_count = 0
    for filename, s3_key in files_to_upload.items():
        local_path = Path(data_dir) / filename

        if not local_path.exists():
            log_error(f"File not found: {local_path}", logger)
            continue

        try:
            # Check if file already exists in S3
            try:
                s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                log_warning(f"File already exists in S3: {s3_key}", logger)
                response = input("Do you want to overwrite it? (y/N): ")
                if response.lower() != "y":
                    log_info(f"Skipping: {filename}", logger)
                    continue
            except s3_client.exceptions.NoSuchKey:
                pass  # File doesn't exist, proceed with upload

            # upload file
            s3_client.upload_file(str(local_path), bucket_name, s3_key)
            log_success(f"Uploaded: {filename} -> s3://{bucket_name}/{s3_key}", logger)
            uploaded_count += 1

        except Exception as e:
            log_error(f"Failed to upload {filename}: {e}", logger)

    log_success(f"Data upload completed! {uploaded_count} files uploaded.", logger)
    log_info(f"Files are now available at: s3://{bucket_name}/{prefix}/", logger)


def main():
    parser = argparse.ArgumentParser(
        description="Upload data files to S3 for Redshift processing"
    )
    parser.add_argument("bucket_name", help="S3 bucket name")
    parser.add_argument(
        "--data-dir", default="data", help="Local data directory (default: data)"
    )
    parser.add_argument(
        "--prefix", default="processed", help="S3 prefix/folder (default: processed)"
    )

    args = parser.parse_args()
    upload_data_to_s3(args.bucket_name, args.data_dir, args.prefix)


if __name__ == "__main__":
    main()
