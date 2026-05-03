import boto3

s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9002",
    aws_access_key_id="aml_minio_user",
    aws_secret_access_key="aml_minio_pass",
)

BUCKET_NAME = "documents"