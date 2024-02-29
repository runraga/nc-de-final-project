import os
from moto import mock_aws
import boto3
import pandas as pd
import pytest
from transform_write_parquet import upload_parquet


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""

    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    """
    S3 client fixture, presents an S3 client
    """
    with mock_aws():
        yield boto3.client("s3")


@mock_aws
def test_upload_parquet(s3):
    """
    tests upload_parquet functionality
    """
    bucket = "test-bucket"
    key = "test.parquet"

    data = pd.DataFrame([{"a": 1}])

    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    upload_parquet(s3, bucket, key, data)

    objects = s3.list_objects_v2(Bucket=bucket)

    files = [file["Key"] for file in objects["Contents"]]

    assert key in files
