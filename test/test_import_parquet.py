import os
from moto import mock_aws
import pandas as pd
import pytest
import boto3
from src.extractor import upload_parquet
from src.transform_get_df_from_parquet import get_df_from_parquet


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""

    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@mock_aws
@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture(scope="function")
def import_parquet_file():
    parquet_file = "./test/parquet_test_files/test.pqt"
    df = pd.read_parquet(parquet_file, engine="pyarrow")
    return df


def test_get_df_from_parquet(s3, import_parquet_file):
    os.environ["S3_EXTRACT_BUCKET"] = "test-bucket"
    key = "test.parquet"

    data = import_parquet_file

    s3.create_bucket(
        Bucket=os.environ["S3_EXTRACT_BUCKET"],
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    upload_parquet(s3, os.environ["S3_EXTRACT_BUCKET"], key, data)

    result = get_df_from_parquet(key)

    assert result["staff_id"][0] == 11
    assert result["first_name"][0] == "Meda"
    assert isinstance(result["created_at"][0], pd.Timestamp)
