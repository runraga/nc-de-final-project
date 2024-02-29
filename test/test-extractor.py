from datetime import datetime
from unittest.mock import Mock, patch
from configparser import ConfigParser
from io import BytesIO
import os
from botocore.exceptions import ClientError
import pandas as pd
import boto3
import pytest
from sample_datasets import sample_dataset
from moto import mock_aws
from t_utils import inhibit_CI
from src.extractor import (
    get_last_updated_time,
    set_last_updated_time,
    get_query,
)
from src.extractor import lambda_handler
from src.extractor import rows_to_dict, upload_parquet
from src.extractor import extract


class SAME_DF:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def __eq__(self, other):
        return isinstance(other, pd.DataFrame) and other.equals(self.df)


class TestRowstoDict:
    def test_empty(self):
        """
        rows to dict all empties test
        """
        items = []
        columns = []
        expected = []

        actual = rows_to_dict(items, columns)

        assert actual == expected

    def test_single(self):
        """
        rows to dict single item single column test
        """
        items = [[1]]
        columns = [{"name": "a"}]
        expected = [{"a": 1}]

        actual = rows_to_dict(items, columns)

        assert actual == expected

    def test_multiple(self):
        """
        rows to dict full table test
        """
        items = [[1, 2, "AAA"], [4, 5, "BBB"]]
        columns = [{"name": "a"}, {"name": "b"}, {"name": "c"}]
        expected = [{"a": 1, "b": 2, "c": "AAA"}, {"a": 4, "b": 5, "c": "BBB"}]

        actual = rows_to_dict(items, columns)

        assert actual == expected


@inhibit_CI
@pytest.fixture(scope="function")
def mockdb_creds():
    """Mocked Database Credentials for local testing."""

    config = ConfigParser()
    config.read(".env.ini")
    section = config["DEFAULT"]

    os.environ["PGUSER"] = section["PGUSER"]
    os.environ["PGPASSWORD"] = section["PGPASSWORD"]
    os.environ["PGHOST"] = "127.0.0.1"
    os.environ["PGDATABASE"] = "totesys_test_subset"


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


@patch("src.extractor.upload_parquet")
def test_extract(upload):
    """
    tests mocked db extraction
    """
    client = "s3"
    datedate = "2024-02-13T10:45:18"

    os.environ["S3_EXTRACT_BUCKET"] = "ingestion"

    conn = Mock()
    conn.run.return_value = [[1, 2, "AAA"], [4, 5, "BBB"]]
    conn.columns = [{"name": "a"}, {"name": "b"}, {"name": "c"}]
    table = "cat"
    time = datetime.fromisoformat(datedate)

    df = pd.DataFrame(
        data=[{"a": 1, "b": 2, "c": "AAA"}, {"a": 4, "b": 5, "c": "BBB"}]
    )

    key = f"{datedate}/{table}.pqt"

    extract(client, conn, "ingestion", table, time, time)

    upload.assert_called_with(client, "ingestion", key, SAME_DF(df))


@mock_aws
@patch("src.extractor.set_last_updated_time")
@patch("src.extractor.get_last_updated_time")
@patch("src.extractor.client")
@patch("src.extractor.extract")
@patch("src.extractor.pg.Connection")
def test_lambda_handler(
    conn, MockExtract, client, get_last_updated_time, set_last_updated_time
):
    """
    tests mocked db lambda handler
    """
    time = datetime.fromisoformat("2024-02-13T10:45:18Z")
    since = datetime.fromisoformat("2024-01-01T00:00:00")
    connMock = Mock()
    get_last_updated_time.return_value = since

    event = {"time": time.isoformat()}
    context = ""

    get_last_updated_time.return_value = None

    client.return_value = "s3"
    conn.return_value = connMock
    connMock.run.return_value = [["address"]]

    lambda_handler(event, context)

    MockExtract.assert_called_with(
        "s3", connMock, "ingestion", "address", time, None
    )


def test_lambda_handler_exceptions():
    """
    tests mocked db lambda handler
    """

    event = {"not_time": "not a time"}
    context = ""

    with pytest.raises(Exception):
        lambda_handler(event, context)


@mock_aws
@inhibit_CI
def test_integrate(s3, mockdb_creds):
    """
    tests local db lambda handler
    """
    TABLES = [
        "currency",
        "payment",
        "department",
        "transaction",
        "design",
        "address",
        "staff",
        "counterparty",
        "purchase_order",
        "payment_type",
        "sales_order",
    ]
    event = {"time": "2025-01-01T10:45:18Z"}
    # ACT
    s3.create_bucket(
        Bucket="ingestion",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    s3.create_bucket(
        Bucket="control_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    lambda_handler(event, "")

    objects = s3.list_objects_v2(Bucket="ingestion")
    files = [file["Key"] for file in objects["Contents"]]

    assert len(files) == 11

    for table in TABLES:
        expected_file_name = f"2025-01-01T10:45:18/{table}.pqt"
        assert expected_file_name in files

        expected_table = sample_dataset[table]
        # print(expected_table[0], table, '-------------------------------')

        resp = s3.get_object(
            Bucket="ingestion", Key=f"2025-01-01T10:45:18/{table}.pqt"
        )
        df = pd.read_parquet(BytesIO(resp["Body"].read()))
        existing_table = df.to_records()

        for i, row in enumerate(existing_table):
            if table not in ["counterparty", "staff"]:
                assert len(row) == len(expected_table[i]) + 1


@mock_aws
def test_read_last_updated(s3):
    bucket = "control_bucket"
    date = datetime.now()

    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    s3.put_object(
        Bucket=bucket,
        Key="last_successful_extraction.txt",
        Body=str(date.timestamp()),
    )

    actual = get_last_updated_time(s3)

    assert actual == date


@mock_aws
def test_read_last_updated_none(s3):
    bucket = "control_bucket"
    date = None

    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    actual = get_last_updated_time(s3)

    assert actual == date


@mock_aws
def test_read_last_updated_reraise(s3):

    with pytest.raises(Exception):

        get_last_updated_time(s3)


def test_set_last_updated_success():
    bucket = "control_bucket"
    date = datetime.now()

    client = Mock()

    client.put_object().return_value = None

    set_last_updated_time(client, date)

    client.put_object.assert_called_with(
        Bucket=bucket,
        Key="last_successful_extraction.txt",
        Body=str(date.timestamp()),
    )


def normalize_sql_query(query):
    return "\n".join(line.strip() for line in query.split("\n")).strip()


def test_get_queries():
    expected_output = [
        normalize_sql_query(
            """SELECT t.*,
            d.department_name, d.location
            FROM staff as t
            LEFT JOIN department d ON t.department_id = d.department_id
            WHERE t.last_updated >= '2022-02-02'
            AND t.last_updated < '2025-01-01';"""
        ),
        normalize_sql_query(
            """
            SELECT t.*,
            a.address_line_1,
            a.address_line_2,
            a.district,
            a.city,
            a.postal_code,
            a.country,
            a.phone
            FROM counterparty t
            LEFT JOIN address a on t.legal_address_id = a.address_id
            WHERE t.last_updated >= '2022-02-02'
            AND t.last_updated < '2025-01-01';"""
        ),
        normalize_sql_query(
            """
            SELECT * FROM design as t
            WHERE t.last_updated >= '2022-02-02'
            AND t.last_updated < '2025-01-01';
            """
        ),
    ]
    assert (
        normalize_sql_query(get_query("staff", "2022-02-02", "2025-01-01"))
        == expected_output[0]
    )
    assert (
        normalize_sql_query(
            get_query("counterparty", "2022-02-02", "2025-01-01")
        )
        == expected_output[1]
    )
    assert (
        normalize_sql_query(get_query("design", "2022-02-02", "2025-01-01"))
        == expected_output[2]
    )


@inhibit_CI
@patch("src.extractor.get_last_updated_time")
def test_database_error(mock_get_time, caplog, mockdb_creds):
    mock_get_time.return_value = "not a time"

    event = {"time": "2024-02-13T10:45:18Z"}

    lambda_handler(event, {})

    assert "pg8000 error:" in caplog.text


@inhibit_CI
@patch("src.extractor.upload_parquet")
def test_client_error(mock_upload_parquet, caplog, mockdb_creds):
    error = {"Error": {"Code": "404", "Message": "Bucket not found"}}

    mock_upload_parquet.side_effect = ClientError(
        error, "SomeServiceException"
    )
    event = {"time": "2024-02-13T10:45:18Z"}

    lambda_handler(event, {})
    assert "Error accessing S3 bucket:" in caplog.text
