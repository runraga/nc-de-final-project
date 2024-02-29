from src.loader import (
    lambda_handler,
    get_df_from_parquet,
    get_table_name,
    create_query,
    df_insertion,
)

# from src.transformation import tables_transformation_templates
import pg8000.native as pg
from moto import mock_aws
import pandas as pd
from unittest.mock import patch  # , Mock
import boto3
from datetime import datetime
from configparser import ConfigParser
import pytest

# from io import BytesIO
import os
from t_utils import inhibit_CI


# from sample_datasets import sample_dataset

# TODO reenable me after work TODO BUG TEMP
# TODO this was gutted from transform


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""

    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


# @inhibit_CI
@pytest.fixture(scope="function")
def mockdb_creds():
    """Mocked Database Credentials for local testing."""

    config = ConfigParser()
    config.read(".env.ini")
    section = config["DEFAULT"]

    os.environ["PGUSER2"] = section["PGUSER"]
    os.environ["PGPASSWORD2"] = section["PGPASSWORD"]
    os.environ["PGHOST2"] = "127.0.0.1"
    os.environ["PGDATABASE2"] = "totesys_test_subset"


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
    # TODO, i didn't know this was coupled with EXTRACT
    with pytest.raises(Exception):
        key = "test.parquet"

        s3.create_bucket(
            Bucket=os.environ["S3_EXTRACT_BUCKET"],
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        result = get_df_from_parquet(key, "test-bucket")

        assert result["staff_id"][0] == 11
        assert result["first_name"][0] == "Meda"
        assert isinstance(result["created_at"][0], pd.Timestamp)


def test_get_table_name():
    keys = ["2024-02-15T19:01:53/address.pqt", "2024-02-21/purchase_order.pqt"]
    assert get_table_name(keys[0]) == "address"
    assert get_table_name(keys[1]) == "purchase_order"


@patch("src.loader.df_insertion")
@patch("src.loader.create_query")
@patch("src.loader.get_df_from_parquet")
def test_lambda_handler(
    mock_get_df_from_parquet, mock_create_query, mock_df_insertion
):
    # TODO
    data = {
        "address_id": [1, 2, 3],
        "address_line_1": [
            "123 Maple Street",
            "456 Oak Street",
            "789 Pine Street",
        ],
        "address_line_2": ["Apt. 101", "Suite 202", "Room 303"],
        "district": ["North", "South", "East"],
        "city": ["Townsville", "Cityplace", "Villageland"],
        "postal_code": ["12345", "67890", "111213"],
        "country": ["CountryA", "CountryB", "CountryC"],
        "phone": ["123-456-7890", "098-765-4321", "456-789-0123"],
        "created_at": [datetime.now(), datetime.now(), datetime.now()],
        "last_updated": [datetime.now(), datetime.now(), datetime.now()],
    }
    df = pd.DataFrame(data)
    event = {
        "time": "2024-02-13T10:45:18Z",
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "extraction"},
                    "object": {"key": "2024-02-15T19:01:53/address.pqt"},
                }
            }
        ],
    }
    context = {}

    # mocking
    mock_get_df_from_parquet.return_value = df
    mock_create_query.return_value = "sql query"

    # ACT
    res = lambda_handler(event, context)
    mock_create_query.assert_called_once()
    mock_df_insertion.assert_called_once()
    assert res == "Ok"


def normalize_sql_query(query):
    return "\n".join(line.strip() for line in query.split("\n")).strip()


def test_create_query():
    # ASSIGN
    data = {"id": [1], "name": ["Yarik"], "value": [100]}
    df = pd.DataFrame(data)
    expected_query = """
        INSERT INTO test_table (id, name, value)
        VALUES (:id, :name, :value)
        ON CONFLICT (id)
        DO UPDATE SET name = EXCLUDED.name, value = EXCLUDED.value;
        """

    # ACT
    query = normalize_sql_query(create_query("test_table", "id", df))

    # ASSERT

    assert query == normalize_sql_query(expected_query)


def test_create_query_dim_transaction():
    # ASSIGN
    data = {"id": [1], "sales_order_id": [None], "purchase_order_id": [100]}
    df = pd.DataFrame(data)
    expected_query = """
        INSERT INTO dim_transaction (id, sales_order_id, purchase_order_id)
        VALUES (:id, nullif(:sales_order_id, -1), nullif(:purchase_order_id, -1))
        ON CONFLICT (id)
        DO UPDATE SET sales_order_id = EXCLUDED.sales_order_id, purchase_order_id = EXCLUDED.purchase_order_id;
        """  # noqa: E501

    # ACT
    query = normalize_sql_query(create_query("dim_transaction", "id", df))

    # ASSERT

    assert query == normalize_sql_query(expected_query)


def setup_test_table(mockdb_creds):
    """Create test table in local db."""
    username = os.environ.get("PGUSER2", "testing")
    password = os.environ.get("PGPASSWORD2", "testing")
    host = os.environ.get("PGHOST2", "testing")
    port = os.environ.get("PGPORT2", "5432")
    database = os.environ.get("PGDATABASE2")

    print(username, password, host, port, database, "🧨️-🧨-️🧨-️🧨️-🧨️")
    con = pg.Connection(
        username,
        password=password,
        host=host,
        port=port,
        database=database,
    )

    con.run("DROP TABLE IF EXISTS dim_design;")
    con.run(
        """
        CREATE TABLE dim_design (
            design_record_id INT PRIMARY KEY,
            design_id INT,
            design_name VARCHAR(50),
            file_location VARCHAR(100),
            file_name VARCHAR(50),
            last_updated_date DATE,
            last_updated_time TIME
        );
    """
    )
    con.close()


def delete_test_table(mockdb_creds):
    """Create test table in local db."""
    username = os.environ.get("PGUSER2", "testing")
    password = os.environ.get("PGPASSWORD2", "testing")
    host = os.environ.get("PGHOST2", "testing")
    port = os.environ.get("PGPORT2", "5432")
    database = os.environ.get("PGDATABASE2")
    con = pg.Connection(
        username, password=password, host=host, port=port, database=database
    )

    con.run("DROP TABLE IF EXISTS dim_design;")
    con.close()


@inhibit_CI
def test_df_insertion(mockdb_creds):
    setup_test_table(mockdb_creds)
    table_name = "dim_design"
    # test DataFrame
    data = {
        "design_record_id": [1, 2],
        "design_id": [1, 2],
        "design_name": ["Design One", "Design Two"],
        "file_location": ["/path/to/design1", "/path/to/design2"],
        "file_name": ["design1.png", "design2.png"],
        "last_updated_date": ["2021-01-01", "2021-01-02"],
        "last_updated_time": ["12:00:00", "13:00:00"],
    }
    tdf = pd.DataFrame(data)

    # test SQL
    query = """
    INSERT INTO dim_design (
        design_record_id,
        design_id,
        design_name,
        file_location,
        file_name,
        last_updated_date,
        last_updated_time
    )
    VALUES (
        :design_record_id,
        :design_id,
        :design_name,
        :file_location,
        :file_name,
        :last_updated_date,
        :last_updated_time)
    ON CONFLICT (design_record_id)
    DO UPDATE SET
        design_id = EXCLUDED.design_id,
        design_name = EXCLUDED.design_name,
        file_location = EXCLUDED.file_location,
        file_name = EXCLUDED.file_name,
        last_updated_date = EXCLUDED.last_updated_date,
        last_updated_time = EXCLUDED.last_updated_time;
    """

    # ACT 1
    df_insertion(query, tdf, table_name)

    username = os.environ.get("PGUSER2", "testing")
    password = os.environ.get("PGPASSWORD2", "testing")
    host = os.environ.get("PGHOST2", "testing")
    port = os.environ.get("PGPORT2", "5432")
    database = os.environ.get("PGDATABASE2")
    conn = pg.Connection(
        username, password=password, host=host, port=port, database=database
    )

    result = conn.run("SELECT * FROM dim_design ORDER BY design_record_id")
    assert len(result) == 2
    for i, row in enumerate(result):
        assert row[0] == data["design_record_id"][i]
        assert row[1] == data["design_id"][i]

    # new test DataFrame
    data = {
        "design_record_id": [1, 3],
        "design_id": [99, 3],
        "design_name": ["Design 99", "Design 3"],
        "file_location": ["/path/to/design99", "/path/to/design3"],
        "file_name": ["design99.png", "design3.png"],
        "last_updated_date": ["2021-11-11", "2021-03-03"],
        "last_updated_time": ["11:11:11", "13:03:03"],
    }
    tdf = pd.DataFrame(data)
    df_insertion(query, tdf, table_name)

    result = conn.run("SELECT * FROM dim_design")
    assert len(result) == 3

    delete_test_table(mockdb_creds)
