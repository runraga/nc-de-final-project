import pandas as pd
import datetime
import pytest
from src.transform_date_time import get_date_and_time


@pytest.fixture(scope="function")
def import_parquet_file():
    parquet_file = "./test/parquet_test_files/test.pqt"
    df = pd.read_parquet(parquet_file, engine="pyarrow")
    return df


def test_returns_date_and_time_objects(import_parquet_file):
    df = import_parquet_file
    expected_date = datetime.date(2022, 11, 3)
    expected_time = datetime.time(14, 20, 51, 563000)

    result_date, result_time = get_date_and_time(df["created_at"][0])
    assert result_date == expected_date
    assert result_time == expected_time

    expected_date = datetime.date(2022, 11, 3)
    expected_time = datetime.time(14, 20, 51, 563000)

    result_date, result_time = get_date_and_time(df["last_updated"][0])
    assert result_date == expected_date
    assert result_time == expected_time
