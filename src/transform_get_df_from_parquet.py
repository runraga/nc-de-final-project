import awswrangler as wr
import os


def get_df_from_parquet(key):
    """
    Reads a DataFrame from a Parquet file stored
    in an S3 bucket.

    Parameters:
    - key (str): The key or path to the Parquet file
    in the S3 bucket.

    Returns:
    - DataFrame: A pandas DataFrame containing the
    data read from the Parquet file.

    Example:
    ```
    dataframe = get_df_from_parquet("my_file.parquet")
    ```
    """
    bucket = os.environ["S3_EXTRACT_BUCKET"]
    pqt_object = [f"s3://{bucket}/{key}"]
    df = wr.s3.read_parquet(path=pqt_object)
    return df
