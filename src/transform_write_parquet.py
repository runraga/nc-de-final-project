def upload_parquet(client, bucket, key, data):
    """
    Uploads a Pandas DataFrame as a Parquet file
    to an S3 bucket.

    Args:
        client (boto3.client): An S3 client object
        for interacting with AWS S3.
        bucket (str): The name of the S3 bucket
        to upload the Parquet file to.
        key (str): The key (object name) to use
        for the Parquet file within the S3 bucket.
        data (pd.DataFrame): The Pandas DataFrame
        to be uploaded as a Parquet file.

    Returns:
        None: The function does not return a specific value.
        It performs the upload operation directly.
    """
    data.to_parquet(path="/tmp/output.parquet")
    client.upload_file(Bucket=bucket, Key=key, Filename="/tmp/output.parquet")
