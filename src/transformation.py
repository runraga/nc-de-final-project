import logging
from urllib.request import urlopen
from json import loads
import os
import boto3
import pandas as pd
import awswrangler as wr
import botocore

s3 = boto3.client("s3")
logger = logging.getLogger()
logger.setLevel("INFO")


def lambda_handler(event, context):
    """
    Handles Lambda events triggered by S3 object
    creations, applies transformation templates
    to Parquet files, and uploads transformed files
    to another S3 bucket.

    Parameters:
    - event (dict): The Lambda event object containing
    information about the event.
    - context (LambdaContext): The Lambda execution context.

    Returns:
    - None

    Example:
    ```
    lambda_handler(event, context)
    ```

    Notes:
    - This function assumes that the event is
    triggered by an S3 object creation event.
    - It retrieves the bucket name and file key
    from the event, replaces characters in the
      file key if necessary, and determines the
      table name from the file key.
    - If the table name corresponds to a transformation
    template, it reads the Parquet file,
      applies the transformation, and uploads the
      transformed file to another S3 bucket.
    - It logs errors encountered during the process,
    including any ClientError exceptions
      from accessing S3, and raises other exceptions.
    """
    try:
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        file_key = event["Records"][0]["s3"]["object"]["key"].replace(
            "%3A", ":"
        )
        table_name = get_table_name(file_key)

        if table_name in list(tables_transformation_templates.keys()):
            # read the file - return dataframe
            df = get_df_from_parquet(file_key, bucket_name)
            # transform df due to template
            new_df = tables_transformation_templates[table_name](df)
            upload_parquet(
                s3,
                os.environ.get(
                    "S3_TRANSFORMATION_BUCKET", "test_transform_bucket"
                ),
                file_key,
                new_df,
            )

    except botocore.exceptions.ClientError as e:
        logger.error(
            """Error accessing S3 name: %s, object key: %s
                     Response error: %s, Message: %s""",
            bucket_name,
            file_key,
            e.response["Error"]["Code"],
            e.response["Error"]["Message"],
        )
    except Exception as e:
        logger.error(e)
        raise e


def get_df_from_parquet(key, bucket_name):
    """
    Reads a Parquet file from an S3 bucket into a DataFrame.

    Parameters:
    - key (str): The key (path) of the Parquet file in the S3 bucket.
    - bucket_name (str): The name of the S3 bucket.

    Returns:
    - DataFrame: A pandas DataFrame containing the
    data from the Parquet file.

    Example:
    ```
    df = get_df_from_parquet("my_file.parquet", "my_bucket")
    ```

    Notes:
    - This function reads a Parquet file located in the
    specified S3 bucket.
    - It uses the AWS Data Wrangler library (wr) to
    read the Parquet file into a DataFrame.
    - Ensure that appropriate permissions are set for accessing
    the S3 bucket.
    """
    pqt_object = [f"s3://{bucket_name}/{key}"]
    df = wr.s3.read_parquet(path=pqt_object)
    return df


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


def get_table_name(key):
    """
    Extracts the table name from a file key.

    Parameters:
    - key (str): The key (path) of the file.

    Returns:
    - str: The extracted table name.

    Example:
    ```
    table_name = get_table_name("folder/my_file.parquet")
    ```

    Notes:
    - This function assumes that the file key follows
    a specific format where the table name
      is located after the first slash and before the
      file extension.
    - It extracts the table name by removing the file
    extension and then splitting the key
      by slashes and returning the second element.
    """
    return key[:-4].split("/")[1]


def split_time(df, col_name, new_date_col_name, new_time_col_name):
    """
    Splits a datetime column into separate date and
    time columns in a DataFrame.

    Parameters:
    - df (DataFrame): The pandas DataFrame containing
    the datetime column to be split.
    - col_name (str): The name of the datetime column
    to be split.
    - new_date_col_name (str): The name for the new
    date column to be created.
    - new_time_col_name (str): The name for the new
    time column to be created.

    Returns:
    - DataFrame: A pandas DataFrame with the datetime
    column split into separate date and time columns.

    Example:
    ```
    df = split_time(my_dataframe, "datetime_column",
    "date_column", "time_column")
    ```

    Notes:
    - This function assumes that the datetime column
    specified by `col_name` contains
      datetime values that can be parsed by `pd.to_datetime`.
    - It creates new columns for the date and time
    components extracted from the original
      datetime column.
    """
    df[col_name] = pd.to_datetime(df[col_name])

    # split
    df[new_date_col_name] = df[col_name].dt.date
    df[new_time_col_name] = df[col_name].dt.time

    return df


def payment_transformation(df):
    """
    Applies transformations to a DataFrame containing payment data.

    Parameters:
    - df (DataFrame): The pandas DataFrame containing payment data.

    Returns:
    - DataFrame: A pandas DataFrame with the specified transformations applied.

    Example:
    ```
    transformed_df = payment_transformation(payment_dataframe)
    ```

    Notes:
    - This function assumes that the input DataFrame
    contains columns like 'payment_id',
      'created_at', 'last_updated', 'transaction_id',
      'counterparty_id', 'currency_id',
      and 'payment_type_id'.
    - It renames certain columns to match a specific
    naming convention for record IDs.
    - It splits datetime columns 'created_at' and
    'last_updated' into separate date and
      time columns using the 'split_time' function.
    - It drops specific columns that are no longer
    needed for analysis or have been
      replaced by renamed columns.
    """
    df["payment_record_id"] = df["payment_id"]
    df = split_time(df, "created_at", "created_date", "created_time")
    df = split_time(
        df, "last_updated", "last_updated_date", "last_updated_time"
    )
    df.rename(
        columns={"transaction_id": "transaction_record_id"}, inplace=True
    )
    df.rename(
        columns={"counterparty_id": "counterparty_record_id"}, inplace=True
    )
    df.rename(columns={"currency_id": "currency_record_id"}, inplace=True)
    df.rename(
        columns={"payment_type_id": "payment_type_record_id"}, inplace=True
    )
    df.drop("company_ac_number", axis=1, inplace=True)
    df.drop("counterparty_ac_number", axis=1, inplace=True)
    df.drop("created_at", axis=1, inplace=True)
    df.drop("last_updated", axis=1, inplace=True)
    return df


def purchase_order_transformation(df):
    """
    Applies transformation rules specific to payment data to a DataFrame.

    Parameters:
    - df (DataFrame): The pandas DataFrame containing the payment data.

    Returns:
    - DataFrame: A pandas DataFrame with applied transformations.

    Example:
    ```
    transformed_df = payment_transformation(payment_dataframe)
    ```

    Notes:
    - This function assumes that the input DataFrame contains
    payment-related columns
      such as 'payment_id', 'created_at', 'last_updated',
      etc.
    - It creates new columns, renames existing columns,
    splits datetime columns into date
      and time components, and drops unnecessary columns
      specific to payment data.
    """
    df["purchase_record_id"] = df["purchase_order_id"]
    df = split_time(df, "created_at", "created_date", "created_time")
    df = split_time(
        df, "last_updated", "last_updated_date", "last_updated_time"
    )
    df.rename(columns={"staff_id": "staff_record_id"}, inplace=True)
    df.rename(
        columns={"counterparty_id": "counterparty_record_id"}, inplace=True
    )
    df.rename(columns={"currency_id": "currency_record_id"}, inplace=True)
    df.drop("created_at", axis=1, inplace=True)
    df.drop("last_updated", axis=1, inplace=True)
    return df


def sales_order_transformation(df):
    """
    Applies transformation rules specific to sales order data to a DataFrame.

    Parameters:
    - df (DataFrame): The pandas DataFrame containing the sales order data.

    Returns:
    - DataFrame: A pandas DataFrame with applied transformations.

    Example:
    ```
    transformed_df = sales_order_transformation(sales_order_dataframe)
    ```

    Notes:
    - This function assumes that the input DataFrame
    contains sales order-related
      columns such as 'sales_order_id', 'created_at',
      'last_updated', etc.
    - It creates new columns, renames existing columns,
    splits datetime columns into date
      and time components, and drops unnecessary columns
      specific to sales order data.
    """
    df["sales_record_id"] = df["sales_order_id"]
    df = split_time(df, "created_at", "created_date", "created_time")
    df = split_time(
        df, "last_updated", "last_updated_date", "last_updated_time"
    )
    df.rename(
        columns={
            "design_id": "design_record_id",
            "staff_id": "sales_staff_id",
            "counterparty_id": "counterparty_record_id",
            "currency_id": "currency_record_id",
        },
        inplace=True,
    )
    df.drop("created_at", axis=1, inplace=True)
    df.drop("last_updated", axis=1, inplace=True)
    return df


def transform_address_table(df):
    """
    Applies transformation rules specific to an address table to a DataFrame.

    Parameters:
    - df (DataFrame): The pandas DataFrame containing the address data.

    Returns:
    - DataFrame: A pandas DataFrame with applied transformations.

    Example:
    ```
    transformed_df = transform_address_table(address_dataframe)
    ```

    Notes:
    - This function assumes that the input DataFrame contains address-related
      columns such as 'address_id', 'last_updated', 'created_at', etc.
    - It creates new columns, such as 'last_updated_date', 'last_updated_time',
      and 'location_record_id'.
    - It drops columns 'last_updated' and 'created_at'.
    """
    df["last_updated_date"] = df["last_updated"].dt.date
    df["last_updated_time"] = df["last_updated"].dt.time
    df["location_record_id"] = df["address_id"]
    df.drop(
        columns=[
            "last_updated",
            "created_at",
        ],
        inplace=True,
    )
    return df


def transform_counterparty_table(df):
    """
    Applies transformation rules specific to a counterparty table
    to a DataFrame.

    Parameters:
    - df (DataFrame): The pandas DataFrame containing the
    counterparty data.

    Returns:
    - DataFrame: A pandas DataFrame with applied transformations.

    Example:
    ```
    transformed_df = transform_counterparty_table(counterparty_dataframe)
    ```

    Notes:
    - This function assumes that the input DataFrame
    contains counterparty-related
      columns such as 'counterparty_id', 'last_updated',
      'created_at', etc.
    - It creates new columns, such as 'last_updated_date',
    'last_updated_time',
      and 'counterparty_record_id'.
    - It renames existing columns according to the provided
    rename_dict.
    - It drops columns specified in the drop list, including
    'last_updated',
      'created_at', 'legal_address_id', 'commercial_contact',
      and 'delivery_contact'.
    """
    df["last_updated_date"] = df["last_updated"].dt.date
    df["last_updated_time"] = df["last_updated"].dt.time
    df["counterparty_record_id"] = df["counterparty_id"]
    rename_dict = {
        "address_line_1": "counterparty_legal_address_line_1",
        "address_line_2": "counterparty_legal_address_line_2",
        "district": "counterparty_legal_district",
        "city": "counterparty_legal_city",
        "postal_code": "counterparty_legal_postal_code",
        "country": "counterparty_legal_country",
        "phone": "counterparty_legal_phone_number",
    }
    df.rename(columns=rename_dict, inplace=True)
    df.drop(
        columns=[
            "last_updated",
            "created_at",
            "legal_address_id",
            "commercial_contact",
            "delivery_contact",
        ],
        inplace=True,
    )
    return df


def transform_currency(df):
    """
    Applies transformation rules specific to a currency table to a DataFrame.

    Parameters:
    - df (DataFrame): The pandas DataFrame containing the currency data.

    Returns:
    - DataFrame: A pandas DataFrame with applied transformations.

    Example:
    ```
    transformed_df = transform_currency(currency_dataframe)
    ```

    Notes:
    - This function assumes that the input DataFrame contains currency-related
      columns such as 'currency_id', 'currency_code',
      'last_updated', 'created_at', etc.
    - It retrieves currency data from a remote JSON file
    and creates a DataFrame from it.
    - It merges the currency DataFrame with the input DataFrame
    based on the 'currency_code'
      column, assuming a many-to-one relationship.
    - It creates new columns, such as 'last_updated_date', 'last_updated_time',
      and 'currency_record_id'.
    - It drops columns 'last_updated' and 'created_at'
    from the input DataFrame.
    """
    url = "https://raw.githubusercontent.com/umpirsky/currency-list/master/data/en_GB/currency.json"  # noqa
    response = urlopen(url)
    json_raw = response.read().decode("utf-8")
    currencies = loads(json_raw)
    curr_ls = [
        {
            "currency_code": k,
            "currency_name": v,
        }
        for k, v in currencies.items()
    ]

    currency_df = pd.DataFrame(data=curr_ls)
    df = df.merge(currency_df, how="left", on="currency_code", validate="m:1")
    df["last_updated_date"] = df["last_updated"].dt.date
    df["last_updated_time"] = df["last_updated"].dt.time
    df["currency_record_id"] = df["currency_id"]
    df.drop(columns=["last_updated", "created_at"], inplace=True)
    return df


def transform_design_table(df):
    """
    Applies transformation rules specific to a design table to a DataFrame.

    Parameters:
    - df (DataFrame): The pandas DataFrame containing the design data.

    Returns:
    - DataFrame: A pandas DataFrame with applied transformations.

    Example:
    ```
    transformed_df = transform_design_table(design_dataframe)
    ```

    Notes:
    - This function assumes that the input DataFrame contains design-related
      columns such as 'design_id', 'last_updated', 'created_at', etc.
    - It creates new columns, such as 'last_updated_date', 'last_updated_time',
      and 'design_record_id'.
    - It drops columns 'last_updated' and 'created_at'
    from the input DataFrame.
    """
    df["last_updated_date"] = df["last_updated"].dt.date
    df["last_updated_time"] = df["last_updated"].dt.time
    df["design_record_id"] = df["design_id"]
    df.drop(columns=["last_updated", "created_at"], inplace=True)
    return df


def transform_payment_type_table(df):
    """
    Applies transformation rules specific to a payment
    type table to a DataFrame.

    Parameters:
    - df (DataFrame): The pandas DataFrame containing the payment type data.

    Returns:
    - DataFrame: A pandas DataFrame with applied transformations.

    Example:
    ```
    transformed_df = transform_payment_type_table(payment_type_dataframe)
    ```

    Notes:
    - This function assumes that the input DataFrame
    contains payment type-related
      columns such as 'payment_type_id', 'last_updated', 'created_at', etc.
    - It creates new columns, such as 'last_updated_date', 'last_updated_time',
      'payment_type_record_id', and 'payment_record_id'.
    - It drops columns 'last_updated', 'created_at', and 'payment_type_id'
      from the input DataFrame.
    """
    df["last_updated_date"] = df["last_updated"].dt.date
    df["last_updated_time"] = df["last_updated"].dt.time
    df["payment_type_record_id"] = df["payment_type_id"]
    df["payment_record_id"] = df["payment_type_id"]
    df.drop(
        columns=["last_updated", "created_at", "payment_type_id"], inplace=True
    )
    return df


def transform_staff_table(df):
    """
    Applies transformation rules specific to a staff table to a DataFrame.

    Parameters:
    - df (DataFrame): The pandas DataFrame containing the staff data.

    Returns:
    - DataFrame: A pandas DataFrame with applied transformations.

    Example:
    ```
    transformed_df = transform_staff_table(staff_dataframe)
    ```

    Notes:
    - This function assumes that the input DataFrame contains staff-related
      columns such as 'staff_id', 'last_updated', 'created_at', etc.
    - It creates new columns, such as 'last_updated_date', 'last_updated_time',
      and 'staff_record_id'.
    - It drops columns 'last_updated', 'created_at', and 'department_id'
      from the input DataFrame.
    """
    df["last_updated_date"] = df["last_updated"].dt.date
    df["last_updated_time"] = df["last_updated"].dt.time
    df["staff_record_id"] = df["staff_id"]
    df.drop(
        columns=["last_updated", "created_at", "department_id"], inplace=True
    )
    return df


def transform_transaction_table(df):
    """
    Applies transformation rules specific to a transaction
    table to a DataFrame.

    Parameters:
    - df (DataFrame): The pandas DataFrame containing the transaction data.

    Returns:
    - DataFrame: A pandas DataFrame with applied transformations.

    Example:
    ```
    transformed_df = transform_transaction_table(transaction_dataframe)
    ```

    Notes:
    - This function assumes that the input DataFrame
    contains transaction-related
      columns such as 'transaction_id', 'last_updated',
      'created_at', etc.
    - It creates new columns, such as 'last_updated_date',
    'last_updated_time',
      and 'transaction_record_id'.
    - It replaces NaN values with None.
    - It drops columns 'last_updated' and 'created_at'
    from the input DataFrame.
    """
    df["last_updated_date"] = df["last_updated"].dt.date
    df["last_updated_time"] = df["last_updated"].dt.time
    df["transaction_record_id"] = df["transaction_id"]
    df.drop(
        columns=[
            "last_updated",
            "created_at",
        ],
        inplace=True,
    )
    return df


tables_transformation_templates = {
    "payment": payment_transformation,
    "purchase_order": purchase_order_transformation,
    "sales_order": sales_order_transformation,
    "address": transform_address_table,
    "counterparty": transform_counterparty_table,
    "currency": transform_currency,
    "design": transform_design_table,
    "payment_type": transform_payment_type_table,
    "staff": transform_staff_table,
    "transaction": transform_transaction_table,
}
