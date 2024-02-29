import boto3
import logging
import pg8000.native as pg

# import pandas as pd
# from io import BytesIO
import awswrangler as wr
from os import environ
import numpy as np

s3 = boto3.client("s3")
logger = logging.getLogger()
logger.setLevel("INFO")


table_relations = {
    "currency": ("dim_currency", "currency_record_id"),
    "payment": ("fact_payment", "payment_record_id"),
    "transaction": ("dim_transaction", "transaction_record_id"),
    "design": ("dim_design", "design_record_id"),
    "address": ("dim_location", "location_record_id"),
    "staff": ("dim_staff", "staff_record_id"),
    "counterparty": ("dim_counterparty", "counterparty_record_id"),
    "purchase_order": ("fact_purchase_order", "purchase_record_id"),
    "payment_type": ("dim_payment_type", "payment_type_record_id"),
    "sales_order": ("fact_sales_order", "sales_record_id"),
}


def lambda_handler(event, context):
    """
    Handles Lambda events triggered by S3 object creations,
    processes Parquet files,
    and inserts data into a database table.

    Parameters:
    - event (dict): The Lambda event object containing
    information about the event.
    - context (LambdaContext): The Lambda execution context.

    Returns:
    - str: A status message indicating the success or
    failure of the operation.

    Example:
    ```
    lambda_handler(event, context)
    ```

    Notes:
    - This function assumes that the event is triggered
    by an S3 object creation event.
    - It processes Parquet files from the specified S3 bucket
    and inserts the data into
      the appropriate database table based on predefined
      table relations.
    - If successful, it returns 'Ok'. If an error occurs,
    it logs the error and does not
      raise an exception.
    """
    try:
        # get bucket
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        file_key = event["Records"][0]["s3"]["object"]["key"].replace(
            "%3A", ":"
        )
        table_name = get_table_name(file_key)
        # get dataframe
        logger.info(f"📂 Processing file {file_key} from bucket {bucket_name}")
        df = get_df_from_parquet(file_key, bucket_name)
        # get db_table_name and primary_key
        table_name, primary_key = table_relations[table_name]
        # create query template for specific table fill with placeholders
        sql_query_template = create_query(table_name, primary_key, df)
        # insert
        logger.info(f"🚀 Executing SQL query on table {table_name}")
        df_insertion(sql_query_template, df, table_name)
        logger.info(f"✅ Successfully inserted data into {table_name}")
        return "Ok"
    except Exception as e:
        logger.error(f"❌ Failed to process file: {str(e)}")


def create_query(table_name, primary_key, df):
    """
    Creates an SQL query template for inserting data
    into a specified table.

    Parameters:
    - table_name (str): The name of the database table.
    - primary_key (str): The name of the primary key column.
    - df (DataFrame): The pandas DataFrame containing
    the data to be inserted.

    Returns:
    - str: An SQL query template for inserting data
    into the specified table.

    Example:
    ```
    sql_query_template = create_query("my_table",
    "id", my_dataframe)
    ```

    Notes:
    - This function generates an SQL query template
    for inserting data into a PostgreSQL
      database table using the DataFrame's column
      names as placeholders for values.
    - It assumes that the primary key column is
    specified and that conflicts are resolved
      by updating existing rows.
    """
    columns = list(df.columns)
    placeholders = ", ".join([f":{col}" for col in columns])
    assignments = ", ".join(
        [f"{col} = EXCLUDED.{col}" for col in columns if col != primary_key])

    sql_query_template = f"""
    INSERT INTO {table_name} ({', '.join(columns)})
    VALUES ({placeholders})
    ON CONFLICT ({primary_key})
    DO UPDATE SET {assignments};
    """
    if table_name == "dim_transaction":
        sql_query_template = sql_query_template.replace(
            ":sales_order_id", "nullif(:sales_order_id, -1)"
        )
        sql_query_template = sql_query_template.replace(
            ":purchase_order_id", "nullif(:purchase_order_id, -1)"
        )
    return sql_query_template


def get_df_from_parquet(key, bucket_name):
    """
    Reads a Parquet file from an S3 bucket into a DataFrame.

    Parameters:
    - key (str): The key (path) of the Parquet file
    in the S3 bucket.
    - bucket_name (str): The name of the S3 bucket.

    Returns:
    - DataFrame: A pandas DataFrame containing the
    data from the Parquet file.

    Example:
    ```
    df = get_df_from_parquet("my_file.parquet", "my_bucket")
    ```

    Notes:
    - This function reads a Parquet file located
    in the specified S3 bucket.
    - It uses the AWS Data Wrangler library (wr)
    to read the Parquet file into a DataFrame.
    - Ensure that appropriate permissions are set
    for accessing the S3 bucket.
    """
    pqt_object = [f"s3://{bucket_name}/{key}"]
    df = wr.s3.read_parquet(path=pqt_object)
    return df


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


def df_insertion(query, df, table_name):
    """
    Inserts data from a DataFrame into a PostgreSQL database
    table using the provided query.

    Parameters:
    - query (str): The SQL query template for inserting
    data into the database table.
    - df (DataFrame): The pandas DataFrame containing the
    data to be inserted.
    - table_name (str): The name of the database table.

    Returns:
    - str: A status message indicating the success of the data
    insertion process.

    Example:
    ```
    status_message = df_insertion("INSERT INTO my_table (...)
    VALUES (...) ON CONFLICT ...", my_dataframe, "my_table")
    ```

    Notes:
    - This function assumes that the DataFrame columns
    match the columns in the database table
      specified in the query.
    - It uses environment variables to retrieve connection
    parameters for accessing the PostgreSQL
      database.
    - The data insertion process is performed row by row
    using the prepared statement.
    """
    try:
        username = environ.get("PGUSER2", "testing")
        password = environ.get("PGPASSWORD2", "testing")
        host = environ.get("PGHOST2", "testing")
        port = environ.get("PGPORT2", "5432")
        database = environ.get("PGDATABASE2")
        if table_name == "dim_transaction":
            df = df.replace({np.nan: -1})
            df = df.astype(
                {col: "int64" for col in df.select_dtypes("float64").columns}
            )
        with pg.Connection(
            username,
            password=password,
            host=host,
            port=port,
            database=database,
        ) as con:
            ps = con.prepare(query)
            for _, row in df.iterrows():
                logger.info(str(row.to_dict()))
                ps.run(**row.to_dict())
            #
        return f"{table_name} Loaded ✅️🤘️"
    except Exception as e:
        logger.error(f"❗ Failed to insert data into {table_name}: {str(e)}")
