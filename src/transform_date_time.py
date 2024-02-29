import pandas


def get_date_and_time(timestamp: pandas.Timestamp):
    """
    Extracts the date and time components from
    a pandas Timestamp object.

    Parameters:
    - timestamp (pandas.Timestamp): The timestamp
    from which to extract date and time.

    Returns:
    - date: The date component extracted
    from the timestamp.
    - time: The time component extracted
    from the timestamp.

    Example:
    ```
    date, time = get_date_and_time(my_timestamp)
    ```

    Notes:
    - This function assumes that the input
    timestamp is a pandas Timestamp object.
    - It returns the date and time components
    extracted from the input timestamp.
    """
    return timestamp.date(), timestamp.time()
