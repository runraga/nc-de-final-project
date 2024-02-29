resource "aws_cloudwatch_log_metric_filter" "extraction_cloudwatch_log_group" {
    /*
    Creates a CloudWatch Logs metric filter to monitor logs for the occurrence of the "ERROR" pattern in the specified log group.

    Args:
        name (str): The name of the CloudWatch Logs metric filter.
        pattern (str): The filter pattern to search for in the logs.
        log_group_name (str): The name of the CloudWatch Logs group to associate the metric filter with.

    Returns:
        None
    */
    name = "ReadError"
    pattern = "ERROR"
    log_group_name = aws_cloudwatch_log_group.extraction_log_group.name
    metric_transformation {
      name = "ErrorMetric"
      namespace = "ExtractionLambdaError"
      value = 1
    }
    
}
resource "aws_cloudwatch_log_metric_filter" "transformation_cloudwatch_log_group" {
    /*
    Creates a CloudWatch Logs metric filter to monitor logs for the occurrence of the "ERROR" pattern in the specified log group.

    Args:
        name (str): The name of the CloudWatch Logs metric filter.
        pattern (str): The filter pattern to search for in the logs.
        log_group_name (str): The name of the CloudWatch Logs group to associate the metric filter with.

    Returns:
        None
    */
    name = "ReadError"
    pattern = "ERROR"
    log_group_name = aws_cloudwatch_log_group.transformation_log_group.name
    metric_transformation {
      name = "ErrorMetric"
      namespace = "TransformationLambdaError"
      value = 1
    }
    
}
resource "aws_cloudwatch_log_metric_filter" "loader_cloudwatch_log_group" {
    /*
    Creates a CloudWatch Logs metric filter to monitor logs for the occurrence of the "ERROR" pattern in the specified log group.

    Args:
        name (str): The name of the CloudWatch Logs metric filter.
        pattern (str): The filter pattern to search for in the logs.
        log_group_name (str): The name of the CloudWatch Logs group to associate the metric filter with.

    Returns:
        None
    */
    name = "ReadError"
    pattern = "ERROR"
    log_group_name = aws_cloudwatch_log_group.loader_log_group.name
    metric_transformation {
      name = "ErrorMetric"
      namespace = "LoaderLambdaError"
      value = 1
    }
    
}



resource "aws_cloudwatch_metric_alarm" "extraction_alert_errors" {
    /* 
    Creates a CloudWatch metric alarm to monitor the "ErrorMetric" metric in the "ExtractionLambdaError" namespace and trigger an alert when the value is greater than or equal to the specified threshold.

    Args:
        alarm_name (str): The name of the CloudWatch metric alarm.
        comparison_operator (str): The comparison operator used to evaluate the alarm condition.
        metric_name (str): The name of the metric to monitor.
        namespace (str): The namespace of the metric.
        evaluation_periods (int): The number of periods over which data is compared to the threshold.
        period (int): The length of time, in seconds, over which the specified  statistic is applied.
        statistic (str): The statistic to apply to the metric data.
        threshold (float): The value against which the specified statistic is compared.
        alarm_description (str): The description of the alarm.
        alarm_actions (list): The list of ARNs of the actions to take when the alarm transitions to the ALARM state.
        actions_enabled (str): Indicates whether actions should be executed during any changes to the alarm's state.

    Returns:
        None
    */

  alarm_name          = "ExtractionErrorAlert"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  metric_name         = "ErrorMetric"
  namespace           = "ExtractionLambdaError"
  evaluation_periods  = 1
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "This triggers when ExtractionErrorAlert is triggered in a minute."
  alarm_actions = ["arn:aws:sns:eu-west-2:730335327822:ConnectionErrorTest"]
  actions_enabled = "true"
} 
resource "aws_cloudwatch_metric_alarm" "transformation_alert_errors" {
    /* 
    Creates a CloudWatch metric alarm to monitor the "ErrorMetric" metric in the "ExtractionLambdaError" namespace and trigger an alert when the value is greater than or equal to the specified threshold.

    Args:
        alarm_name (str): The name of the CloudWatch metric alarm.
        comparison_operator (str): The comparison operator used to evaluate the alarm condition.
        metric_name (str): The name of the metric to monitor.
        namespace (str): The namespace of the metric.
        evaluation_periods (int): The number of periods over which data is compared to the threshold.
        period (int): The length of time, in seconds, over which the specified  statistic is applied.
        statistic (str): The statistic to apply to the metric data.
        threshold (float): The value against which the specified statistic is compared.
        alarm_description (str): The description of the alarm.
        alarm_actions (list): The list of ARNs of the actions to take when the alarm transitions to the ALARM state.
        actions_enabled (str): Indicates whether actions should be executed during any changes to the alarm's state.

    Returns:
        None
    */

  alarm_name          = "TransformErrorAlert"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  metric_name         = "ErrorMetric"
  namespace           = "TransformationLambdaError"
  evaluation_periods  = 1
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "This triggers when TransformErrorAlert is triggered in a minute."
  alarm_actions = ["arn:aws:sns:eu-west-2:730335327822:ConnectionErrorTest"]
  actions_enabled = "true"
} 
resource "aws_cloudwatch_metric_alarm" "loader_alert_errors" {
    /* 
    Creates a CloudWatch metric alarm to monitor the "ErrorMetric" metric in the "ExtractionLambdaError" namespace and trigger an alert when the value is greater than or equal to the specified threshold.

    Args:
        alarm_name (str): The name of the CloudWatch metric alarm.
        comparison_operator (str): The comparison operator used to evaluate the alarm condition.
        metric_name (str): The name of the metric to monitor.
        namespace (str): The namespace of the metric.
        evaluation_periods (int): The number of periods over which data is compared to the threshold.
        period (int): The length of time, in seconds, over which the specified  statistic is applied.
        statistic (str): The statistic to apply to the metric data.
        threshold (float): The value against which the specified statistic is compared.
        alarm_description (str): The description of the alarm.
        alarm_actions (list): The list of ARNs of the actions to take when the alarm transitions to the ALARM state.
        actions_enabled (str): Indicates whether actions should be executed during any changes to the alarm's state.

    Returns:
        None
    */

  alarm_name          = "LoaderErrorAlert"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  metric_name         = "ErrorMetric"
  namespace           = "LoaderLambdaError"
  evaluation_periods  = 1
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "This triggers when LoaderErrorAlert is triggered in a minute."
  alarm_actions = ["arn:aws:sns:eu-west-2:730335327822:ConnectionErrorTest"]
  actions_enabled = "true"
} 

