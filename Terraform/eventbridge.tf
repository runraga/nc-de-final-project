resource "aws_cloudwatch_event_rule" "every_fifteen_minutes" {
    /*
    Creates a CloudWatch Events rule that triggers an event every five minutes.

    Args:
        name (str): The name of the CloudWatch Events rule.
        description (str): The description of the CloudWatch Events rule.
        schedule_expression (str): The schedule expression specifying the frequency of the events.

    Returns:
        None
    */
  name                = "every-fifteen-minutes"
  description         = "Fires every fifteen minutes"
  # need to decide on the best frequency
  schedule_expression = "rate(15 minutes)" 
}
resource "aws_cloudwatch_event_target" "every_fifteen_minutes" {
    /*
    Creates a target for a CloudWatch Events rule to trigger a Lambda function every five minutes.

    Args:
        rule (str): The name of the CloudWatch Events rule to associate the target with.
        target_id (str): The unique identifier for the target.
        arn (str): The Amazon Resource Name (ARN) of the Lambda function to be triggered by the CloudWatch Events rule.

    Returns:
        None
    */
  rule      = aws_cloudwatch_event_rule.every_fifteen_minutes.name
  target_id = "extraction_lambda"
  arn       = "${aws_lambda_function.extraction_lambda.arn}"

}

resource "aws_cloudwatch_log_group" "extraction_log_group" {
    /*
    Creates a CloudWatch Logs group for logging Lambda function invocations.

    Args:
        name (str): The name of the CloudWatch Logs group.

    Returns:
        None
    */
  name = "/aws/lambda/${aws_lambda_function.extraction_lambda.function_name}"
}
resource "aws_cloudwatch_log_group" "transformation_log_group" {
    /*
    Creates a CloudWatch Logs group for logging Lambda function invocations.

    Args:
        name (str): The name of the CloudWatch Logs group.

    Returns:
        None
    */
  name = "/aws/lambda/${aws_lambda_function.transformation_lambda.function_name}"
}
resource "aws_cloudwatch_log_group" "loader_log_group" {
    /*
    Creates a CloudWatch Logs group for logging Lambda function invocations.

    Args:
        name (str): The name of the CloudWatch Logs group.

    Returns:
        None
    */
  name = "/aws/lambda/${aws_lambda_function.loader_lambda.function_name}"
}