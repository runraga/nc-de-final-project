resource "aws_iam_role" "extraction_lambda_role" {
  /*
    Creates an IAM role for the Lambda function to assume.
    Args:
    assume_role_policy (str): The JSON-encoded IAM policy document specifying who can assume the role.

    Returns:
        None
    */
  assume_role_policy = jsonencode(
    {
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Effect" : "Allow",
          "Action" : [
            "sts:AssumeRole"
          ],
          "Principal" : {
            "Service" : [
              "lambda.amazonaws.com"
            ]
          }
        }
      ]
    }
  )
}
resource "aws_iam_role" "transformation_lambda_role" {
  /*
    Creates an IAM role for the Lambda function to assume.
    Args:
    assume_role_policy (str): The JSON-encoded IAM policy document specifying who can assume the role.

    Returns:
        None
    */
  assume_role_policy = jsonencode(
    {
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Effect" : "Allow",
          "Action" : [
            "sts:AssumeRole"
          ],
          "Principal" : {
            "Service" : [
              "lambda.amazonaws.com"
            ]
          }
        }
      ]
    }
  )
}
resource "aws_iam_role" "loader_lambda_role" {
  /*
    Creates an IAM role for the Lambda function to assume.
    Args:
    assume_role_policy (str): The JSON-encoded IAM policy document specifying who can assume the role.

    Returns:
        None
    */
  assume_role_policy = jsonencode(
    {
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Effect" : "Allow",
          "Action" : [
            "sts:AssumeRole"
          ],
          "Principal" : {
            "Service" : [
              "lambda.amazonaws.com"
            ]
          }
        }
      ]
    }
  )
}


data "aws_iam_policy_document" "put_s3_document" {
  /*
    Generates an IAM policy document for putting documents into an S3 bucket.

    Args:
        actions (list): The list of actions allowed by the policy.
        resources (list): The list of resources to which the policy applies.
    */
  statement {
    actions = ["s3:PutObject", "s3:ListBucket", "s3:GetObject"]
    resources = [
      "${aws_s3_bucket.rannoch-s3-ingestion-bucket.arn}/*",
      "${aws_s3_bucket.rannoch-s3-ingestion-bucket.arn}",
      "${aws_s3_bucket.rannoch-s3-processed-data-bucket.arn}/*",
      "${aws_s3_bucket.rannoch-s3-processed-data-bucket.arn}",
      "${data.aws_s3_bucket.utility_bucket.arn}/*",
      "${data.aws_s3_bucket.utility_bucket.arn}"
    ]
  }
}

data "aws_iam_policy_document" "cw_document" {
  /*
    Generates an IAM policy document for CloudWatch Logs.

    Args:
        statement (list): A list of statements defining the permissions for the policy.

    */
  statement {
    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
  statement {
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${aws_lambda_function.extraction_lambda.function_name}:*",
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${aws_lambda_function.transformation_lambda.function_name}:*",
      #add the loader lambda here
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${aws_lambda_function.loader_lambda.function_name}:*"
    ]
  }
}

resource "aws_lambda_permission" "allow_cloudwatch" {
  /*
    Grants permission for CloudWatch Events to invoke a Lambda function.

    Args:
        statement_id (str): A unique identifier for the policy statement.
        action (str): The action to allow (e.g., "lambda:InvokeFunction").
        function_name (str): The name of the Lambda function to allow invocation for.
        principal (str): The entity (service, user, role) that is allowed to perform the action.
        source_arn (str): The Amazon Resource Name (ARN) of the event source.

    Returns:
        None
    */
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extraction_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.every_fifteen_minutes.arn
}
resource "aws_iam_policy" "s3_policy" {
  /*
    Creates an IAM policy using the specified IAM policy document.

    Args:
        name_prefix (str): The prefix for the name of the IAM policy.
        policy (str): The JSON-encoded IAM policy document.

    Returns:
        None
    */
  name_prefix = "s3-policy-${var.bucket_name}"
  policy      = data.aws_iam_policy_document.put_s3_document.json
}

resource "aws_iam_policy" "extraction_cloudwatch_policy" {
  /*
    Creates an IAM policy using the specified IAM policy document.

    Args:
        name_prefix (str): The prefix for the name of the IAM policy.
        policy (str): The JSON-encoded IAM policy document.

    Returns:
        None
    */
  name_prefix = "cw-policy-${var.extraction_lambda_name}"
  policy      = data.aws_iam_policy_document.cw_document.json
}
resource "aws_iam_policy" "transformation_cloudwatch_policy" {
  /*
    Creates an IAM policy using the specified IAM policy document.

    Args:
        name_prefix (str): The prefix for the name of the IAM policy.
        policy (str): The JSON-encoded IAM policy document.

    Returns:
        None
    */
  name_prefix = "cw-policy-${var.transform_lambda_name}"
  policy      = data.aws_iam_policy_document.cw_document.json
}
resource "aws_iam_policy" "loader_cloudwatch_policy" {
  /*
    Creates an IAM policy using the specified IAM policy document.

    Args:
        name_prefix (str): The prefix for the name of the IAM policy.
        policy (str): The JSON-encoded IAM policy document.

    Returns:
        None
    */
  name_prefix = "cw-policy-${var.loader_lambda_name}"
  policy      = data.aws_iam_policy_document.cw_document.json
}

resource "aws_iam_role_policy_attachment" "extraction_s3_policy_attachment" {
  /*
    Attaches an IAM policy to an IAM role.

    Args:
        policy_arn (str): The Amazon Resource Name (ARN) of the IAM policy.
        role_name (str): The name of the IAM role.

    Returns:
        None
    */
  role       = aws_iam_role.extraction_lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}
resource "aws_iam_role_policy_attachment" "transfomation_s3_policy_attachment" {
  /*
    Attaches an IAM policy to an IAM role.

    Args:
        policy_arn (str): The Amazon Resource Name (ARN) of the IAM policy.
        role_name (str): The name of the IAM role.

    Returns:
        None
    */
  role       = aws_iam_role.transformation_lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}
resource "aws_iam_role_policy_attachment" "loader_s3_policy_attachment" {
  /*
    Attaches an IAM policy to an IAM role.

    Args:
        policy_arn (str): The Amazon Resource Name (ARN) of the IAM policy.
        role_name (str): The name of the IAM role.

    Returns:
        None
    */
  role       = aws_iam_role.loader_lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "extraction_cw_policy_attachment" {
  /*
    Attaches an IAM policy to an IAM role.

    Args:
        policy_arn (str): The Amazon Resource Name (ARN) of the IAM policy.
        role_name (str): The name of the IAM role.

    Returns:
        None
    */
  role       = aws_iam_role.extraction_lambda_role.name
  policy_arn = aws_iam_policy.extraction_cloudwatch_policy.arn
}
resource "aws_iam_role_policy_attachment" "transformation_cw_policy_attachment" {
  /*
    Attaches an IAM policy to an IAM role.

    Args:
        policy_arn (str): The Amazon Resource Name (ARN) of the IAM policy.
        role_name (str): The name of the IAM role.

    Returns:
        None
    */
  role       = aws_iam_role.transformation_lambda_role.name
  policy_arn = aws_iam_policy.transformation_cloudwatch_policy.arn
}
resource "aws_iam_role_policy_attachment" "loader_cw_policy_attachment" {
  /*
    Attaches an IAM policy to an IAM role.

    Args:
        policy_arn (str): The Amazon Resource Name (ARN) of the IAM policy.
        role_name (str): The name of the IAM role.

    Returns:
        None
    */
  role       = aws_iam_role.loader_lambda_role.name
  policy_arn = aws_iam_policy.loader_cloudwatch_policy.arn
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.rannoch-s3-ingestion-bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.transformation_lambda.arn
    events              = ["s3:ObjectCreated:*"]
  }
}
resource "aws_s3_bucket_notification" "bucket_notification_loader" {
  bucket = aws_s3_bucket.rannoch-s3-processed-data-bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.loader_lambda.arn
    events              = ["s3:ObjectCreated:*"]
  }
}

resource "aws_lambda_permission" "allow_s3" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.transformation_lambda.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.rannoch-s3-ingestion-bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}
resource "aws_lambda_permission" "allow_s3_loader" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.loader_lambda.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.rannoch-s3-processed-data-bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}
