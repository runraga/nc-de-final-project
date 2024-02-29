data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "archive_file" "extraction_lambda" {
  /*
    Creates an archive file containing the Lambda function code and dependencies.

    Args:
        type (str): The type of archive file to create (e.g., "zip").
        source_file (str): The path to the main source file or directory to include in the archive.
        output_path (str): The path where the archive file will be generated.

    Returns:
        None
    */
  type        = "zip"
  source_file = "${path.module}/../src/extractor.py"
  output_path = "${path.module}/../extraction_lambda.zip"

}
data "archive_file" "transformation_lambda" {
  /*
    Creates an archive file containing the Lambda function code and dependencies.

    Args:
        type (str): The type of archive file to create (e.g., "zip").
        source_file (str): The path to the main source file or directory to include in the archive.
        output_path (str): The path where the archive file will be generated.

    Returns:
        None
    */
  type        = "zip"
  source_file = "${path.module}/../src/transformation.py"
  output_path = "${path.module}/../transformation_lambda.zip"
}
data "archive_file" "loader_lambda" {
  /*
    Creates an archive file containing the Lambda function code and dependencies.

    Args:
        type (str): The type of archive file to create (e.g., "zip").
        source_file (str): The path to the main source file or directory to include in the archive.
        output_path (str): The path where the archive file will be generated.

    Returns:
        None
    */
  type = "zip"

  source_file = "${path.module}/../src/loader.py"
  output_path = "${path.module}/../loader_lambda.zip"
}

data "aws_s3_bucket" "utility_bucket" {
  bucket = var.utility_bucket
}
