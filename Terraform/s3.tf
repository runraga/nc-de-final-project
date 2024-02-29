resource "aws_s3_bucket" "rannoch-s3-ingestion-bucket" {
  /*
    Creates an Amazon S3 bucket for data ingestion.

    Args:
        bucket (str): The name of the S3 bucket.
        force_destroy (bool): A boolean flag indicating whether all objects should be deleted from the bucket before deleting the bucket.

    Returns:
        None
    */
  bucket        = "${var.bucket_name}ingestion-bucket"
  force_destroy = true
}
resource "aws_s3_bucket" "rannoch-s3-processed-data-bucket" {
  /*
    Creates an Amazon S3 bucket for data ingestion.

    Args:
        bucket (str): The name of the S3 bucket.
        force_destroy (bool): A boolean flag indicating whether all objects should be deleted from the bucket before deleting the bucket.

    Returns:
        None
    */
  bucket        = "${var.bucket_name}processed-data-bucket"
  force_destroy = true
}

resource "aws_s3_object" "extraction_lambda_code" {
  /*
    Creates an Amazon S3 bucket for data ingestion.

    Args:
        bucket (str): The name of the S3 bucket.
        force_destroy (bool): A boolean flag indicating whether all objects should be deleted from the bucket before deleting the bucket.

    Returns:
        None
    */

  bucket      = var.utility_bucket
  key         = "lambda-code/extraction_lambda.zip"
  source      = "${path.module}/../extraction_lambda.zip"
  source_hash = filemd5("${path.module}/../src/extractor.py")

}
resource "aws_s3_object" "transformation_lambda_code" {
  /*
    Creates an Amazon S3 bucket for data ingestion.

    Args:
        bucket (str): The name of the S3 bucket.
        force_destroy (bool): A boolean flag indicating whether all objects should be deleted from the bucket before deleting the bucket.

    Returns:
        None
    */
  bucket      = var.utility_bucket
  key         = "lambda-code/transformation_lambda.zip"
  source      = "${path.module}/../transformation_lambda.zip"
  source_hash = filemd5("${path.module}/../src/transformation.py")

}
resource "aws_s3_object" "loader_lambda_code" {
  /*
    Creates an Amazon S3 bucket for data ingestion.

    Args:
        bucket (str): The name of the S3 bucket.
        force_destroy (bool): A boolean flag indicating whether all objects should be deleted from the bucket before deleting the bucket.

    Returns:
        None
    */
  bucket      = var.utility_bucket
  key         = "lambda-code/loader_lambda.zip"
  source      = "${path.module}/../loader_lambda.zip"
  source_hash = filemd5("${path.module}/../src/loader.py")

}

resource "aws_s3_bucket_versioning" "ingestion_bucket" {
  /*
   Enables versioning for an Amazon S3 bucket.

    Args:
        bucket (str): The ID of the S3 bucket for which versioning is enabled.

    Returns:
        None
   */
  bucket = aws_s3_bucket.rannoch-s3-ingestion-bucket.id
  versioning_configuration {
    status = "Enabled"
    #disabled by default
  }
}
resource "aws_s3_bucket_versioning" "processed_data_bucket" {
  /*
   Enables versioning for an Amazon S3 bucket.

    Args:
        bucket (str): The ID of the S3 bucket for which versioning is enabled.

    Returns:
        None
   */
  bucket = aws_s3_bucket.rannoch-s3-processed-data-bucket.id
  versioning_configuration {
    status = "Enabled"
    #disabled by default
  }
}

# this will not run if versioning is disabled
# have to do another terraform apply to ensure retention
resource "aws_s3_bucket_object_lock_configuration" "ingestion_bucket" {
  /*
    Configures object lock for an Amazon S3 bucket.

    Args:
        bucket (str): The ID of the S3 bucket for which object lock is configured.

    Returns:
        None
    */
  bucket = aws_s3_bucket.rannoch-s3-ingestion-bucket.id

  rule {
    default_retention {
      mode = "GOVERNANCE"
      days = 5
    }
  }
}
resource "aws_s3_bucket_object_lock_configuration" "processed_data_bucket" {
  /*
    Configures object lock for an Amazon S3 bucket.

    Args:
        bucket (str): The ID of the S3 bucket for which object lock is configured.

    Returns:
        None
    */
  bucket = aws_s3_bucket.rannoch-s3-processed-data-bucket.id

  rule {
    default_retention {
      mode = "GOVERNANCE"
      days = 5
    }
  }
}
