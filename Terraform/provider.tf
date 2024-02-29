provider "aws" {
  region = "eu-west-2"
}

terraform {
  /*
      Configures Terraform to use an S3 backend for state management and specifies the required AWS provider.

    Args:
        bucket (str): The name of the S3 bucket to store Terraform state files.
        key (str): The key (path) in the S3 bucket where the state file will be stored.
        region (str): The AWS region where the S3 bucket is located.

    Returns:
        None 
    */
  backend "s3" {
    #bucket = "rannoch-s3-utility-bucket"
    bucket = "rannoch-s3-utility-bucket"
    key    = "utility/tfstate"
    region = "eu-west-2"
  }
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}
