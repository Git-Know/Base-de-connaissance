provider "aws" {
  region                      = "eu-west-3"
  access_key                  = var.aws_access_key
  secret_key                  = var.aws_secret_key
  s3_force_path_style         = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    ec2 = var.ec2_endpoint
    s3  = var.s3_endpoint
  }
}

variable "aws_access_key" {
  type        = string
  description = "AWS Access Key"
}

variable "aws_secret_key" {
  type        = string
  description = "AWS Secret Key"
}

variable "ec2_endpoint" {
  type        = string
  description = "Custom EC2 endpoint URL"
}

variable "s3_endpoint" {
  type        = string
  description = "Custom S3 endpoint URL"
}
