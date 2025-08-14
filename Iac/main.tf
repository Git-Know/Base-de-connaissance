#Configuration du provider AWS
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

#Connexion à AWS (via LocalStack)
provider "aws" {
  region                      = "eu-west-3"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3 = "http://localhost:4566"
  }

  s3_use_path_style = true
}

#Création du bucket S3
resource "aws_s3_bucket" "angular_bucket" {
  bucket = "angular-app-bucket"
  acl    = "public-read"
}

#Configuration de l’hébergement web
resource "aws_s3_bucket_website_configuration" "angular_bucket_website" {
  bucket = aws_s3_bucket.angular_bucket.id

  index_document {
    suffix = "index.html"
  }

  error_document {
    key = "index.html"
  }
}

#Ouverture de l’accès public
resource "aws_s3_bucket_policy" "bucket_policy" {
  bucket = aws_s3_bucket.angular_bucket.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid       = "PublicReadGetObject",
        Effect    = "Allow",
        Principal = "*",
        Action    = [
          "s3:GetObject"
        ],
        Resource  = "${aws_s3_bucket.angular_bucket.arn}/*"
      }
    ]
  })
}

resource "aws_s3_bucket_cors_configuration" "angular_bucket_cors" {
  bucket = aws_s3_bucket.angular_bucket.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["*"]   # ou plus restrictif, exemple: ["http://angular-app-bucket.localhost:4566"]
    max_age_seconds = 3000
  }
}


output "website_url" {
  value = aws_s3_bucket.angular_bucket.website_endpoint
}

