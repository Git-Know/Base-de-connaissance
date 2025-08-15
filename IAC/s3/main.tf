# get current account id (used in safer policy example)
data "aws_caller_identity" "current" {}

# Création du bucket S3
resource "aws_s3_bucket" "angular_bucket" {
  bucket = "angular-app-bucke-talan-summer-internship"
}

resource "aws_s3_bucket_ownership_controls" "aws_s3_bucket_control" {
  bucket = aws_s3_bucket.angular_bucket.id
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

# Allow public ACLs/policies on this bucket (only because we intend to host a public site).
# Keep these set to `false` only for test/demo static site hosting. For production,
# prefer keeping the bucket private and use CloudFront OAC.
resource "aws_s3_bucket_public_access_block" "angular_bucket_public_access" {
  bucket                  = aws_s3_bucket.angular_bucket.id
  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

resource "aws_s3_bucket_acl" "aws_s3_bucket_acl" {
  depends_on = [
    aws_s3_bucket_public_access_block.angular_bucket_public_access,
    aws_s3_bucket_ownership_controls.aws_s3_bucket_control
  ]

  bucket = aws_s3_bucket.angular_bucket.id
  acl    = "public-read"
}

# Configuration de l’hébergement web
resource "aws_s3_bucket_website_configuration" "angular_bucket_website" {
  depends_on = [
    aws_s3_bucket_acl.aws_s3_bucket_acl
  ]

  bucket = aws_s3_bucket.angular_bucket.id

  index_document {
    suffix = "index.html"
  }

  error_document {
    key = "index.html"
  }
}

# Ouverture de l’accès public — applied AFTER public access block & ACL
resource "aws_s3_bucket_policy" "bucket_policy" {
  depends_on = [
    aws_s3_bucket_public_access_block.angular_bucket_public_access,
    aws_s3_bucket_acl.aws_s3_bucket_acl,
    aws_s3_bucket_ownership_controls.aws_s3_bucket_control
  ]

  bucket = aws_s3_bucket.angular_bucket.id

  # Default: public read only (recommended for static website hosting)
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid       = "PublicReadGetObject",
        Effect    = "Allow",
        Principal = "*",
        Action = [
          "s3:GetObject"
        ],
        Resource = "${aws_s3_bucket.angular_bucket.arn}/*"
      }
    ]
  })
}

# ---- safer alternative: allow public read but restrict uploads to your AWS account only ----
# To use the safer upload policy, replace the above 'policy =' with the block below
# (and remove or comment the public-read-only policy).
#
# Note: this allows PUT only by the account owner (not public).
#
# resource "aws_s3_bucket_policy" "bucket_policy" {
#   depends_on = [
#     aws_s3_bucket_public_access_block.angular_bucket_public_access,
#     aws_s3_bucket_acl.aws_s3_bucket_acl,
#     aws_s3_bucket_ownership_controls.aws_s3_bucket_control
#   ]
#
#   bucket = aws_s3_bucket.angular_bucket.id
#
#   policy = jsonencode({
#     Version = "2012-10-17",
#     Statement = [
#       {
#         Sid       = "PublicReadGetObject",
#         Effect    = "Allow",
#         Principal = "*",
#         Action = ["s3:GetObject"],
#         Resource = "${aws_s3_bucket.angular_bucket.arn}/*"
#       },
#       {
#         Sid    = "AllowAccountPutObject",
#         Effect = "Allow",
#         Principal = {
#           AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
#         },
#         Action = ["s3:PutObject"],
#         Resource = "${aws_s3_bucket.angular_bucket.arn}/*"
#       }
#     ]
#   })
# }

resource "aws_s3_bucket_cors_configuration" "angular_bucket_cors" {
  bucket = aws_s3_bucket.angular_bucket.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["*"]
    max_age_seconds = 3000
  }
}

output "website_url" {
  value = aws_s3_bucket.angular_bucket.website_endpoint
}
