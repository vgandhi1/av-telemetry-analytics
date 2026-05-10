variable "project"     { type = string }
variable "environment" { type = string }

locals {
  data_bucket = "${var.project}-data-${var.environment}"
  logs_bucket = "${var.project}-logs-${var.environment}"
}

# ---------------------------------------------------------------------------
# Data bucket (telemetry Parquet + ML models)
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "data" {
  bucket        = local.data_bucket
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "raw-expire"
    status = "Enabled"
    filter { prefix = "telemetry/v1/raw/" }
    expiration { days = 90 }
  }

  rule {
    id     = "processed-ia-transition"
    status = "Enabled"
    filter { prefix = "telemetry/v1/processed/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# ---------------------------------------------------------------------------
# Logs bucket (EMR, pipeline logs — no versioning needed)
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "logs" {
  bucket        = local.logs_bucket
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "logs" {
  bucket                  = aws_s3_bucket.logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    id     = "logs-expire"
    status = "Enabled"
    filter {}
    expiration { days = 30 }
  }
}

output "data_bucket_id" { value = aws_s3_bucket.data.id }
output "logs_bucket_id" { value = aws_s3_bucket.logs.id }
output "data_bucket_arn" { value = aws_s3_bucket.data.arn }
