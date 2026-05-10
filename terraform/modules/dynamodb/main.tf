variable "project" {
  type = string
}
variable "environment" {
  type = string
}

locals {
  prefix = "${var.project}-${var.environment}"
}

# ---------------------------------------------------------------------------
# Vehicle metadata — fleet registry
# ---------------------------------------------------------------------------

resource "aws_dynamodb_table" "vehicle_metadata" {
  name         = "${local.prefix}-vehicle-metadata"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "vehicle_id"

  attribute {
    name = "vehicle_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = false
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = { Name = "${local.prefix}-vehicle-metadata" }
}

# ---------------------------------------------------------------------------
# Data catalog — tracks Parquet partitions and schema versions
# ---------------------------------------------------------------------------

resource "aws_dynamodb_table" "data_catalog" {
  name         = "${local.prefix}-data-catalog"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "partition_key"
  range_key    = "sort_key"

  attribute {
    name = "partition_key" # sensor_type#year#month#day
    type = "S"
  }

  attribute {
    name = "sort_key" # vehicle_id#hour
    type = "S"
  }

  global_secondary_index {
    name            = "vehicle-index"
    hash_key        = "sort_key"
    projection_type = "ALL"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = { Name = "${local.prefix}-data-catalog" }
}

# ---------------------------------------------------------------------------
# Alert history
# ---------------------------------------------------------------------------

resource "aws_dynamodb_table" "alert_history" {
  name         = "${local.prefix}-alert-history"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "alert_id"
  range_key    = "timestamp"

  attribute {
    name = "alert_id"
    type = "S"
  }

  attribute {
    name = "timestamp"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  tags = { Name = "${local.prefix}-alert-history" }
}

# ---------------------------------------------------------------------------
# Pipeline state — checkpoint / offset tracking for non-Spark jobs
# ---------------------------------------------------------------------------

resource "aws_dynamodb_table" "pipeline_state" {
  name         = "${local.prefix}-pipeline-state"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pipeline_id"
  range_key    = "component"

  attribute {
    name = "pipeline_id"
    type = "S"
  }

  attribute {
    name = "component"
    type = "S"
  }

  tags = { Name = "${local.prefix}-pipeline-state" }
}

output "vehicle_metadata_table_name" {
  value = aws_dynamodb_table.vehicle_metadata.name
}
output "data_catalog_table_name" {
  value = aws_dynamodb_table.data_catalog.name
}
output "alert_history_table_name" {
  value = aws_dynamodb_table.alert_history.name
}
output "pipeline_state_table_name" {
  value = aws_dynamodb_table.pipeline_state.name
}
