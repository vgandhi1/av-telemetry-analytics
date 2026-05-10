output "data_bucket_name" {
  description = "S3 bucket for telemetry data"
  value       = module.s3.data_bucket_id
}

output "logs_bucket_name" {
  description = "S3 bucket for EMR and pipeline logs"
  value       = module.s3.logs_bucket_id
}

output "emr_cluster_id" {
  description = "EMR cluster ID"
  value       = module.emr.cluster_id
}

output "emr_master_dns" {
  description = "EMR master node public DNS (if in public subnet)"
  value       = module.emr.master_public_dns
}

output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnet IDs"
  value       = module.vpc.private_subnet_ids
}

output "vehicle_metadata_table" {
  description = "DynamoDB table for vehicle metadata"
  value       = module.dynamodb.vehicle_metadata_table_name
}

output "data_catalog_table" {
  description = "DynamoDB table for data catalog"
  value       = module.dynamodb.data_catalog_table_name
}
