variable "project"                  { type = string }
variable "environment"              { type = string }
variable "aws_region"               { type = string }
variable "subnet_id"                { type = string }
variable "emr_security_group_id"    { type = string }
variable "release_label"            { type = string }
variable "master_instance_type"     { type = string }
variable "core_instance_type"       { type = string }
variable "core_instance_count"      { type = number }
variable "task_instance_type"       { type = string }
variable "task_min_instances"       { type = number }
variable "task_max_instances"       { type = number }
variable "key_pair"                 { type = string }
variable "logs_bucket"              { type = string }
variable "data_bucket"              { type = string }
variable "emr_service_role_arn"     { type = string }
variable "emr_instance_profile_arn" { type = string }

locals {
  name = "${var.project}-${var.environment}-spark"
}

resource "aws_emr_cluster" "spark" {
  name          = local.name
  release_label = var.release_label
  applications  = ["Spark", "Hadoop", "Ganglia", "JupyterEnterpriseGateway"]

  log_uri = "s3://${var.logs_bucket}/emr/"

  ec2_attributes {
    subnet_id                         = var.subnet_id
    emr_managed_master_security_group = var.emr_security_group_id
    emr_managed_slave_security_group  = var.emr_security_group_id
    instance_profile                  = var.emr_instance_profile_arn
    key_name                          = var.key_pair != "" ? var.key_pair : null
  }

  master_instance_group {
    instance_type = var.master_instance_type
  }

  core_instance_group {
    instance_type  = var.core_instance_type
    instance_count = var.core_instance_count

    ebs_config {
      size                 = 100
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  # Task group with auto-scaling for burst processing
  task_instance_group {
    name           = "${local.name}-task"
    instance_type  = var.task_instance_type
    instance_count = var.task_min_instances

    ebs_config {
      size                 = 50
      type                 = "gp3"
      volumes_per_instance = 1
    }

    autoscaling_policy = jsonencode({
      Constraints = {
        MinCapacity = var.task_min_instances
        MaxCapacity = var.task_max_instances
      }
      Rules = [
        {
          Name        = "ScaleOut"
          Description = "Scale out if YARN memory available < 15%"
          Action = {
            SimpleScalingPolicyConfiguration = {
              AdjustmentType    = "CHANGE_IN_CAPACITY"
              ScalingAdjustment = 2
              CoolDown          = 120
            }
          }
          Trigger = {
            CloudWatchAlarmDefinition = {
              ComparisonOperator = "LESS_THAN"
              EvaluationPeriods  = 1
              MetricName         = "YARNMemoryAvailablePercentage"
              Namespace          = "AWS/ElasticMapReduce"
              Period             = 300
              Statistic          = "AVERAGE"
              Threshold          = 15
            }
          }
        },
        {
          Name        = "ScaleIn"
          Description = "Scale in if YARN memory available > 75%"
          Action = {
            SimpleScalingPolicyConfiguration = {
              AdjustmentType    = "CHANGE_IN_CAPACITY"
              ScalingAdjustment = -1
              CoolDown          = 300
            }
          }
          Trigger = {
            CloudWatchAlarmDefinition = {
              ComparisonOperator = "GREATER_THAN"
              EvaluationPeriods  = 3
              MetricName         = "YARNMemoryAvailablePercentage"
              Namespace          = "AWS/ElasticMapReduce"
              Period             = 300
              Statistic          = "AVERAGE"
              Threshold          = 75
            }
          }
        }
      ]
    })
  }

  configurations_json = jsonencode([
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.sql.adaptive.enabled"                  = "true"
        "spark.sql.adaptive.coalescePartitions.enabled" = "true"
        "spark.hadoop.fs.s3a.fast.upload"             = "true"
        "spark.hadoop.fs.s3a.multipart.size"          = "104857600"
        "spark.serializer"                            = "org.apache.spark.serializer.KryoSerializer"
      }
    },
    {
      Classification = "spark-env"
      Configurations = [
        {
          Classification = "export"
          Properties = {
            PYSPARK_PYTHON = "/usr/bin/python3"
          }
        }
      ]
    },
    {
      Classification = "hadoop-env"
      Configurations = [
        {
          Classification = "export"
          Properties = {
            JAVA_HOME = "/usr/lib/jvm/java-17-amazon-corretto"
          }
        }
      ]
    }
  ])

  service_role = var.emr_service_role_arn

  auto_termination_policy {
    idle_timeout = var.environment == "prod" ? 0 : 3600
  }

  tags = {
    Name        = local.name
    Environment = var.environment
  }

  lifecycle {
    ignore_changes = [core_instance_group[0].instance_count]
  }
}

output "cluster_id"         { value = aws_emr_cluster.spark.id }
output "master_public_dns"  { value = aws_emr_cluster.spark.master_public_dns }
output "cluster_arn"        { value = aws_emr_cluster.spark.arn }
