variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-west-2"
}

variable "environment" {
  description = "Deployment environment (dev | staging | prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "project" {
  description = "Project name prefix for all resource names"
  type        = string
  default     = "av-telemetry"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets (one per AZ)"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "emr_release_label" {
  description = "EMR release (Spark version)"
  type        = string
  default     = "emr-7.0.0"
}

variable "emr_master_instance_type" {
  type    = string
  default = "m5.xlarge"
}

variable "emr_core_instance_type" {
  type    = string
  default = "r5.2xlarge"
}

variable "emr_core_instance_count" {
  type    = number
  default = 3
}

variable "emr_task_instance_type" {
  type    = string
  default = "r5.2xlarge"
}

variable "emr_task_min_instances" {
  type    = number
  default = 1
}

variable "emr_task_max_instances" {
  type    = number
  default = 10
}

variable "emr_key_pair" {
  description = "EC2 key pair name for SSH access to EMR nodes"
  type        = string
  default     = ""
}

variable "alert_email" {
  description = "Email for CloudWatch alarm notifications"
  type        = string
  default     = ""
}
