terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
  }

  # Remote state — swap bucket/key per environment
  backend "s3" {
    bucket         = "av-telemetry-tf-state"
    key            = "terraform.tfstate"
    region         = "us-west-2"
    dynamodb_table = "av-telemetry-tf-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------

module "vpc" {
  source               = "./modules/vpc"
  project              = var.project
  environment          = var.environment
  vpc_cidr             = var.vpc_cidr
  private_subnet_cidrs = var.private_subnet_cidrs
  aws_region           = var.aws_region
}

module "s3" {
  source      = "./modules/s3"
  project     = var.project
  environment = var.environment
}

module "dynamodb" {
  source      = "./modules/dynamodb"
  project     = var.project
  environment = var.environment
}

module "emr" {
  source                   = "./modules/emr"
  project                  = var.project
  environment              = var.environment
  aws_region               = var.aws_region
  subnet_id                = module.vpc.private_subnet_ids[0]
  emr_security_group_id    = module.vpc.emr_security_group_id
  release_label            = var.emr_release_label
  master_instance_type     = var.emr_master_instance_type
  core_instance_type       = var.emr_core_instance_type
  core_instance_count      = var.emr_core_instance_count
  task_instance_type       = var.emr_task_instance_type
  task_min_instances       = var.emr_task_min_instances
  task_max_instances       = var.emr_task_max_instances
  key_pair                 = var.emr_key_pair
  logs_bucket              = module.s3.logs_bucket_id
  data_bucket              = module.s3.data_bucket_id
  emr_service_role_arn     = aws_iam_role.emr_service_role.arn
  emr_instance_profile_arn = aws_iam_instance_profile.emr_profile.arn
}

# ---------------------------------------------------------------------------
# IAM — EMR service role + instance profile
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_service_role" {
  name               = "${var.project}-${var.environment}-emr-service-role"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_instance_role" {
  name               = "${var.project}-${var.environment}-emr-instance-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "emr_ec2_s3" {
  role       = aws_iam_role.emr_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "emr_ec2_dynamo" {
  role       = aws_iam_role.emr_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
}

resource "aws_iam_role_policy_attachment" "emr_ec2_profile" {
  role       = aws_iam_role.emr_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

resource "aws_iam_instance_profile" "emr_profile" {
  name = "${var.project}-${var.environment}-emr-profile"
  role = aws_iam_role.emr_instance_role.name
}
