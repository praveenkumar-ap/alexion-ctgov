# General
variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev|staging|prod)"
  type        = string
  default     = "dev"
}

# ECR/Lambda image (repo must already exist and be pushed by your script)
variable "ecr_repo_name" {
  description = "ECR repository name (e.g. ctgov-ingestion-dev)"
  type        = string
}

variable "image_tag" {
  description = "Image tag to deploy (e.g. latest or a git sha)"
  type        = string
}

# Logs & schedule
variable "log_retention_days" {
  description = "Retention days for Lambda CloudWatch logs"
  type        = number
  default     = 14
}

variable "eventbridge_cron" {
  description = "Schedule for first Monday every 2h UTC"
  type        = string
  default     = "cron(0 0/2 ? * MON#1 *)"
}

# Snowflake: provider creds (used only at terraform apply time)
variable "snowflake_account" {
  description = "Snowflake account identifier for provider"
  type        = string
  sensitive   = true
}

variable "snowflake_user" {
  description = "Snowflake user for provider"
  type        = string
  sensitive   = true
}

variable "snowflake_password" {
  description = "Snowflake password for provider"
  type        = string
  sensitive   = true
}

# Snowflake: runtime secret for Lambda (Secrets Manager)
variable "snowflake_secret_arn" {
  description = "ARN of Secrets Manager secret with Snowflake {account,user,password}. Leave empty to skip."
  type        = string
  default     = ""

  validation {
    condition     = var.snowflake_secret_arn == "" || can(regex("^arn:aws:secretsmanager:[^:]+:\\d{12}:secret:.+", var.snowflake_secret_arn))
    error_message = "If provided, snowflake_secret_arn must be a valid Secrets Manager ARN."
  }
}

# Who will run deploys?
variable "iam_user_name" {
  description = "IAM user to grant deployment permissions (e.g. 'prav')"
  type        = string
  default     = "prav"
}

# If true, attach AdministratorAccess (single policy). If false, attach a curated set.
variable "grant_admin" {
  description = "Grant AdministratorAccess to iam_user_name (use only in sandboxes!)"
  type        = bool
  default     = true
}