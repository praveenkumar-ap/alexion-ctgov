# Configure AWS Provider
provider "aws" {
  region = var.aws_region
}

# Configure Snowflake Provider (used only at apply-time)
provider "snowflake" {
  account  = var.snowflake_account
  user     = var.snowflake_user
  password = var.snowflake_password
  role     = "SYSADMIN"
}
