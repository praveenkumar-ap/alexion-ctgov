variable "environment" {
  type    = string
  default = "dev"
}

variable "snowflake_account" {
  type      = string
  sensitive = true
}

variable "snowflake_user" {
  type      = string
  sensitive = true
}

variable "snowflake_password" {
  type      = string
  sensitive = true
}
