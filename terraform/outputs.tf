output "snowflake_database_name" {
  description = "Name of the Snowflake database for dbt connection"
  value       = snowflake_database.ctgov.name
}

output "snowflake_schemas" {
  description = "Names of the Snowflake schemas for dbt models"
  value = {
    raw     = snowflake_schema.raw.name
    staging = snowflake_schema.staging.name
    marts   = snowflake_schema.marts.name
  }
}

output "snowflake_warehouse_name" {
  description = "Name of the Snowflake warehouse for dbt"
  value       = snowflake_warehouse.transform_wh.name
}

output "snowflake_raw_table_name" {
  description = "Fully qualified name of the raw studies table"
  value       = "${snowflake_database.ctgov.name}.${snowflake_schema.raw.name}.${snowflake_table.raw_ctgov_studies.name}"
}

output "lambda_function_name" {
  description = "Name of the Lambda function for ingestion"
  value       = aws_lambda_function.ctgov_ingestion.function_name
}

output "eventbridge_rule_arn" {
  description = "ARN of the EventBridge rule for scheduling"
  value       = aws_cloudwatch_event_rule.monthly_schedule.arn
}

output "next_steps" {
  description = "Next steps after Terraform deployment"
  value       = <<EOT
Next steps:
1. Update dbt/profiles.yml with these Snowflake details:
   - Database: ${snowflake_database.ctgov.name}
   - Schema: ${snowflake_schema.staging.name}
   - Warehouse: ${snowflake_warehouse.transform_wh.name}

2. Package your Python code: 
   cd ingestion && zip -r clinical_trials_api.zip .

3. Test the Lambda function manually first.
EOT
}