# ---------------------- Snowflake ----------------------
resource "snowflake_database" "ctgov" {
  name    = "CLINICAL_TRIALS_${upper(var.environment)}"
  comment = "ClinicalTrials.gov data database"
}

resource "snowflake_schema" "raw" {
  database   = snowflake_database.ctgov.name
  name       = "RAW"
  comment    = "Raw JSON data from API"
  depends_on = [snowflake_database.ctgov]
}

resource "snowflake_table" "raw_ctgov_studies" {
  database = snowflake_database.ctgov.name
  schema   = snowflake_schema.raw.name
  name     = "RAW_CTGOV_STUDIES"

  column {
    name = "RAW_DATA"
    type = "VARIANT"
  }

  column {
    name = "INGESTION_TIMESTAMP"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "BATCH_ID"
    type = "VARCHAR(50)"
  }

  depends_on = [snowflake_schema.raw]
}

resource "snowflake_schema" "staging" {
  database   = snowflake_database.ctgov.name
  name       = "STAGING"
  comment    = "Staging data for dbt"
  depends_on = [snowflake_database.ctgov]
}

resource "snowflake_schema" "marts" {
  database   = snowflake_database.ctgov.name
  name       = "MARTS"
  comment    = "Mart data for business logic"
  depends_on = [snowflake_database.ctgov]
}

resource "snowflake_warehouse" "transform_wh" {
  name           = "TRANSFORM_WH"
  warehouse_size = "X-SMALL"
  auto_suspend   = 60
}

# ---------------------- AWS IAM for Lambda ----------------------
resource "aws_iam_role" "lambda_exec_role" {
  name = "ctgov-lambda-exec-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action    = "sts:AssumeRole",
      Effect    = "Allow",
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Allow Lambda to read the provided secret ARN (optional)
resource "aws_iam_role_policy" "lambda_secrets_read" {
  count = var.snowflake_secret_arn != "" ? 1 : 0
  role  = aws_iam_role.lambda_exec_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect : "Allow",
      Action : ["secretsmanager:GetSecretValue"],
      Resource : var.snowflake_secret_arn
    }]
  })
}

# ---------------------- ECR image URI (no repo resource needed) ----------------------
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  ecr_image_uri = "${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com/${var.ecr_repo_name}:${var.image_tag}"
}

# ---------------------- Lambda from ECR image ----------------------
resource "aws_lambda_function" "ctgov_ingestion" {
  function_name = "ctgov-ingestion-${var.environment}"
  role          = aws_iam_role.lambda_exec_role.arn

  package_type = "Image"
  image_uri    = local.ecr_image_uri

  timeout       = 300
  memory_size   = 512
  architectures = ["arm64"]


  environment {
    variables = {
      # Snowflake runtime config (your code reads creds from Secrets Manager if SNOWFLAKE_SECRET_ARN is set)
      SNOWFLAKE_DATABASE   = snowflake_database.ctgov.name
      SNOWFLAKE_SCHEMA     = snowflake_schema.raw.name
      SNOWFLAKE_TABLE      = snowflake_table.raw_ctgov_studies.name
      SNOWFLAKE_WAREHOUSE  = snowflake_warehouse.transform_wh.name
      SNOWFLAKE_SECRET_ARN = var.snowflake_secret_arn

      # Ingestion knobs
      LOG_LEVEL        = "INFO"
      CTGOV_START_DATE = "2015-01-01"
      CTGOV_END_DATE   = "MAX"
      CTGOV_PAGE_SIZE  = "100"
      CTGOV_MAX_PAGES  = "0" # 0 = all pages
      SINK             = "snowflake"
    }
  }

  # only depend on basic execution; secrets policy is optional (count may be 0)
  depends_on = [
    aws_iam_role_policy_attachment.lambda_basic_execution
  ]
}

# ---------------------- EventBridge schedule ----------------------
resource "aws_cloudwatch_event_rule" "monthly_schedule" {
  name                = "ctgov-monthly-ingestion-${var.environment}"
  description         = "Run every 2 hours on first Monday of month starting 00:00 UTC"
  schedule_expression = var.eventbridge_cron
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ctgov_ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.monthly_schedule.arn
}

resource "aws_cloudwatch_event_target" "trigger_lambda" {
  rule      = aws_cloudwatch_event_rule.monthly_schedule.name
  target_id = "CTGovLambda"
  arn       = aws_lambda_function.ctgov_ingestion.arn
}