# ---------------------- Snowflake only ----------------------

# Database
resource "snowflake_database" "ctgov" {
  name    = "CLINICAL_TRIALS_${upper(var.environment)}"
  comment = "ClinicalTrials.gov data database"
}

# Schemas
resource "snowflake_schema" "raw" {
  database   = snowflake_database.ctgov.name
  name       = "RAW"
  comment    = "Raw JSON data from API"
  depends_on = [snowflake_database.ctgov]
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

# Landing table in RAW
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

# Warehouse used by ingestion & dbt
resource "snowflake_warehouse" "transform_wh" {
  name           = "TRANSFORM_WH"
  warehouse_size = "X-SMALL"
  auto_suspend   = 60
}
