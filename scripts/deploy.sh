#!/usr/bin/env bash
set -euo pipefail

# -------- Config (defaults; overridable via flags) ----------
ENV="${ENV:-dev}"
REGION="${REGION:-us-east-1}"
ECR_REPO_NAME="${ECR_REPO_NAME:-ctgov-ingestion}"   # final repo will be exactly this
IMAGE_TAG="${IMAGE_TAG:-latest}"

# Required for Terraform (passed down); keeps Snowflake password out of TF state
SNOWFLAKE_SECRET_ARN="${SNOWFLAKE_SECRET_ARN:-}"    # e.g. arn:aws:secretsmanager:...:secret:ctgov/snowflake/ingestion-XXXX

# -------- Flags ---------------------------------------------
usage() {
  cat <<EOF
Usage: $0 [--env dev] [--region us-east-1] [--repo ctgov-ingestion] [--tag latest] [--secret-arn <ARN>]
Environment variables also supported: ENV, REGION, ECR_REPO_NAME, IMAGE_TAG, SNOWFLAKE_SECRET_ARN

Examples:
  $0 --env dev --region us-east-1 --repo ctgov-ingestion --tag latest --secret-arn arn:aws:secretsmanager:...
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env) ENV="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    --repo) ECR_REPO_NAME="$2"; shift 2 ;;
    --tag) IMAGE_TAG="$2"; shift 2 ;;
    --secret-arn) SNOWFLAKE_SECRET_ARN="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 1 ;;
  esac
done

# -------- Pretty logging ------------------------------------
green(){ printf "\033[0;32m==>\033[0m %s\n" "$*"; }
err(){   printf "\033[0;31mERR:\033[0m %s\n" "$*" >&2; }
trap 'err "Failed at line $LINENO. See output above."' ERR

# -------- Pre-flight checks --------------------------------
green "Checking tooling"
command -v aws >/dev/null || { err "aws cli not found"; exit 1; }
command -v docker >/dev/null || { err "docker not found"; exit 1; }
command -v terraform >/dev/null || { err "terraform not found"; exit 1; }

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
[[ -n "$ACCOUNT_ID" && "$ACCOUNT_ID" != "None" ]] || { err "AWS credentials not configured"; exit 1; }
green "AWS identity: account=$ACCOUNT_ID, region=$REGION"

# -------- Build container ----------------------------------
green "Building Docker image ingestion -> ${ECR_REPO_NAME}:${IMAGE_TAG}"
[[ -f ingestion/Dockerfile ]] || { err "ingestion/Dockerfile not found"; exit 1; }
docker build -t "${ECR_REPO_NAME}:${IMAGE_TAG}" ingestion

# -------- Ensure ECR repo exists ----------------------------
green "Ensuring ECR repo ${ECR_REPO_NAME} exists"
if ! aws ecr describe-repositories --repository-names "${ECR_REPO_NAME}" --region "${REGION}" >/dev/null 2>&1; then
  if ! aws ecr create-repository --repository-name "${ECR_REPO_NAME}" --region "${REGION}" >/dev/null 2>&1; then
    err "Cannot create ECR repo ${ECR_REPO_NAME}. Ask an admin to create it or grant ecr:CreateRepository."
    exit 1
  fi
fi
ECR_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPO_NAME}"

# -------- ECR login + push ---------------------------------
green "ECR login"
aws ecr get-login-password --region "${REGION}" \
| docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

green "Tag & push ${ECR_REPO_NAME}:${IMAGE_TAG} -> ${ECR_URI}:${IMAGE_TAG}"
docker tag "${ECR_REPO_NAME}:${IMAGE_TAG}" "${ECR_URI}:${IMAGE_TAG}"
docker push "${ECR_URI}:${IMAGE_TAG}"

# -------- Terraform apply ----------------------------------
green "Terraform: init/validate/apply"
pushd terraform >/dev/null

# If a Secrets Manager ARN is provided, also try to hydrate Snowflake provider creds
# from the secret so the operator doesn't need to export TF_VAR_snowflake_* manually.
if [[ -n "${SNOWFLAKE_SECRET_ARN}" ]]; then
  green "Fetching Snowflake provider creds from Secrets Manager (for terraform provider only)"
  # Expect secret JSON like: {"account":"ACCT","user":"USER","password":"PASS"}
  SECRET_JSON="$(aws secretsmanager get-secret-value \
    --secret-id "${SNOWFLAKE_SECRET_ARN}" \
    --query SecretString --output text --region "${REGION}")" || {
      err "Unable to read secret ${SNOWFLAKE_SECRET_ARN}. Ensure your IAM user can GetSecretValue."
      exit 1
    }

  # Parse without jq (portable): use python
  TF_VAR_snowflake_account="$(python - <<'PY' "$SECRET_JSON"
import json,sys
print(json.loads(sys.argv[1])["account"])
PY
)"
  TF_VAR_snowflake_user="$(python - <<'PY' "$SECRET_JSON"
import json,sys
print(json.loads(sys.argv[1])["user"])
PY
)"
  TF_VAR_snowflake_password="$(python - <<'PY' "$SECRET_JSON"
import json,sys
print(json.loads(sys.argv[1])["password"])
PY
)"
  export TF_VAR_snowflake_account TF_VAR_snowflake_user TF_VAR_snowflake_password
else
  green "SNOWFLAKE_SECRET_ARN not provided. Expecting TF_VAR_snowflake_account/user/password in env."
  : "${TF_VAR_snowflake_account:?Missing TF_VAR_snowflake_account}"
  : "${TF_VAR_snowflake_user:?Missing TF_VAR_snowflake_user}"
  : "${TF_VAR_snowflake_password:?Missing TF_VAR_snowflake_password}"
fi

terraform init -reconfigure
terraform fmt
terraform validate

terraform apply -auto-approve \
  -var="aws_region=${REGION}" \
  -var="environment=${ENV}" \
  -var="ecr_repo_name=${ECR_REPO_NAME}" \
  -var="image_tag=${IMAGE_TAG}" \
  -var="snowflake_secret_arn=${SNOWFLAKE_SECRET_ARN}"

popd >/dev/null

green "Done! Lambda will use image ${ECR_URI}:${IMAGE_TAG}"
green "Invoke once to create logs: aws lambda invoke --function-name ctgov-ingestion-${ENV} --payload '{}' /tmp/out.json --region ${REGION}"
