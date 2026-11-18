#!/usr/bin/env bash
# Tear down everything deploy_aws.sh created, so nothing keeps billing.
#   ./scripts/destroy_aws.sh <stack-name> <region>
set -euo pipefail
STACK=${1:?usage: destroy_aws.sh <stack-name> <region>}
REGION=${2:?usage: destroy_aws.sh <stack-name> <region>}
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
LAKE="${STACK}-datalake-${ACCOUNT}"
ARTIFACTS="${STACK}-artifacts-${ACCOUNT}"

empty_bucket() {  # versioned buckets need every version and delete marker removed
  local bucket=$1
  aws s3api head-bucket --bucket "$bucket" 2>/dev/null || return 0
  python - "$bucket" <<'PY'
import sys, boto3
b = boto3.resource("s3").Bucket(sys.argv[1])
b.object_versions.delete()
b.objects.all().delete()
PY
}

empty_bucket "$LAKE"
aws cloudformation delete-stack --region "$REGION" --stack-name "$STACK"
aws cloudformation wait stack-delete-complete --region "$REGION" --stack-name "$STACK"
empty_bucket "$ARTIFACTS"
aws s3api delete-bucket --bucket "$ARTIFACTS" --region "$REGION" || true
echo "Stack $STACK and its buckets are deleted."
