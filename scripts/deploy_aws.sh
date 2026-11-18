#!/usr/bin/env bash
# Deploy the pipeline to your AWS account and run it once on sample data.
#
#   ./scripts/deploy_aws.sh <stack-name> <region> [alert-email]
#
# Needs: AWS CLI v2 with credentials, Python 3.10+, zip. Creates billable resources
# (Glue, Redshift Serverless); run ./scripts/destroy_aws.sh when you are done.
set -euo pipefail

STACK=${1:?usage: deploy_aws.sh <stack-name> <region> [alert-email]}
REGION=${2:?usage: deploy_aws.sh <stack-name> <region> [alert-email]}
EMAIL=${3:-}
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
ARTIFACTS="${STACK}-artifacts-${ACCOUNT}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$ROOT/build"

echo "==> Packaging code"
rm -rf "$BUILD" && mkdir -p "$BUILD"
(cd "$ROOT/src" && zip -qr "$BUILD/delivery_pipeline.zip" delivery_pipeline -x "*__pycache__*")
cp "$BUILD/delivery_pipeline.zip" "$BUILD/ingest.zip"   # Lambda uses only the stdlib + boto3 parts

echo "==> Artifacts bucket s3://$ARTIFACTS"
aws s3api head-bucket --bucket "$ARTIFACTS" 2>/dev/null || {
  if [ "$REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$ARTIFACTS" --region "$REGION"
  else
    aws s3api create-bucket --bucket "$ARTIFACTS" --region "$REGION" \
      --create-bucket-configuration LocationConstraint="$REGION"
  fi
  aws s3api put-public-access-block --bucket "$ARTIFACTS" --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
}
aws s3 cp "$BUILD/delivery_pipeline.zip" "s3://$ARTIFACTS/glue/delivery_pipeline.zip" --sse AES256
aws s3 cp "$ROOT/jobs/glue_etl_job.py"   "s3://$ARTIFACTS/glue/glue_etl_job.py"     --sse AES256
aws s3 cp "$BUILD/ingest.zip"            "s3://$ARTIFACTS/lambda/ingest.zip"        --sse AES256

echo "==> Deploying CloudFormation stack $STACK"
aws cloudformation deploy --region "$REGION" --stack-name "$STACK" \
  --template-file "$ROOT/infra/cloudformation.yaml" \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides ArtifactsBucket="$ARTIFACTS" AlertEmail="$EMAIL"

output() {
  aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}
LAKE=$(output DataLakeBucketName)
ROLE=$(output RedshiftCopyRoleArn)
WORKGROUP=$(output RedshiftWorkgroupName)

echo "==> Uploading reference data and a sample batch"
python -m delivery_pipeline.generate_data --lake "$BUILD/lake" --packages 50000
aws s3 cp "$BUILD/lake/reference/" "s3://$LAKE/reference/" --recursive --sse AES256
aws s3 cp "$BUILD/lake/landing/"   "s3://$LAKE/landing/"   --recursive --sse AES256
echo "    The S3 trigger now moves the files to raw/ and starts the Glue job."

cat <<EOF

Deployed.
  Data lake:      s3://$LAKE
  Firehose:       $(output FirehoseStreamName)
  Glue job:       $(output GlueJobName)
  Redshift:       workgroup $WORKGROUP, database 'delivery'

When the Glue run succeeds, create and load the warehouse:
  aws redshift-data execute-statement --region $REGION --workgroup-name $WORKGROUP --database delivery \\
      --sql "\$(cat sql/redshift/01_create_star_schema.sql)"
  sed -e "s#:bucket#$LAKE#g" -e "s#:role_arn#$ROLE#g" sql/redshift/02_load_from_s3.sql > build/load.sql
  # then run build/load.sql in the Redshift Query Editor v2 (or execute-statement per statement)
EOF
