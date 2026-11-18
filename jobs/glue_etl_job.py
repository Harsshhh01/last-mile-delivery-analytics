"""AWS Glue (Spark) entry point. Uploaded to S3 by scripts/deploy_aws.sh.

The pipeline code ships as --extra-py-files (delivery_pipeline.zip), so this job runs
exactly the ETL that is unit-tested locally.

Job arguments:
    --lake_root          s3://<data-lake-bucket>
    --run_date           YYYY-MM-DD, or "auto" for today (IST)
    --min_dq_pass_rate   e.g. 0.95
    --pii_salt_secret    Secrets Manager secret id holding the PII hashing salt
"""

import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from delivery_pipeline.config import PipelineConfig
from delivery_pipeline.etl import run_etl
from delivery_pipeline.ingest import today_ist
from delivery_pipeline.metrics import RunMetrics
from delivery_pipeline.spark_session import apply_conf

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "lake_root", "run_date", "min_dq_pass_rate", "pii_salt_secret"]
)
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = apply_conf(glue_context.spark_session)
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

run_date = today_ist() if args["run_date"] == "auto" else args["run_date"]
salt = boto3.client("secretsmanager").get_secret_value(SecretId=args["pii_salt_secret"])["SecretString"]
cfg = PipelineConfig(
    lake_root=args["lake_root"],
    run_date=run_date,
    min_dq_pass_rate=float(args["min_dq_pass_rate"]),
    pii_salt=salt,
)
metrics = RunMetrics(pipeline=args["JOB_NAME"], run_date=run_date)
try:
    result = run_etl(spark, cfg, metrics)
    print(result)
finally:
    # Publish whatever was measured, including the DQ pass rate of a failed batch,
    # so the CloudWatch alarm fires on bad data.
    metrics.publish_cloudwatch()
job.commit()
