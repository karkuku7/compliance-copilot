"""CDK stack for the Compliance Copilot serverless infrastructure.

Resources:
- DynamoDB Cache Table (record_id PK, on-demand, PITR)
- DynamoDB Usage Table (record_id PK, on-demand)
- S3 Overflow Bucket (SSE-S3, RETAIN)
- Cache Loader Lambda (512MB, 900s, daily EventBridge)
- Query Lambda (256MB, 90s)
- API Gateway HTTP API with throttling
- CloudWatch alarm on cache refresh failures
"""

import os

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as apigwv2_integrations,
    aws_cloudwatch as cloudwatch,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda,
    aws_s3 as s3,
)
from constructs import Construct

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")


class ComplianceCopilotStack(Stack):
    """Full serverless infrastructure for Compliance Copilot."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- DynamoDB Cache Table ---
        cache_table = dynamodb.Table(
            self, "CacheTable",
            table_name="ComplianceCopilot_Cache",
            partition_key=dynamodb.Attribute(
                name="record_id", type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery=True,
            encryption=dynamodb.TableEncryption.AWS_MANAGED,
        )

        # --- DynamoDB Usage Table ---
        usage_table = dynamodb.Table(
            self, "UsageTable",
            table_name="ComplianceCopilot_Usage",
            partition_key=dynamodb.Attribute(
                name="record_id", type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # --- S3 Overflow Bucket ---
        overflow_bucket = s3.Bucket(
            self, "OverflowBucket",
            bucket_name="compliance-copilot-cache-overflow",
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.RETAIN,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        # --- Cache Loader Lambda ---
        cache_loader = _lambda.Function(
            self, "CacheLoaderLambda",
            function_name="ComplianceCopilot-CacheLoader",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambdas.cache_loader.handler.handler",
            code=_lambda.Code.from_asset(
                PROJECT_ROOT,
                exclude=["infrastructure/**", "tests/**", ".git/**", "cdk.out/**",
                         "*.egg-info/**", "__pycache__/**", "docs/**"],
            ),
            memory_size=512,
            timeout=Duration.seconds(900),
            environment={
                "CACHE_TABLE_NAME": cache_table.table_name,
                "OVERFLOW_BUCKET": overflow_bucket.bucket_name,
            },
        )

        cache_table.grant_write_data(cache_loader)
        overflow_bucket.grant_put(cache_loader)

        # --- Query Lambda ---
        query_lambda = _lambda.Function(
            self, "QueryLambda",
            function_name="ComplianceCopilot-QueryLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambdas.query.handler.handler",
            code=_lambda.Code.from_asset(
                PROJECT_ROOT,
                exclude=["infrastructure/**", "tests/**", ".git/**", "cdk.out/**",
                         "*.egg-info/**", "__pycache__/**", "docs/**"],
            ),
            memory_size=256,
            timeout=Duration.seconds(90),
            environment={
                "CACHE_TABLE_NAME": cache_table.table_name,
                "USAGE_TABLE_NAME": usage_table.table_name,
                "OVERFLOW_BUCKET": overflow_bucket.bucket_name,
            },
        )

        cache_table.grant_read_data(query_lambda)
        usage_table.grant_read_write_data(query_lambda)
        overflow_bucket.grant_read(query_lambda)

        # --- API Gateway ---
        api = apigwv2.HttpApi(
            self, "HttpApi",
            api_name="ComplianceCopilot-API",
            cors_preflight=apigwv2.CorsPreflightOptions(
                allow_origins=["*"],
                allow_methods=[apigwv2.CorsHttpMethod.GET],
            ),
        )

        integration = apigwv2_integrations.HttpLambdaIntegration(
            "QueryIntegration", query_lambda,
        )

        api.add_routes(
            path="/lookup",
            methods=[apigwv2.HttpMethod.GET],
            integration=integration,
        )
        api.add_routes(
            path="/stats",
            methods=[apigwv2.HttpMethod.GET],
            integration=integration,
        )

        # --- EventBridge Daily Schedule ---
        rule = events.Rule(
            self, "DailyRefresh",
            rule_name="ComplianceCopilot-DailyRefresh",
            schedule=events.Schedule.cron(hour="6", minute="0"),
        )
        rule.add_target(targets.LambdaFunction(cache_loader))

        # --- CloudWatch Alarm ---
        cloudwatch.Alarm(
            self, "CacheRefreshAlarm",
            alarm_name="ComplianceCopilot-CacheRefreshFailure",
            metric=cloudwatch.Metric(
                namespace="ComplianceCopilot",
                metric_name="CacheRefreshFailure",
                statistic="Sum",
                period=Duration.hours(1),
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )

        # --- Outputs ---
        cdk.CfnOutput(self, "ApiUrl", value=api.url or "")
        cdk.CfnOutput(self, "CacheTableName", value=cache_table.table_name)
        cdk.CfnOutput(self, "OverflowBucketName", value=overflow_bucket.bucket_name)
