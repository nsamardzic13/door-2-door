import json
from aws_cdk import (
    aws_iam as iam,
    Stack,
    Duration,
    aws_sns as sns,
    aws_sns_subscriptions as subscription,
    aws_secretsmanager as secretsmanager,
    aws_lambda as lambda_,
    aws_events as events,
    aws_events_targets as targets,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cloudwatch_actions
)
from constructs import Construct


# load config file
with open('config.json', 'r') as f:
    config = json.load(f)


class Door2DoorStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here
        
        # create SNS Topic and add subscriptions
        topic = sns.Topic(
            self,
            config["topicID"]
        )
        for email in config['emailList']:
            topic.add_subscription(
                subscription.EmailSubscription(email)
            )
        
        role = iam.Role(
            self,
            config["roleID"],
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            description='Role used in this stack'
        )

        # append permisions
        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                'AmazonSNSFullAccess'
            )
        )
        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                'AmazonS3FullAccess'
            )
        )
        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                'AmazonRDSFullAccess'
            )
        )
        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                'service-role/AWSLambdaBasicExecutionRole'
            )
        )

        # add read access to SM
        secret = secretsmanager.Secret.from_secret_complete_arn(
            self,
            config['secretID'],
            config['secretARN']
        )
        secret.grant_read(role)

        # prepare layers
        lambda_layers = []
        for layer_arn in config['layersARN']:
            layer_sufix = layer_arn.split('-')[-1]
            lambda_layers.append(
                lambda_.LayerVersion.from_layer_version_arn(
                    self,
                    f"{config['layersARN']}-{layer_sufix}",
                    layer_version_arn=layer_arn
                )
            )

        # create lambda
        lambda_function = lambda_.Function(
            self,
            config['lambdaName'],
            code=lambda_.Code.from_asset('lambda'),
            handler='main.lambda_handler',
            timeout=Duration.seconds(900),
            role=role,
            runtime=lambda_.Runtime.PYTHON_3_9,
            description='Daily Lambda for Door2Door ETL Load',
            memory_size=config['lambdaMemorySize'],
            layers=lambda_layers,
            retry_attempts=0
        )

        # create rule
        rule = events.Rule(
            self,
            config['ruleName'],
            # 00 00 * * ? *
            schedule=events.Schedule.cron(
                minute='00',
                hour='00',
                day=None,
                month=None,
                week_day=None,
                year=None,
            )
        )
        rule.add_target(
            targets.LambdaFunction(lambda_function)
        )

        # create cloudwatch
        alarm = cloudwatch.Alarm(
            self,
            config['alarmName'],
            metric=lambda_function.metric_all_errors(),
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            threshold=1,
            evaluation_periods=1
        )
        alarm.add_alarm_action(
            cloudwatch_actions.SnsAction(
                topic=topic
            )
        )