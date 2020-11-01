from aws_cdk import core
from aws_cdk import aws_dynamodb
from aws_cdk import aws_lambda
from aws_cdk import aws_sns
from aws_cdk import aws_events
from aws_cdk import aws_events_targets


class cdkStack(core.Stack):
	def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
		super().__init__(scope, id, **kwargs)

		db_table_name = "findAflatDB"

		# create dynamoDB table
		table = aws_dynamodb.Table(
			self, "findAflat_db",
			table_name = db_table_name,
			partition_key=aws_dynamodb.Attribute(
				name="dataID",
				type=aws_dynamodb.AttributeType.STRING
			)
		)

		# create lambda function
		function = aws_lambda.Function(
			self, "findAflat_fnc",
			runtime = aws_lambda.Runtime.PYTHON_3_7,
			code = aws_lambda.Code.asset("findAFlat"),
			handler = "lambda_function.lambda_handler",
			timeout = core.Duration.minutes(1)
		)

		function.add_environment("LAMBDA","True")
		function.add_environment("DB_TABLE", db_table_name)
		table.grant_read_write_data(function)

		# create and configure sns topic
		sns_topic = aws_sns.Topic(
			self, "findAflat_sns_tpc",
			display_name = "flatsWeeklyUpdate",
			topic_name = "flatsWeeklyUpdate"
		)

		sns_topic.grant_publish(function)

		sns_subscr = aws_sns.Subscription(
			self, "findAflat_sns_sbscr",
			topic = sns_topic,
			protocol = aws_sns.SubscriptionProtocol.EMAIL,
			endpoint = "nati.ernst@gmail.com"
		)

		function.add_environment("SNS_TOPIC", sns_topic.topic_arn)
		
		# add EventBridge
		event = aws_events.Rule(
			self, "findAflat_event",
			enabled = True, 
			rule_name = "daily_at_17",
			schedule = aws_events.Schedule.cron(hour = "17", minute = "00")
		)

		event.add_target(aws_events_targets.LambdaFunction(handler=function))


app = core.App()
cdkStack(app, "findAflat", env={'region':'eu-central-1'})

app.synth()