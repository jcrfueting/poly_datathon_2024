from aws_advanced_python_wrapper import AwsWrapperConnection
import psycopg

awsconn = AwsWrapperConnection.connect(
    psycopg.Connection.connect,
    host="database-2.cluster-czkmkismw47i.us-west-2.rds.amazonaws.com",
    dbname="datathon_db",
    secrets_manager_secret_id="arn:aws:secretsmanager:us-west-2:668768749855:secret:rds!cluster-aa54c25e-601a-4c58-97a4-cff5aab6fb75-LYilyP",
    secrets_manager_region="us-west-2",
	plugins="aws_secrets_manager"
)

awscursor = awsconn.cursor()
awscursor.execute("SELECT aurora_db_instance_identifier()")
awscursor.fetchone()
for record in awscursor:
    print(record)
