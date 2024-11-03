from aws_advanced_python_wrapper import AwsWrapperConnection
from psycopg import Connection

with AwsWrapperConnection.connect(
    Connection.connect,
    "host=database-1.cluster-czkmkismw47i.us-west-2.rds.amazonaws.com dbname=datathon_db user=janos password=pwd",
    plugins="failover",
    wrapper_dialect="aurora-pg",
    autocommit=True
) as awsconn:
    awscursor = awsconn.cursor()
    awscursor.execute("SELECT aurora_db_instance_identifier()")
    awscursor.fetchone()
    for record in awscursor:
        print(record)

