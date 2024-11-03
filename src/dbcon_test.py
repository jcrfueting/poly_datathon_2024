from aws_advanced_python_wrapper import AwsWrapperConnection
# from psycopg import Connection

# with AwsWrapperConnection.connect(
#     Connection.connect,
#     "host=database-1.cluster-czkmkismw47i.us-west-2.rds.amazonaws.com dbname=datathon_db user=janos password=pwd",
#     plugins="aws_secrets_manager",
#     wrapper_dialect="aurora-pg",
#     autocommit=True
# ) as awsconn:
#     awscursor = awsconn.cursor()
#     awscursor.execute("SELECT aurora_db_instance_identifier()")
#     awscursor.fetchone()
#     for record in awscursor:
#         print(record)


import psycopg

awsconn = AwsWrapperConnection.connect(
        psycopg.Connection.connect,
        host="database-1.cluster-czkmkismw47i.us-west-2.rds.amazonaws.com",
        dbname="datathon_db",
        secrets_manager_secret_id="arn:aws:kms:us-west-2:668768749855:key/639153d6-69be-4d3b-93b3-7651fc881e4f",
        plugins="aws_secrets_manager"
)

awscursor = awsconn.cursor()
awscursor.execute("SELECT aurora_db_instance_identifier()")
awscursor.fetchone()
for record in awscursor:
    print(record)