###
###     Script for loading the structured data into db
###

from src.functions import *

###     database connection

from aws_advanced_python_wrapper import AwsWrapperConnection
import psycopg

awsconn = AwsWrapperConnection.connect(
    psycopg.Connection.connect,
    host="database-1.cluster-czkmkismw47i.us-west-2.rds.amazonaws.com",
    dbname="datathon_db",
    secrets_manager_secret_id="arn:aws:secretsmanager:us-west-2:668768749855:secret:rds!cluster-72ab118b-8a0b-491d-9c6b-4b0a49833510-bxwrMt",
    secrets_manager_region="us-west-2",
	plugins="aws_secrets_manager"
)
c = awsconn.cursor()


###     getting data and writing to db

import pandas as pd
import pandas.io.sql as sqlio
import yfinance as yf
import pandas_ta as ta
from io import StringIO

metadata = sqlio.read_sql_query("SELECT * FROM metadata", awsconn)
# c.execute("SELECT * FROM metadata").fetchall()

# actual loading
for t in metadata["ticker"]:
    load_technical(t, awsconn)


###     exit
awsconn.commit()
awsconn.close()