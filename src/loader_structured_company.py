###
###     Script for loading the structured data into db
###

from functions import *

###     database connection

from aws_advanced_python_wrapper import AwsWrapperConnection
import psycopg

# awsconn = AwsWrapperConnection.connect(
#     psycopg.Connection.connect,
#     host="database-3.cluster-czkmkismw47i.us-west-2.rds.amazonaws.com",
#     dbname="datathon_db",
#     secrets_manager_secret_id="arn:aws:secretsmanager:us-west-2:668768749855:secret:rds!cluster-3d5a56e7-5726-4a32-bfcb-71ed527de975-0HPBdb",
#     secrets_manager_region="us-west-2",
# 	plugins="aws_secrets_manager"
# )

conn = psycopg.connect("postgresql://janos:(q0)r:0|QL>N[}b>>PA<6Eu6fE5x@database-3.cluster-czkmkismw47i.us-west-2.rds.amazonaws.com/datathon_db")

# c = awsconn.cursor()
c = conn.cursor()

###     getting data and writing to db

import pandas as pd
import pandas.io.sql as sqlio
import yfinance as yf
import pandas_ta as ta
from io import StringIO


metadata = get_table("metadata", conn)
# c.execute("SELECT * FROM metadata").fetchall()

# actual loading
for t in metadata["yf_ticker"]:
    load_technical(t, conn)


###     exit
conn.commit()
conn.close()
