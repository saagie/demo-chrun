import ibis
import pandas as pd
import os
import logging
import sys

log_format = "%(asctime)s::%(levelname)s::%(name)s::"\
             "%(filename)s::%(lineno)d::%(message)s"
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=log_format)


def connect_hdfs():
	hdfs = ibis.hdfs_connect(host=os.environ['HDFS_HOST'], port=50070)
	logging.info('Step 1: Connect to HDFS: OK!!')
	return hdfs

hdfs = connect_hdfs()

def connect_impala():
	client_impala = ibis.impala.connect(host=os.environ['IP_IMPALA'], port=21050,hdfs_client=hdfs, user=os.environ['CHANDIMA_LOGIN'], password=os.environ['CHANDIMA_PSWD'],auth_mechanism='PLAIN')
	logging.info('Step 2: Connect to Impala: OK!!')
	return client_impala

client_impala = connect_impala()


client_impala.raw_sql('CREATE DATABASE IF NOT EXISTS churn_bank3')
logging.info('Step 3: Create a Database: OK!!')
	#return client_impala

client_impala.raw_sql('DROP TABLE IF EXISTS churn_bank3.bank')
client_impala.raw_sql("CREATE EXTERNAL TABLE churn_bank3.bank (Id int,RowNumber int,CustomerId int,Surname string,CreditScore int,Geography string,Gender string,Age int,Tenure int,Balance float,NumOfProducts int,HasCrCard int,IsActiveMember int,EstimatedSalary float,Exited int )ROW FORMAT DELIMITED FIELDS TERMINATED BY ','LOCATION  '/OpenData/kaggle'  tblproperties ('skip.header.line.count'='1')")
logging.info('Step 4: Create Table: OK!!')
 	#return client_impala

client_impala.raw_sql('DROP TABLE IF EXISTS churn_bank3.clean_bank')
client_impala.raw_sql("""CREATE TABLE churn_bank3.clean_bank STORED AS PARQUET AS SELECT * from churn_bank3.bank""")
logging.info('Step 5: Save as Parquet: OK!!')

