import sys
import os
import logging
import pandas as pd



log_format = "%(asctime)s::%(levelname)s::%(name)s::"\
             "%(filename)s::%(lineno)d::%(message)s"
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=log_format)

def readData():
	data = pd.read_csv('https://saagiedemo-bankchurn.s3.eu-west-3.amazonaws.com/Churn_Modelling.csv')
	logging.info('Step 1: Read CSV from AWS S3: OK!!')
	return data
	
data = readData()


from hdfs import InsecureClient 
def connect_hdfs():
	client_hdfs = InsecureClient('http://'+os.environ['HDFS_HOST']+':50070', user=os.environ['CHANDIMA_LOGIN'])
	logging.info('Step 2: Connect to HDFS: OK!!')
	return client_hdfs

client_hdfs = connect_hdfs()

def write_to_hdfs():
	with client_hdfs.write('/OpenData/kaggle/Churn_Modelling.csv',overwrite=True, encoding = 'utf-8') as writer:
		logging.info('Step 3: Write to HDFS: OK!!')
		return data.to_csv(writer)

write_to_hdfs()    


print("Data Extraction is Succeeded!!!")




