import sys
import os
import pandas as pd

data = pd.read_csv('https://saagiedemo-bankchurn.s3.eu-west-3.amazonaws.com/Churn_Modelling.csv')

from hdfs import InsecureClient  

client_hdfs = InsecureClient('http://'+os.environ['HDFS_HOST']+':50070', user=os.environ['CHANDIMA_LOGIN'])

with client_hdfs.write('/OpenData/kaggle/Churn_Modelling.csv',overwrite=True, encoding = 'utf-8') as writer:
    data.to_csv(writer)

print("Success !!!")