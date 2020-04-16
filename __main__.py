
import sys
import os
import pandas as pd


os.environ["KAGGLE_USERNAME"] = "chandi8203"
os.environ["KAGGLE_KEY"]="  "


import requests
import kaggle


kaggle.api.dataset_download_files('adammaus/predicting-churn-for-bank-customers', path="./kaggle", unzip=True)


from hdfs import InsecureClient  

client_hdfs = InsecureClient('http://'+os.environ['HDFS_HOST']+':50070', user=os.environ['CHANDIMA_LOGIN'])

client_hdfs.upload(hdfs_path='/OpenData/kaggle/Churn_Modelling.csv', local_path='./kaggle/Churn_Modelling.csv', overwrite=True)

