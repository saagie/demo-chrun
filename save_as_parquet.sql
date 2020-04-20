                               
--Save Churn_Modelling.csv as a parquet to HDFS

--Create a database in Impala

CREATE DATABASE IF NOT EXISTS churn_bank;

--Store Churn_Modelling.csv as a table 

DROP TABLE IF EXISTS churn_bank.bank;

CREATE EXTERNAL TABLE churn_bank.bank (
RowNumber int,
CustomerId int,
Surname string,
CreditScore int,
Geography string,
Gender string,
Age int,
Tenure int,
Balance float,
NumOfProducts int,
HasCrCard int,
IsActiveMember int,
EstimatedSalary float,
Exited int )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION  '/OpenData/kaggle'  
tblproperties ("skip.header.line.count"="1");

--Save Impala table as a Parquet

DROP TABLE IF EXISTS churn_bank.clean_bank;

CREATE TABLE churn_bank.clean_bank STORED AS PARQUET * FROM churn_bank.bank ;

