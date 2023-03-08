# Data Centralisation Project
In this project our goal is to prepare data to be uploaded into the PostgreSQL database in and show some data insights using SQL queries. 

## Project Structure

## Project utils

During this project we need to perform three types of tasks:

1. Data extraction. In "data_extraction.py" we store methods responsible for the upload data into pandas dataframe from different sources. 
2. Data cleaning. In "data_cleaning.py" we develope class DataCleaning that clean different tables, that we uploaded in "data_extraction.py". 
3. Uploading data into database. We write DatabaseConnector class "database_utils.py", which initiating database engine basing on credentials provided in "*.yml" file.
 
## Step by step data download

We have 6 different data sources with data. 

1. Remote Postgres database in AWS Cloud. The table "order_table" is the data of the most interest for the client as it contain actual sales information. In the table we need to use the following fields "date_uuid","user_uuid","card_number","store_code","product_code" and "product_quantity". First 5 fields will become foreign keys in our database, therefore we need to clean this columns from all Nans and missing values. The "product_quantity" field has to be an integer.
2. Remote Postgres database in AWS Cloud. The users data  "dim_users" table. This table also stored in remote database, so we use the same upload technics as in the previus case. Primary key here is the "user_uuid" field.
3. Public link in AWS cloud. The "dim_card_details" is accessable by link from s3 server and stored as ".pdf" file. We handle reading ".pdf" using tabula package. The primary key is the card number. The card number has to be converted into string to avoid possible problems and cleaned from "?" artifacts.
4. The AWS-s3 bucket. The "dim_product" table. We utilise boto3 package to download this data. The primary key is the "product code" field. The field "product_price" has to be converted into float number and field "weight" has to convert into grams concerning cases like ("kg","oz","l","ml").
5. The restful-API.  The "dim_store_details" data is availble by GET method. The ".json" response has to be converted into the pandas dataframe. The primary key field is "store_code".
6. The "dim_date_times" data is available by link. The ".json" response has to be converted into the pandas datagrame. The primary key is "date_uuid".
 

## General Data Cleaning Notes

1. All data cleaning has to be performed in concern of "primary key" field. Therefore, we remove raws of the table only in the case, if duplicates (NaNs, missing value etc) appear in this field. Otherwise, there is a risk that in the "foreign key" in the "orders_table" will not be found in "primary key" and database schema would not work.
2. The date transformation has to account for different time formats, so we fix this issie in the following way

```
        df[column_name] = pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%Y %B %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%B %Y %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')

```

## SQL queries

%%  example
