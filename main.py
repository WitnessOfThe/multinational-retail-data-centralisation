#%%
import pandas as pd
from database_utils  import DatabaseConnector 
from data_extraction import DataExtractor 
from data_cleaning   import DataCleaning

def upload_dim_users():
    de = DataExtractor()
    db = DatabaseConnector()
    dc = DataCleaning()
    # connect to base and get list of frames
    cred   = db.read_db_creds("db_creds_remote.yaml") 
    engine = db.init_db_engine(cred)
    engine.connect()
    tables_list = db.list_db_tables(engine)
    # get clean chosen frame
    df_name = tables_list[1]
    df = dc.clean_user_data(de.read_rds_table( engine, df_name))
    print(df.head())
    # upload to the db
    cred   = db.read_db_creds("db_creds local.yaml") 
    engine = db.init_db_engine(cred)
    engine.connect()
    db.upload_to_db(df,'dim_users',engine)

def upload_dim_card_details():
    de = DataExtractor()
    db = DatabaseConnector()
    dc = DataCleaning()  
    # get data from pdf
    df = de.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    print(df.head())
    print(df.info())
    # clean data
    df = dc.clean_card_data(df)
    print(df.info())
    print(df.head())
    # upload to the db
    cred   = db.read_db_creds("db_creds local.yaml") 
    engine = db.init_db_engine(cred)
    db.upload_to_db(df,'dim_card_details',engine)

def upload_dim_products():
    de = DataExtractor()
    db = DatabaseConnector()
    dc = DataCleaning()  
    # get data from s3
    df =  de.extract_from_s3()
    df =  dc.convert_product_weights(df,'weight')
#    df.to_csv('dim_products.csv')
    # clean data 
    df =  dc.clean_products_data(df)
    print(df['product_price'].sum())
    # upload to db 
    cred   = db.read_db_creds("db_creds local.yaml") 
    engine = db.init_db_engine(cred)
    engine.connect()
    
#    db.upload_to_db(df,'dim_products',engine)

def upload_dim_store_details():
    de = DataExtractor()
    db = DatabaseConnector()
    dc = DataCleaning()  
    # get data
    df = de.retrieve_stores_data()
    print(df[df['store_code']=='WEB-1388012W'])
    df.to_csv('dim_store_details.csv')
    # clean data 
    df = dc.called_clean_store_data(df)
    # upload to db 
    cred   = db.read_db_creds("db_creds local.yaml") 
    engine = db.init_db_engine(cred)
    engine.connect()
 #   db.upload_to_db(df,'dim_store_details',engine)

def upload_orders():
    de = DataExtractor()
    db = DatabaseConnector()
    dc = DataCleaning()
    # connect to db
    cred   = db.read_db_creds("db_creds_remote.yaml") 
    engine = db.init_db_engine(cred)
    engine.connect()
    tables_list = db.list_db_tables(engine)
    # get frame name and download
    df_name = tables_list[2]
    df = de.read_rds_table( engine, df_name)
    df.to_csv('orders_table.csv')    
    # clean data 
    df = dc.clean_order_data(df)
#    print(df.info())
    print(df['product_quantity'].sum())
    # upload to db 
    cred   = db.read_db_creds("db_creds local.yaml") 
    engine = db.init_db_engine(cred)
    engine.connect()
#    db.upload_to_db(df,'orders_table',engine)

def dim_date_times():
    de = DataExtractor()
    db = DatabaseConnector()
    dc = DataCleaning()
    df = de.extract_from_s3_by_link()
    df.to_csv('dim_date_times.csv')
    
    df = dc.clean_date_time(df)
    cred   = db.read_db_creds("db_creds local.yaml") 
    engine = db.init_db_engine(cred)
    engine.connect()
#    db.upload_to_db(df,'dim_date_times',engine)

if __name__ == '__main__':
  #  upload_orders()
   # upload_dim_products()
    #dim_date_times()
#  upload_orders()
    upload_dim_store_details()


# %%
