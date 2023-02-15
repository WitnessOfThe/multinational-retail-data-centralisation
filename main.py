#%%
from database_utils import DatabaseConnector 
from data_extraction import DataExtractor 
from data_cleaning  import DataCleaning
import pandas as pd

if __name__ == '__main__':

    de = DataExtractor()
    db = DatabaseConnector()
    dc = DataCleaning()

#    cred   = db.read_db_creds("db_creds_remote.yaml") 
 #   engine = db.init_db_engine(cred)
  #  engine.connect()

#    tables_list = db.list_db_tables(engine)
 #   df_name = tables_list[1]
  #  df = dc.clean_user_data(de.read_rds_table( engine, df_name))
   # print(df.head())

#    cred   = db.read_db_creds("db_creds local.yaml") 
 #   engine = db.init_db_engine(cred)
  #  engine.connect()
   # db.upload_to_db(df,'dim_users',engine)

## Call From PDF
#    df = de.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
 #   print(df.head())
  #  df = dc.clean_card_data(df)

#    cred   = db.read_db_creds("db_creds local.yaml") 
 #   engine = db.init_db_engine(cred)
  #  engine.connect()
   # db.upload_to_db(df,'dim_card_details',engine)
     
#    print(df.head())
#    print(df.info())

## API Calls
#    df = de.retrieve_stores_data()
 #   print(df.head())
  #  print(df.info())
   # df = dc.called_clean_store_data(df)
   # print(df.head())
   # print(df.info())
   # cred   = db.read_db_creds("db_creds local.yaml") 
   # engine = db.init_db_engine(cred)
   # engine.connect()
   # db.upload_to_db(df,'dim_store_details',engine)

## CSV Call

#    df =  de.extract_from_s3()
#    print(df.head())
#    print(df.info())    
#    df =  dc.convert_product_weights(df,'weight')
#    df =  dc.clean_products_data(df)
#    print(df.head())
#    print(df.info())    
#    cred   = db.read_db_creds("db_creds local.yaml") 
#    engine = db.init_db_engine(cred)
#    engine.connect()
#    db.upload_to_db(df,'dim_products',engine)


## rdb_call_for_orders

    cred   = db.read_db_creds("db_creds_remote.yaml") 
    engine = db.init_db_engine(cred)
    engine.connect()

    tables_list = db.list_db_tables(engine)
    df_name = tables_list[2]
    df = de.read_rds_table( engine, df_name)
    print(df.head())
    print(df.info())    
    df = dc.clean_order_data(df)
    print(df.head())
    print(df.info())    

    cred   = db.read_db_creds("db_creds local.yaml") 
    engine = db.init_db_engine(cred)
    engine.connect()
    db.upload_to_db(df,'orders_table',engine)
# %%
