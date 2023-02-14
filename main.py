#%%
from database_utils import DatabaseConnector 
from data_extraction import DataExtractor 
from data_cleaning  import DataCleaning
import pandas as pd

if __name__ == '__main__':

    de = DataExtractor()
    db = DatabaseConnector()
    dc = DataCleaning()

    cred = db.read_db_creds("db_creds_remote.yaml") 
    engine = db.init_db_engine(cred)
    engine.connect()

    tables_list = db.list_db_tables(engine)
    print(tables_list)
    df_name = tables_list[1]

    df = dc.clean_user_data(de.read_rds_table( engine,df_name),df_name)
    
    cred   = db.read_db_creds("db_creds local.yaml") 
    engine = db.init_db_engine(cred)
    engine.connect()
    db.upload_to_db(df,'dim_users',engine)

    print(df.head())   
    print(df.info())
    print(df.describe())
 #   df.drop(df['store_code'].isnull().index,inplace=True)
#    df = df[df['store_code'].notnull()]
 #   df = df[df['address'].notnull()]
 #   df['opening_date'] = pd.to_datetime(df['opening_date'], errors = 'coerce')

#    print(df.info())
 #       df.reset_index()

    
#    filt = df['store_code'] != 'None'    
 #   df = df.drop(index = df[filt].index)
#    print(df["address"].isnull())

 #   print(table['store_code'].duplicated().sum())
    
#    print(table[table['store_code'].duplicated()])

 #   print(table[table['index']== 405])

    #print(table.columns())
#    for col in table.columns:
 #       print(col)
#    print(table['user_uuid'].duplicated().sum())        

 #   table.dropna(inplace=True)
#    print(table.head(5))
#    print(table.info())
 #   print(table.describe())

 #    table.drop(0,axis = 'index',inplace=True)    
#    print(table[table['address'] =='N/A'])
 #   table["opening_date"] = pd.to_datetime(table["opening_date"], format = "%Y-%m-%d")     print(table["opening_date"][1])

# %%
