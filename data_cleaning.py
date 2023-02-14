import pandas as pd

class DataCleaning:

    def clean_user_data(self,df,df_name):

        if  df_name == 'legacy_store_details':
            df.drop(columns='lat',inplace=True)
            df = self.clean_invalid_date(df,'opening_date')            
            df = self.clean_NaNs_Nulls_misses(df)
        
        if  df_name == 'legacy_users':
            df = self.clean_invalid_date(df,'date_of_birth')            
            df = self.clean_invalid_date(df,'join_date')            
            df = self.clean_NaNs_Nulls_misses(df)
  #          df = self.clean_NaNs_Nulls_misses(df)

        if  df_name == 'orders_table':
            df.drop(columns='1',inplace=True)
           # df = self.clean_NaNs_Nulls_misses(df)

        return df

    def clean_NaNs_Nulls_misses(self,df):
#       There should be better way
        list_of_removal = ['None','N/A']
        for col in df.columns:            
            df.dropna(subset = col,how='any',inplace= True)
            for _ in list_of_removal:
                filt = df[col] == _
                df.drop(index = df[filt].index,inplace =True)
#                df.reset_index(inplace=True)
        return df
            
    def clean_invalid_date(self,df,column_name):
#       There should be better way
        df[column_name] = pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%Y %B %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%B %Y %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
        df.dropna(subset = column_name,how='any',inplace= True)
        df.reset_index()       
        return df