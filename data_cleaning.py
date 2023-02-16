import pandas as pd
import numpy as np
class DataCleaning:

    def clean_user_data(self,df):
        df = self.clean_invalid_date(df,'date_of_birth')
        df = self.clean_invalid_date(df,'join_date')        
        df = self.clean_NaNs_Nulls_misses(df)
        df.drop(columns='1',inplace=True)
        return df

    def clean_order_data(self,df):
        df.drop(columns='1',inplace=True)
        df.drop(columns='first_name',inplace=True)
        df.drop(columns='last_name',inplace=True)
        df.drop(columns='level_0',inplace=True)
        df.dropna(how='any',inplace= True)
        return df

    def called_clean_store_data(self,df):
        df.drop(columns='lat',inplace=True)
        df = self.clean_invalid_date(df,'opening_date')             
        df.dropna(subset = 'country_code',how='any',inplace= True)
        return df

    def clean_card_data(self,df):
        df['card_number']            =  pd.to_numeric( df['card_number'],errors='coerce', downcast="integer")
        df['expiry_date']            =  pd.to_datetime(df['expiry_date'], format='%m/%d', errors='coerce')
        df['card_provider']          =  df['card_provider'].astype('str',errors='ignore')
        df['date_payment_confirmed'] =  pd.to_datetime(df['date_payment_confirmed'], format='%Y-%m-%d', errors='coerce') 
        df.dropna(how='any',inplace= True)
        return df

    def clean_date_time(self,df):
        df['month']         =  pd.to_numeric( df['month'],errors='coerce', downcast="integer")
        df['year']          =  pd.to_numeric( df['year'], errors='coerce', downcast="integer")
        df['day']           =  pd.to_numeric( df['day'], errors='coerce', downcast="integer")
        df['timestamp']     =  pd.to_datetime(df['timestamp'], format='%H:%M:%S', errors='coerce')
        df.dropna(how='any',inplace= True)
        df.reset_index(inplace=True)       
        return df

    def clean_products_data(self,df):
        df['date_added']            =  pd.to_datetime(df['date_added'], format='%Y-%m-%d', errors='coerce')
        df.dropna(how='any',inplace= True)
        df.reset_index(inplace=True)       
        return df

    def convert_product_weights(self,df,column_name):
#        df[column_name] = df.apply(lambda x:self.get_grams(x[column_name]))
        df[column_name] = df[column_name].apply(self.get_grams)
        return df

    def get_grams(self,value):
        value = str(value)
        if value.endswith('kg'):
            value = value.replace('kg','')
            return 1000*float(value) if self.isfloat(value) else np.nan
        elif value.endswith('g'):   
            value = value.replace('g','')
            return float(value) if self.isfloat(value) else np.nan
        elif value.endswith('ml'):   
            value = value.replace('ml','')
            return float(value) if self.isfloat(value) else np.nan
        elif value.endswith('l'):   
            value = value.replace('l','')
            return 1000*float(value) if self.isfloat(value) else np.nan
        else:
            np.nan

    def isfloat(self,num):
        try:
            float(num)
            return True
        except ValueError:
            return False

    def clean_custom_str(self,df):
#       There should be better way
        list_of_removal = ['None','N/A']        
        for col in df.columns:            
            for _ in list_of_removal:
                filt = df[col] == _
                df.drop(index = df[filt].index,inplace =True)
        return df
            
    def clean_invalid_date(self,df,column_name):
#       There should be better way
        df[column_name] = pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%Y %B %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%B %Y %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
        df.dropna(subset = column_name,how='any',inplace= True)
#        df.reset_index(inplace=True)       
        return df
if __name__ == '__main__':

    dc = DataCleaning()

    print(str(dc.get_grams('1kg')))
    print(str(dc.get_grams('1g')))
    print(str(dc.get_grams('1l')))
    print(str(dc.get_grams('1ml')))
    print('l1'.isdigit())
    print(str(dc.get_grams('l1ml')))
