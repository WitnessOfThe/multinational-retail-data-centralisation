import boto3
import pandas as pd
import json
import requests

class DataExtractor:

    def __init__(self):
        pass

    def read_rds_table(self,engine,table_name):
        with engine.begin() as conn:
            return pd.read_sql_table(table_name, con=conn)

    def retrieve_pdf_data(self,link):
        return pd.concat(tabula.read_pdf(link, pages='all'))

    def API_key(self):
        return  {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

    def retrieve_stores_data(self):
        list_of_frames = []
        store_number   = self.list_number_of_stores()
        for _ in range(store_number):
            api_url_base = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{_}'
            response = requests.get(
                                api_url_base,
                                headers=self.API_key()
                                )
            list_of_frames.append( pd.json_normalize(response.json()))
        return pd.concat(list_of_frames)
                                
    def list_number_of_stores(self):
        api_url_base = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        response = requests.get(
                                api_url_base,
                                headers=self.API_key()
                                )
        return response.json()['number_stores']
    def extract_from_s3(self)