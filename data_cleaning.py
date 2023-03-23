import pandas as pd
import numpy as np
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

class DataCleaning:

    def clean_user_data(self,):
        users = DataExtractor.read_rds_table("table_name", engine)
        # replace Null 
        users = users.replace("Null", np.nan)

        # replace invalid contries 
        countries = ['United Kingdom', 'United States', 'Germany']
        #  replace GGB to GB
        country_codes = ['GB','US','DE']
        users['country'] = users['country'].apply(lambda x: x if x in countries else np.nan)
        users['country_code'] = users ['country_code'].str.rplace('GGB','GB')
        # replace invalid country codes to nan
        users['join_date']= users['country_code'].apply(lambda x: x if x in country_codes else np.nan)

        # change date to datetime
        users['date_of_birth'] = pd.to_datetime(users['date_of_birth'], infer_datetime_format=True, errors='coerce')
        users['join_date']=pd.to_datetime(users['join_date'],infer_datetime_format=True, errors='coerce')

        # drop duplicate emails
        users.drop.duplicates(subset='email_address',keep='first', inplace=True)

        # drop nulls and index and reset index
        users.dropna(inplace=True, subset=['country'])
        users.drop('index', axis = 1, inplace=True)
        users.reset_index(drop=True, inplace=True)

        return users

        #  Remove Null values and duplicates
        # df = df.dropna()
        # df = df.drop_duplicates() 


        # print(df.info())        
        # return dfx
    
    #  Create a method to clean card details
#     def clean_card_data(self, data):
#         pdf_dataframe = data[data['card_number'] != 'NULL']
#         pdf_dataframe = pdf_dataframe[pdf_dataframe['card_number'] != 'card_number']
#         pdf_dataframe = pdf_dataframe[pd.to_numeric(pdf_dataframe['card_number'], errors='coerce').notnull()]
#         return pdf_dataframe 
#  df =  DataExtractor.retrieve_pdf_data(self, "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
if __name__ == '__main__': 
    cleaning = DataCleaning()
    pdf = cleaning.clean_user_data
    print(pdf)

obj = DataCleaning()
df_clean = obj.clean_user_data()
DatabaseConnector.upload_to_db(df_clean)

# df_clean = DataExtractor.read_rds_table(engine,'lagacy_users')
# print(df)