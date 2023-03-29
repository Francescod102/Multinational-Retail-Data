import pandas as pd
import numpy as np


class DataCleaning:

    def clean_user_data(self,users):
        
        # engine = db.init_db_engine()
        
        # replace Null
        users = users.replace("Null", np.nan)
        # print (users.head())

        # replace invalid contries 
        countries = ['United Kingdom', 'United States', 'Germany']
        # print (users.head())
        #  replace GGB to GB
        country_codes = ['GB','US','DE']
        users['country'] = users['country'].apply(lambda x: x if x in countries else np.nan)
        users['country_code'] = users ['country_code'].str.replace('GGB','GB')
    
        # replace invalid country codes to nan
        users['country_code']= users['country_code'].apply(lambda x: x if x in country_codes else np.nan)
        
        # change date to datetime
        users['date_of_birth'] = pd.to_datetime(users['date_of_birth'], infer_datetime_format=True, errors='coerce')
        users['join_date']=pd.to_datetime(users['join_date'],errors='coerce')
        # print(users['join_date'].value_counts())
        # drop duplicate emails
        users.drop_duplicates(subset='email_address', keep='first', inplace=True)
       
        # users.drop.duplicates(subset='email_address',keep='first', inplace=True)

        # drop nulls and index and reset index
        # users.dropna(inplace=True, subset=True)
        users.dropna(inplace=True)
        # print (users.head())
        users.drop('index', axis = 1, inplace=True)
        users.reset_index(drop=True, inplace=True)
        print (users.head())
        return users

        #  Remove Null values and duplicates
        # df = df.dropna()
        # df = df.drop_duplicates() 


        # print(df.info())        
        # return dfx
    
    #  Create a method to clean card details
    def clean_card_data(self,data):
        cleaning = DataCleaning()
        # Convert the list to a DataFrame
        pdf_dataframe = pd.DataFrame(data)
        # Apply data cleaning operation to the Dataframe
        # pdf_dataframe = cleaning.clean_card_data(df)
        pdf_dataframe = pdf_dataframe.replace('NULL', np.nan)
        # pdf_dataframe = data[data['card_number'] != 'NULL']
        pdf_dataframe = pdf_dataframe[pdf_dataframe['card_number'].notnull()]
        pdf_dataframe = pdf_dataframe[pdf_dataframe['card_number'] != 'card_number']
        pdf_dataframe = pdf_dataframe[pd.to_numeric(pdf_dataframe['card_number'], errors='coerce').notnull()]
        
        return pdf_dataframe 
    
    def __init__(self, stores_df):
        self.stores_df = stores_df
        
    def clean_store_data(self):
        # Perform data cleaning operations on the stores_df dataframe
        
        
        # Return cleaned dataframe
        return cleaned_df


  # pdf_dataframe = dbex.retrieve_pdf_data ( "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        # pdf_dataframe = data[data['card_number'] != 'NULL']
# if __name__ == '__main__': 
    # cleaning = DataCleaning()
    # pdf = cleaning.clean_card_data(df)
    # print(pdf.head)

# obj = DataCleaning()
# df_clean = obj.clean_card_data()
# # print(df_clean.head)
# db_connector = DatabaseConnector()
# db_connector.upload_to_db(df_clean)

# DatabaseConnector.upload_to_db(df_clean)

# df_clean = DataExtractor.read_rds_table(engine,'lagacy_users')
# print(df)