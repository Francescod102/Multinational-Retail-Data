import pandas as pd
import numpy as np
from data_extraction import DataExtractor

class DataCleaning:

    def clean_user_data(self):

        df =  DataExtractor.retrieve_pdf_data(self, "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        #  Remove Null values and duplicates
        df = df.dropna()
        df = df.drop_duplicates() 
        print(df.info())        
        return df
    
    #  Create a method to clean card details
#     def clean_card_data(self, data):
#         pdf_dataframe = data[data['card_number'] != 'NULL']
#         pdf_dataframe = pdf_dataframe[pdf_dataframe['card_number'] != 'card_number']
#         pdf_dataframe = pdf_dataframe[pd.to_numeric(pdf_dataframe['card_number'], errors='coerce').notnull()]
#         return pdf_dataframe 
       
db = DataCleaning()
pdf = db.clean_user_data() 
print(pdf)
# user_data = db.clean_user_data()

df_orders = DataExtractor.read_rds_table('orders_table',engine)
    print(df_orders)