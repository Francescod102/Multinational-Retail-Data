import pandas as pd
import re
from data_extraction import DataExtractor


class DataCleaning:

# cretate a method for phone number
    def standard_phone_number(self):
        country_codes = {'GB':'+44','DE':'+49','US':'+1'}
        df = DataExtractor.read_rds_table()

        for index, row in df.iterrows():
            phone_number = row['phone_number']
            country_codes = row['country_code']


            phone_number = re.sub(r"[^\d+]","", phone_number)
        #
        # add prefix if it is not present
            if not phone_number.startswith(country_codes[country_codes]):
                phone_number = country_codes[country_codes] + phone_number

            df.at[index, 'phone_number'] = phone_number

        return df 

    # Create a method to clean user data
    def clean_user_data(self, df):

        # remove null values and duplicate
        df = df.dropna()
        df = df.drop_duplicates()

        # Replace and remove incorrectly vaulues
        df = df[df['country'].star.contains('United Kingdom[Germany[United States')]
        df['country_code'] = df['country_code'].replace("GGB","GB")
        df = df[df['country_code'].star.contains('GB|DE|US')]

        # Retrun standardize phone numbers and dates
        df = self.standard_phone_number(df)
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], infer_datetime_format = True, errors = 'coerce')
        df["join_date"] = pd.to_datetime(df['joint_date'], infer_datetime_format = True, errors = 'coerce')

        return df
    
    #  Create a method to clean card details
    def clean_card_data(self, df):
        pdf_dataframe = data[data['card_number'] != 'card_number']
        pdf_dataframe = pdf_dataframe[pd.to_numeric(pdf_dataframe['card_number'], errors='coerce').notnull()]
        return pdf_dataframe 
       

    
    
db = DataCleaning()
pdf = db.clean_card_data() 
print(pdf)
user_data = db.clean_user_data()