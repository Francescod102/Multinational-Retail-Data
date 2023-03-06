import pandas as pd
import numpy
from data_extraction import DataExtractor



class DataCleaning:

# cretate a method for phone number
    def clean_user_data(self, data, tablename):
        
        if tablename is "orders_table":
            data = data [['date_uuid', 'firt_name', 'last_name','user_uuid', 'card_number','store_code','product_cod']]
            data = data [(data['first_name'].notnull()) | (data['last_name'].notnull()) ]
        elif tablename is " legacy_users":
            data = data [['first_name','last_name','date_of_birth', 'company','email_address', 'country',]]              
        elif tablename is "legacy_store_details":
            data = data[['address','longitude','locality','store_code','staff_number','opening_date','store_typ']]                   
            data.loc[data['continet']== 'eeAmerica','continet'] = "America"
            data.loc[data['continet']== 'eeEuropa','continet'] = 'Europe'
            data = data[(data['continet'] == 'Europa') | (data['continet'] == 'America') | (data['continet'].isna())]
            data = data.fillna("N/A")                                                                          
                     
        return data
    
    #  Create a method to clean card details
    def clean_card_data(self, data):
        pdf_dataframe = data[data['card_number'] != 'NULL']
        pdf_dataframe = pdf_dataframe[pdf_dataframe['card_number'] != 'card_number']
        pdf_dataframe = pdf_dataframe[pd.to_numeric(pdf_dataframe['card_number'], errors='coerce').notnull()]
        return pdf_dataframe 
       

    
    
db = DataCleaning()
pdf = db.clean_user_data() 
print(pdf)
user_data = db.clean_user_data()