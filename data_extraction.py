import pandas as pd
import tabula as t
import requests
import boto3
from botocore import UNSIGNED
from database_utils import DatabaseConnector


class DataExtractor:


    def __init__ (self):
        pass


    #  create a method that extracts and reads user data from the databse
    def read_rds_table(self,table_name, engine):
        user_data = pd.read_sql_table(table_name, engine)
        print('hereeeee')
        return user_data
    
#     # Create a method that extracts and reads card details from a PDF document
#     def retrieve_pdf_data(self, pdf_link):
#         # df = pd.read_csv('card_details.csv', index_col = 0, skiprows = 1, on_bad_lines = 'skip')
#         pdf_dataframes = t.read_pdf(pdf_link, pages = "all")
#         #df= pd.DataFrame(pdf_dataframes[0])
#         #return df
#         return pd.concat( pdf_dataframes, ignore_index= True)
        
    
# #     # Create a method to get that number of stores using an API
# #     def list_number_of_stores(self, url, headers):
# #         response = requests.get(url, headers = headers)

# #         # return the number of stores if code is correct
# #         if response.status_code == 200:
# #             data = response.jason()
# #             return data['number_stores']
# #         else:
# #             return None
        
# print()
    # print(extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"))
if __name__ == '__main__': 
    extractor = DataExtractor()
    

    db = DatabaseConnector()
    engine = db.init_db_engine()
    df_orders = extractor.read_rds_table('legacy_users',engine)
    print(df_orders)
