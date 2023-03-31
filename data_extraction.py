import pandas as pd
import tabula as t
import requests
import boto3
import requests
from botocore import UNSIGNED
from botocore.client import Config

class DataExtractor:


    def __init__ (self):
        api_key = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
        self.header = {"x-api-key": api_key}
        pass


    #  create a method that extracts and reads user data from the databse
    def read_rds_table(self, table_name, db_connector):
        engine = db_connector.init_db_engine()
        user_data = pd.read_sql_table(table_name, con=engine)
        print('hereeeee')
        return user_data
    
#     # Create a method that extracts and reads card details from a PDF document
    def retrieve_pdf_data(self):
        pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
         # read_pdf returns list of DataFrames
        dfs = t.read_pdf(pdf_link, pages='all')
        df = pd.concat(dfs, ignore_index=True)
        print(df.head())
        # Concatenate the dataframes into a single dataframe
        # df = pd.concat(dfs, ignore_index=True)
        # print(dfs)
        return df
        # for dfs in dfs:
        #     print(len(dfs))
        # pdf_data = t.read_pdf(pdf_link, pages="all", multiple_tables=True)
        # df = pd.concat(pdf_data)
        # print (pdf_link)
        # return df

# #     # Create a method to get that number of stores using an API

    def list_number_of_stores(self):
        api_key ={'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        store = requests.get("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores",headers=api_key)
        number_stores = store.json()
        return number_stores["number_stores"]
    

    def retrieve_stores_data(self):
        api_key ={'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        number_of_stores = self.list_number_of_stores()
        stores_list = []
        for store_number in range(0,number_of_stores):
            stores_list.append(requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'+str(store_number), headers=api_key).json())
        return pd.json_normalize(stores_list)
        

 # Create methods to download and extract product information stored in an S3 bucket on AWS
    def extract_from_s3(self, bucket_name, object_name, file_name):
        s3_client = boto3.client("s3", config = Config(signature_version = UNSIGNED))
        s3_client.download_file(bucket_name, object_name, file_name)
        product_details = pd.read_csv(file_name)
        return product_details


# if __name__ == '__main__': 
#    extractor = DataExtractor()
#    product_details = extractor.extract_from_s3("data-handling-public", "product.csv", "product.csv")