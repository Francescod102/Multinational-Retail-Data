import pandas as pd
import tabula as t
import requests
import boto3
import requests
from botocore import UNSIGNED

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
       
    
    # def list_number_of_store(self):
    #     stores = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',headers=self.api_key)
    #     n_of_stores = stores.json()
    #     return n_of_stores["number_stores"]
        

    # def retrieve_stores_data(self):
    #     n_of_stores = self.list_number_of_store()
    #     stores_list = []
    #     for store_number in range(0, n_of_stores):
    #         stores_list.append(requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'+str(store_number), headers=self.api_key).json())
    #     return pd.json_normalize(stores_list)
    #     print()

    # def retrieve_pdf_data(self, pdf_link):
    #     pdf_link = ("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
    #     # df = pd.read_csv('card_details.csv', index_col = 0, skiprows = 1, on_bad_lines = 'skip')
    #     pdf_data= t.read_pdf(pdf_link, pages = "all" , multiple_tables=True)
    #     df =pd.concat(pdf_data)
    #     # df= pd.DataFrame(pdf_dataframes[0])
        
        # return df
        # return pd.concat( pdf_dataframes, ignore_index= True)
        
    
# #     # Create a method to get that number of stores using an API
    # def list_number_of_stores(self, url, headers):
    #     response = requests.get(url, headers = headers)

    #     # return the number of stores if code is correct
    #     if response.status_code == 200:
    #         data = response.jason()
    #         return data['number_stores']
    #     else:
    #         # return None
    #         print(data)


if __name__ == '__main__': 
#     api_key = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
     extractor = DataExtractor()
     print(extractor.retrieve_stores_data())

    

#     # db = DatabaseConnector()
#     df_orders = extractor.retrieve_pdf_data()
#     print(df_orders)
