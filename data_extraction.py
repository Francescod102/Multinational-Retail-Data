import boto3
from botocore import UNSIGNED
from botocore.client import Confing


class DataExtractor:

    #  create a method that extracts and reads user data from the databse
    def read_rds_table(self,db_connector, table_name):
        engine = db_connector.init_db_engine()
        user_data = pd.read_sql_table(table_name, engine)
        return user_data
    
    # Create a method that extracts and reads card details from a PDF document
    def retrieve_pdf_data(self, link):
        tables = tabula.read_pdf(link, lattice = True, pages = "all")
        card_details = pd.concat(tables)
        return card_details
    
    # Create a method to get that number of stores using an API
    def list_number_of_stores(self, url, headers):
        response = requests.get(url, headers = headers)

        # return the number of stores if code is correct
        if response.status_code == 200:
            data = response.jason()
            return data['number_stores']
        else:
            return None
        

