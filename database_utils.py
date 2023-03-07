import yaml
from sqlalchemy import create_engine , inspect 
import psycopg2
import config


class DatabaseConnector:

    # create a method that will read the creddentials and return a dictionary
    def read_db_creds(self):
        with open('db_creds.yaml', "r") as f:
            data = yaml.safe_load(f)
            return data
        
    # method to initian a return a database engine
    def init_db_engine(self):
        data = self.read_db_creds()
       
    #    use the dictionary to set variables
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = data['RDS_HOST']
        USER = data['RDS_USER'] 
        PASSWORD = data['RDS_PASSWORD'] 
        DATABASE = data['RDS_DATABASE']
        PORT = data['RDS_PORT']
    
    # create a database engine
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine
    
#  create a method to list all the tables in the database step4
    def list_db_tables(self):
        engine = self.init_db_engine()
        # retrieve information about the table inside the databe
        inspector = inspect(engine)
        table_name = inspector.get_table_names()
        
        return table_name
    
#  create a method to upload the data in the database step7
    def upload_to_db(self,df,table_name):
        
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = "localhost"
        USER = 'postgres'
        DATABASE = 'Sales_Data'
        PORT = 5432

    # `Store the data in the databe step 8
        local_engine = create_engine('postgresql://admin:admin:adm1n@localhost:5432/Sales_Data')
        df.to_sql('dim_date_times', local_engine, if_exists = "replace")
        


                                    
                            
      
        