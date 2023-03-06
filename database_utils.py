import yaml
from sqlalchemy import create_engine , inspect


class DatabaseConnector:

    # create a method that will read the creddentials and return a dictionary
    def read_db_creds(self):
        with open('db_creds.yaml', "r") as f:
            data = yaml.safe_load(f)
            return data
        
    # method to initian a return a database engine
    def init_db_engine(self,data):
        data = self.read_db_creds()
       
    #   Use the dictionary to set variable

        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = data["RDS_HOST"]
        USER = data['RDS_USER']
        PASSWORD = data['RDS_PASSWORD']
        DATABASE: data['RDS_DATABASE']
        PORT = data['RDS_PORT']
        
        # create a database engine

        engine = create_engine(f'{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')
        
        return engine

#  create a method to list all the tables in the database
    def list_db_tables(self):
        engine = self.init_db_engine()

        # retrieve information about the table inside the databe
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()

        return table_names()
    
#  create a method to upload the data in the database
    def upload_to_db(self,df):
        
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = "localhost"
        USER = 'postgres'
        PASSWORD = 'yqcjftVD644'
        DATABASE = 'Sales_Data'
        PORT = 5432

    # `Store the data in the databe
        local_engine = create_engine(f'{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')
        df.to_sql('dim_date_times', local_engine, if_exists = "replace")
        

db = DatabaseConnector()
print(db.list_db_tables())

                                    
                            
      
        