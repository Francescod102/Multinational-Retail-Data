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
        engine = create_engine(f"postgresql://{data['RDS_USER']}:{data['RDS_PASSWORD']}@{data['RDS_HOST']}:{data['RDS_PORT']}/{data['RDS_DATABASE']}:{data['RDS_PORT']}/")
        engine.connect()
        return engine

#  create a method to list all the tables in the database
    def list_db_tables(self):
        engine = self.init_db_engine()

        engine.connect()

        # retrieve information about the table inside the databe
        inspector = inspect(engine)
        return inspector.get_table_names()

#  create a method to upload the data in the database
    def upload_to_db(self,df, table_name):
        
        # DATABASE_TYPE = 'postgresql'
        # DBAPI = 'psycopg2'
        # HOST = "localhost"
        # USER = 'postgres'
        # PASSWORD = 'yqcjftVD644'
        # DATABASE = 'Sales_Data'
        # PORT = 5432

    # `Store the data in the databe
        local_engine = create_engine('postgresql://admin:admin:adm1n@localhost:5432/Sales_Data')
        df.to_sql('dim_date_times', local_engine, if_exists = "replace")
        
if '__main__' == '__main__':
    db = DatabaseConnector()
    print(db.list_db_tables)

                                    
                            
      
        