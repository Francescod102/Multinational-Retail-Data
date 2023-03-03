import yaml
from sqlalchemy import create_engine, inspect
import psycopg2

class DatabaseConnector:

    def read_db_creds(self):
        with open('db_creds.yaml', "r") as f:
            data = yaml.safe_load(f)
        return data
    
    def init_db_engine(self):
        data = self.read_db_creds()
       

        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = data["RDS_HOST"]
        USER = data['RDS_USER']
        PASSWORD = data['RDS_PASSWORD']
        DATABASE: data['RDS_DATABASE']
        PORT = data['RDS_PORT']
    
        engine = create_engine(f'{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')
        return engine

#  create a method to list all the tables in the database
    def list_db_tables(self):
        engine = self.init_db_engine()

        engine.connect()

        inspector = inspect(engine)
        table_names = inspector.get_table_names()

        return table_names()
#  create a method to upload the data in the database
    def upload_to_db(self,df, table_name):
        
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = "localhost"
        USER = 'postgres'
        PASSWORD = 'yqcjftVD644'
        DATABASE = 'Sales_Data'
        PORT = 5432

        engine = create_engine(f'{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')
        df.to_sql('dim_date_times', engine, if_exists = "replace")
        

    if __name__ == '__main__':
            db = DatabaseConnector()
            print(db.list_db_tables())

                                    
                            
      
        