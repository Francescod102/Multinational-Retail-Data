from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Create instances for each class
db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaning = DataCleaning()


# Retrieve the name of the table that contains user data
tables_list = db_connector.list_db_tables()
print(tables_list)

# Extract and read user data from the database
user_data = data_extractor.read_rds_table(db_connector, "legacy_users")

# Perform the cleaning of user data
user_data = data_cleaning.clean_user_data(user_data)

# Use the method to upload user data in the database
db_connector.upload_to_db(user_data)
