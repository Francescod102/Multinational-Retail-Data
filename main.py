# %%
#  Import classes and methods
import sys
sys.path.insert(0, "../Multinational-Retail-Data/scripts")

from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Create instances for each class
db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaning = DataCleaning()

# %%
# Retrieve the name of the table that contains user data
tables_list = db_connector.list_db_tables()
print(tables_list)

# Extract and read user data from the database
user_data = data_extractor.read_rds_table(db_connector, "legacy_users")

# Perform the cleaning of user data
user_data = data_cleaning.clean_user_data(user_data)

# Use the method to upload user data in the database
db_connector.upload_to_db(user_data)

# %%
# Extract and read card details from a PDF document
card_details = data_extractor.retrieve_pdf_data( link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

# Perform the cleaning of card data
card_details = data_cleaning.clean_card_data(card_details)

# Use the method to upload card details in the database
db_connector.upload_to_db(card_details)


# %%

product_details = db_extractor.extract_from_s3("data-handling-public", "product.csv", "product.csv")
