import pandas as pd
import numpy as np
import re


class DataCleaning:

    def clean_user_data(self,users):
        
        # engine = db.init_db_engine()
        
        # replace Null
        users = users.replace("Null", np.nan)
        # print (users.head())

        # replace invalid contries 
        countries = ['United Kingdom', 'United States', 'Germany']
        # print (users.head())
        #  replace GGB to GB
        country_codes = ['GB','US','DE']
        users['country'] = users['country'].apply(lambda x: x if x in countries else np.nan)
        users['country_code'] = users ['country_code'].str.replace('GGB','GB')
    
        # replace invalid country codes to nan
        users['country_code']= users['country_code'].apply(lambda x: x if x in country_codes else np.nan)
        
        # change date to datetime
        users['date_of_birth'] = pd.to_datetime(users['date_of_birth'], infer_datetime_format=True, errors='coerce')
        users['join_date']=pd.to_datetime(users['join_date'],errors='coerce')
        # print(users['join_date'].value_counts())
        # drop duplicate emails
        users.drop_duplicates(subset='email_address', keep='first', inplace=True)
       
        # users.drop.duplicates(subset='email_address',keep='first', inplace=True)

        # drop nulls and index and reset index
        # users.dropna(inplace=True, subset=True)
        users.dropna(inplace=True)
        # print (users.head())
        users.drop('index', axis = 1, inplace=True)
        users.reset_index(drop=True, inplace=True)
        print (users.head())
        return users

        #  Remove Null values and duplicates
        # df = df.dropna()
        # df = df.drop_duplicates() 


        # print(df.info())        
        # return dfx
    
    #  Create a method to clean card details
    def clean_card_data(self,data):
        cleaning = DataCleaning()
        # Convert the list to a DataFrame
        pdf_dataframe = pd.DataFrame(data)
        # Apply data cleaning operation to the Dataframe
        # pdf_dataframe = cleaning.clean_card_data(df)
        pdf_dataframe = pdf_dataframe.replace('NULL', np.nan)
        # pdf_dataframe = data[data['card_number'] != 'NULL']
        pdf_dataframe = pdf_dataframe[pdf_dataframe['card_number'].notnull()]
        pdf_dataframe = pdf_dataframe[pdf_dataframe['card_number'] != 'card_number']
        pdf_dataframe = pdf_dataframe[pd.to_numeric(pdf_dataframe['card_number'], errors='coerce').notnull()]
        
        return pdf_dataframe 
        
    # Create a method to clean store details
    def clean_store_data(self, df):
        # Perform data cleaning operations on the stores_df dataframe
        """Remove null values and duplicates"""
        df = df.drop("lat", axis = 1)
        df = df.dropna()
        df = df.drop_duplicates()

        """Remove and replace incorrectly typed values"""
        df = df[~ df.locality.str.contains(r"\d")]
        df = df[pd.to_numeric(df["latitude"], errors = "coerce").notnull()]
        df = df[pd.to_numeric(df["staff_numbers"], errors = "coerce").notnull()]
        df["continent"] = df["continent"].replace({"eeEurope": "Europe", "eeAmerica": "America"})

        """Return standardize opening dates"""
        df["opening_date"] = pd.to_datetime(df["opening_date"], infer_datetime_format = True, errors = "coerce")
        
        """Drop original index and reset numeration"""
        df = df.reset_index(drop = True)

        return df
        
    def convert_product_weights(self,df):
 # Define a function to convert a weight string to a numeric representation in Kg
        
        df = str(df).strip().lower()
# If the string ends with "kg", remove the suffix and return as float
        if df.endswith("kg"):
            return float(df[:-2])
# If the string ends with "g", remove the suffix and return as float
        elif df.endswith("g"):
            return float(df[:-1]) / 1000
# If the string ends with "ml", assume it's a liquid and convert 1 ml to 1 g, then remove the suffix and return as float
        elif df.endswith("ml"):
            return float(df[:-2]) / 1000
# If the string ends with "l", assume it's a liquid and convert 1 l to 1000 g, then remove the suffix and return as float
        elif df.endswith("l"):
            return float(df[:-1]) * 1000
# Otherwise, try to extract a number from the string using regular expressions and assume it's in g, then convert to Kg
        else:
            match = re.search(r"[\d\.]+", df)
            if match:
                return float(match.group(0)) / 1000 
    
        
        # Apply the convert_weight function to the "weight" column of the DataFrame
        df["weight"] = df["weight"].apply(df)
        
        # Drop rows with missing weight values
        df = df.dropna(subset=["weight"])
        
        # Return the cleaned DataFrame
        # return df
        
    
    # Create an empty column for the weight unit
        df["unit"] = pd.NA

        # Loop through the rows of the dataframe
        for index, row in df.iterrows():
            weight = str(row["weight"])

            # Check the measure of weight
            if "kg" in weight:
                unit = "kg"
            elif "g" in weight:
                unit = "g"
            elif "ml" in weight:
                unit = "ml"
            elif "oz" in weight:
                unit = "oz"
            else:
                continue

            # Save the unit and weight to their respective columns
            df.at[index, "unit"] = unit
            df.at[index, "weight"] = weight.replace(unit, "")

            # Take a value that may contain a quantity multiplier and evaluate it
            if "x" in weight:
                weight = eval(weight.replace("x", "*").replace(" ", "").replace("g", ""))
            else:
                continue

        df.at[1779, "weight"] = 77
        df = df[pd.to_numeric(df["weight"], errors = "coerce").notnull()]

        # Standardize weight type to numeric and units measure
        df["weight"] = df["weight"].astype(float)
        df.loc[df["unit"] == "g", "weight"] /= 1000
        df.loc[df["unit"] == "ml", "weight"] /= 1000
        df.loc[df["unit"] == "oz", "weight"] /= 35.274

        return df
    def clean_product_data(self, df):
        """
        Cleans the product data by removing null values and duplicates, dropping unnecessary columns,
        adding an index column, renaming the index column, and standardizing opening dates.
        Args:
            df: Pandas DataFrame containing product data.
        Returns:
            df: Pandas DataFrame with cleaned product data.
        """
        # Remove null values and duplicates and drop the first and last column
        df = df.dropna()
        df = df.drop_duplicates()
        df = df.drop(columns = ["unit"])
        df = df.drop(df.columns[0], axis = 1)

        df = df.reset_index().rename(columns = {"index": "id"})

        df["removed"] = df["removed"].replace("Still_avaliable", "Available")

        # Standardize opening dates
        df["date_added"] = pd.to_datetime(df["date_added"], infer_datetime_format = True, errors = "coerce")

        return df


    # def __init__(self, stores_df):
    #     self.stores_df = stores_df  
    #     # Return cleaned dataframe
    #     return cleaned_df


  