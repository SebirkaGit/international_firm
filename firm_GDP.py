# ETL script to extract, transform, and load GDP data by country
# Source: IMF via archived Wikipedia page

# Importing required libraries
import pandas as pd                # Import pandas for data manipulation and analysis
import numpy as np                 # Import numpy for numerical operations
import sqlite3                     # Import sqlite3 for database operations
from datetime import datetime      # Import datetime for timestamping logs
from bs4 import BeautifulSoup      # Import BeautifulSoup for parsing HTML
import requests                    # Import requests for making HTTP requests

# ETL Functions

def extract(url, table_attribs):
    '''Extracts the required table from the given URL and returns a dataframe.'''
    response = requests.get(url)  # Get the response object
    if response.status_code != 200:  # Check if the request was successful
        raise Exception(f"Failed to load page: {response.status_code}")
    page = response.text  # Get the HTML content as text
    data = BeautifulSoup(page, 'html.parser')          # Parse the HTML content using BeautifulSoup
    df = pd.DataFrame(columns=table_attribs)           # Create an empty DataFrame with specified columns
    tables = data.find_all('tbody')  
    # for i, table in enumerate(tables):
    #     with open("finding_table.txt", "a", encoding="utf-8") as tbls:
    #         tbls.write(f"Table {i}:\n")
    #         tbls.write(table.prettify())
    #         tbls.write("\n\n")                 # Find all <tbody> elements in the HTML (tables)
    rows = tables[2].find_all('tr')    
    for row in rows:                                   # Iterate through each row in the table
        col = row.find_all('td')                       # Find all columns (cells) in the row
        if len(col) != 0:                              # If the row is not empty
            if col[0].find('a') is not None and '—' not in col[2]:  # If the first cell has a link and GDP is not missing
                data_dict = {                          # Create a dictionary for the row's data
                    "Country": col[0].a.contents[0],   # Extract country name from the link text
                    "GDP_USD_millions": col[2].contents[0]  # Extract GDP value from the third cell
                }
                df1 = pd.DataFrame(data_dict, index=[0])   # Create a single-row DataFrame from the dictionary
                df = pd.concat([df, df1], ignore_index=True)  # Append the row to the main DataFrame
    return df                                           # Return the completed DataFrame

def transform(df):
    '''Converts GDP values to float, transforms from millions to billions (rounded to 2 decimals), and renames column.'''
    GDP_list = df["GDP_USD_millions"].tolist()          # Get the GDP values as a list  
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]  # Remove commas and convert to float
    GDP_list = [np.round(x / 1000, 2) for x in GDP_list]         # Convert millions to billions and round to 2 decimals
    df["GDP_USD_millions"] = GDP_list                   # Replace the original column with the converted values 
    df["GDP_USD_millions"] = df["GDP_USD_millions"].astype(float)  # Ensure the column is of float type
    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})  # Rename the column to reflect new units
    return df                                           # Return the transformed DataFrame

def load_to_csv(df, csv_path):
    '''Saves the dataframe to a CSV file.'''
    df.to_csv(csv_path, index=False)                    # Write the DataFrame to a CSV file without row indices

def load_to_db(df, sql_connection, table_name):
    '''Saves the dataframe to a SQL database table.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)  # Save DataFrame to SQL table, replacing if exists

def run_query(query_statement, sql_connection):
    '''Runs a query on the SQL database and prints the result.'''
    query_output = pd.read_sql(query_statement, sql_connection)  # Execute the query and get results as DataFrame

def log_progress(message):
    '''Logs the progress message with a timestamp.'''
    timestamp_format = '%Y-%b-%d-%H:%M:%S'              # Define the timestamp format
    now = datetime.now()                                # Get the current date and time
    timestamp = now.strftime(timestamp_format)           # Format the timestamp
    with open("./etl_project_log.txt", "a") as f:       # Open the log file in append mode
        f.write(timestamp + ' : ' + message + '\n')     # Write the timestamped message to the log

# Constants
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'  # URL of the data source
table_attribs = ["Country", "GDP_USD_millions"]         # Column names for the DataFrame
db_name = 'World_Economies.db'                          # Name of the SQLite database file
table_name = 'Countries_by_GDP'                         # Name of the table in the database
csv_path = './Countries_by_GDP.csv'                     # Path to save the CSV file

# ETL Pipeline Execution
log_progress('Preliminaries complete. Initiating ETL process')  # Log the start of the ETL process

df = extract(url, table_attribs)                        # Extract the data from the web page into a DataFrame
log_progress('Data extraction complete. Initiating Transformation process')  # Log extraction completion

df = transform(df)                                      # Transform the extracted data
log_progress('Data transformation complete. Initiating loading process')  # Log transformation completion

load_to_csv(df, csv_path)                               # Save the transformed data to a CSV file
log_progress('Data saved to CSV file')                  # Log CSV save completion

sql_connection = sqlite3.connect(db_name)               # Create a connection to the SQLite database
log_progress('SQL Connection initiated.')               # Log database connection

load_to_db(df, sql_connection, table_name)              # Load the data into the database table
log_progress('Data loaded to Database as table. Running the query')  # Log database load completion

query_statement = f"SELECT * FROM {table_name} WHERE GDP_USD_billions >= 100"  # SQL query to select countries with GDP >= 100 billion
run_query(query_statement, sql_connection)              # Run the SQL query and print results

log_progress('Process Complete.')                       # Log the completion of the ETL process

sql_connection.close()                                  # Close the database connection
