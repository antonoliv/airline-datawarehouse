# FEUP-AID December 2023
# Authors: Fernando, António and Ricardo
# Description: This script executes our Extract, Transform & Load process.

# Requirement: You need the travel.sqlite dataset in the folder above this one.

import sqlite3
import pandas as pd
from sqlalchemy import create_engine

# SQLite Connection
try:
    sqlite_conn = sqlite3.connect('../travel.sqlite')
    sqlite_cursor = sqlite_conn.cursor()
except sqlite3.Error as e:
    print(f"SQLite connection error: {e}")
    exit()

# MySQL Connection Settings
mysql_username = 'root'
mysql_password = 'NandoNandoNando'
mysql_hostname = 'localhost'
mysql_port = '3306'
mysql_database = 'feup_aid_proj'

# MySQL Connection
try:
    mysql_connection_str = f"mysql+mysqlconnector://{mysql_username}:{mysql_password}@{mysql_hostname}:{mysql_port}/{mysql_database}"
    mysql_engine = create_engine(mysql_connection_str)
except (OperationalError, ConnectionError) as e:
    print(f"Error connecting to MySQL: {e}")
    sqlite_conn.close()
    exit()

# Extract data from SQLite
try:
    query = "SELECT * FROM `flights`"
    data_from_sqlite = pd.read_sql_query(query, sqlite_conn)
except pd.errors.EmptyDataError:
    print("No data found in the SQLite table.")
    sqlite_conn.close()
    exit()
except OperationalError as e:
    print(f"Error executing query in SQLite: {e}")
    sqlite_conn.close()
    exit()

# Perform transformations if needed
# For example, you might want to clean or modify data here using pandas

# Load data into MySQL
try:
    data_from_sqlite.to_sql('flights', mysql_engine, index=False, if_exists='replace')

except OperationalError as e:
    print(f"Error loading data into MySQL: {e}")
    sqlite_conn.close()
    exit()
else:
    print("Data loaded successfully into MySQL.")

# Close connections
sqlite_conn.close()

print("ETL process finished.")
