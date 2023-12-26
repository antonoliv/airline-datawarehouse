# FEUP-AID December 2023
# Authors: Fernando, António and Ricardo
# Description: This script executes our Extract, Transform & Load process.

# Requirement: You need the travel.sqlite dataset in the folder above this one.

PATH_DATASET_SQLITE = "travel.sqlite"
PATH_DATAWAREHOUSE_SQLITE = "datawarehouse.sqlite"
PATH_DATAWAREHOUSE_CREATE_SQLITE = "create.sql"

import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from sqlite3 import OperationalError

print("Starting ETL process...")

# SQLite Connection
try:
    sqlite_conn = sqlite3.connect(PATH_DATASET_SQLITE)
    sqlite_cursor = sqlite_conn.cursor()
except sqlite3.Error as e:
    print(f"SQLite connection error: {e}")
    exit()

# Create or connect to Data Warehouse
try:
    datawarehouse_conn = sqlite3.connect(PATH_DATAWAREHOUSE_SQLITE)
    datawarehouse_cursor = datawarehouse_conn.cursor()
except sqlite3.Error as e:
    print(f"Data Warehouse connection error: {e}")
    sqlite_conn.close()
    exit()

# Create Data Warehouse tables
try:
    with open(PATH_DATAWAREHOUSE_CREATE_SQLITE, 'r') as f:
        query = f.read()
        datawarehouse_cursor.executescript(query)
except OperationalError as e:
    print(f"Error executing query in Data Warehouse: {e}")
    datawarehouse_conn.close()
    sqlite_conn.close()
    exit()

# Apply transformations and load data into Data Warehouse
try:
    # TODO: Apply transformations and load data into Data Warehouse
    pass
except OperationalError as e:
    print(f"Error executing query in Data Warehouse: {e}")
    datawarehouse_conn.close()
    sqlite_conn.close()
    exit()

# Close connections
sqlite_conn.close()
datawarehouse_conn.close()

print("ETL process finished.")
