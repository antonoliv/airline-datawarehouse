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
start_timestamp = pd.Timestamp.now()

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

# Receives date in the format YYYY-MM-DD HH:MM:SS+XX where the +XX is to be ignored (it's always +03)
# Separates the values into ["Minute", "Hour", "Day", "Weekday", "Week", "Month", "Year"]
def parse_date(date_str):
    date_splitted = date_str.split(" ")
    date_part1 = date_splitted[0]
    date_part2 = date_splitted[1]
    date_part1_splitted = date_part1.split("-")
    date_part2_splitted = date_part2.split(":")
    minute = date_part2_splitted[1]
    hour = date_part2_splitted[0]
    day = date_part1_splitted[2]
    weekday = pd.Timestamp(date_part1).weekday() # Monday is 0 and Sunday is 6
    week = pd.Timestamp(date_part1).week
    month = date_part1_splitted[1]
    year = date_part1_splitted[0]
    return [minute, hour, day, weekday, week, month, year]

# Apply transformations and load data into Data Warehouse
try:
    # From aircrafts_data create Aircraft with aircraft_code, model, range
    print("1. Aircrafts")
    query = "SELECT * FROM aircrafts_data"
    aircrafts = pd.read_sql_query(query, sqlite_conn)
    aircrafts = aircrafts[["aircraft_code", "model", "range"]]
    aircrafts = aircrafts.values.tolist()
    datawarehouse_cursor.executemany("INSERT INTO Aircraft (aircraft_code, model, range) VALUES (?, ?, ?)", aircrafts)

    # From seats create Aircraft_Seat with seat_no, fare_condition, aircraft_code
    print("2. Seats")
    query = "SELECT * FROM seats"
    seats = pd.read_sql_query(query, sqlite_conn)
    seats = seats[["seat_no", "fare_conditions", "aircraft_code"]]
    seats = seats.values.tolist()
    datawarehouse_cursor.executemany("INSERT INTO Aircraft_Seat (seat_no, fare_condition, aircraft_code) VALUES (?, ?, ?)", seats)

    # From airports_data create Airport with airport_code, airport_name, city, coordinates, timezone
    print("3. Airports")
    # Transform coordinates into two columns: latitude and longitude - they are in format (XX.XXXXX, YY.YYYYY)
    query = "SELECT * FROM airports_data"
    airports = pd.read_sql_query(query, sqlite_conn)
    airports = airports[["airport_code", "airport_name", "city", "coordinates", "timezone"]]
    airports["latitude"] = airports["coordinates"].apply(lambda x: x.split(",")[0])
    airports["longitude"] = airports["coordinates"].apply(lambda x: x.split(",")[1])
    # remove ( and )
    airports["latitude"] = airports["latitude"].apply(lambda x: x[1:])
    airports["longitude"] = airports["longitude"].apply(lambda x: x[:-1])
    airports = airports[["airport_code", "airport_name", "city", "latitude", "longitude", "timezone"]]
    airports = airports.values.tolist()
    datawarehouse_cursor.executemany("INSERT INTO Airport (airport_code, name, city, latitude, longitude, timezone) VALUES (?, ?, ?, ?, ?, ?)", airports)

    # From bookings create Booking with book_ref, book_date, total_amount
    print("4. Bookings")
    # book_date becomes a unique Date into Date table
    query = "SELECT * FROM bookings"
    bookings = pd.read_sql_query(query, sqlite_conn)
    bookings = bookings[["book_ref", "book_date", "total_amount"]]
    print("Parsing dates...")
    bookings["book_date"] = bookings["book_date"].apply(lambda x: parse_date(x))
    # For each date, insert into Date table and apply the ID to the booking as date
    print("Inserting dates and bookings...")
    # dates = []
    # for index, row in bookings.iterrows():
    #     date = row["book_date"]
    #     # Check if date is already in dates
    #     if date not in dates:
    #         dates.append(date)
    #         # Insert into Date table
    #         datawarehouse_cursor.execute("INSERT INTO Date (minute, hour, day, weekday, week, month, year) VALUES (?, ?, ?, ?, ?, ?, ?)", date)
    #     # Get the ID of the date
    #     datawarehouse_cursor.execute("SELECT date_id FROM Date WHERE minute = ? AND hour = ? AND day = ? AND weekday = ? AND week = ? AND month = ? AND year = ?", date)
    #     date_id = datawarehouse_cursor.fetchone()[0]
    #     # Insert into Booking table
    #     datawarehouse_cursor.execute("INSERT INTO Booking (book_ref, date, amount) VALUES (?, ?, ?)", (row["book_ref"], date_id, row["total_amount"]))
    for index, row in bookings.iterrows():
        date = row["book_date"]
        # Check if date is already in dates
        # Insert into Date table
        datawarehouse_cursor.execute("INSERT INTO Date (minute, hour, day, weekday, week, month, year) VALUES (?, ?, ?, ?, ?, ?, ?)", date)
        # Get the ID of the date
        date_id = datawarehouse_cursor.lastrowid
        # Insert into Booking table
        datawarehouse_cursor.execute("INSERT INTO Booking (book_ref, date, amount) VALUES (?, ?, ?)", (row["book_ref"], date_id, row["total_amount"]))

    print("Committing changes...")
    # Save & commit
    datawarehouse_conn.commit()

except OperationalError as e:
    print(f"Error executing query in Data Warehouse: {e}")
    datawarehouse_conn.close()
    sqlite_conn.close()
    exit()

# Close connections
sqlite_conn.close()
datawarehouse_conn.close()

print("ETL process finished in " + str(pd.Timestamp.now() - start_timestamp))
