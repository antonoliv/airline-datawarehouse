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
    datawarehouse_cursor.executemany("INSERT INTO Aircraft (aircraft_code, model, range) VALUES (?, ?, ?)", aircrafts.values.tolist())

    # From seats create Aircraft_Seat with seat_no, fare_condition, aircraft_code, model, range
    print("2. Seats")
    query = "SELECT * FROM seats"
    seats = pd.read_sql_query(query, sqlite_conn)
    seats = seats[["seat_no", "fare_conditions", "aircraft_code"]]
    # For each seat, get the aircraft info and insert into Aircraft_Seat
    for index, row in seats.iterrows():
        aircraft_code = row["aircraft_code"]
        aircraft_info = aircrafts.loc[aircrafts["aircraft_code"] == aircraft_code].iloc[0]
        model = aircraft_info["model"]
        range_ = aircraft_info["range"]
        datawarehouse_cursor.execute("INSERT INTO Aircraft_Seat (seat_no, fare_condition, aircraft_code, model, range) VALUES (?, ?, ?, ?, ?)", (row["seat_no"], row["fare_conditions"], row["aircraft_code"], model, range_))

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

    # From tickets create Ticket with ticket_no, passenger_id
    print("4. Tickets")
    query = "SELECT * FROM tickets"
    tickets = pd.read_sql_query(query, sqlite_conn)
    tickets = tickets[["ticket_no", "passenger_id"]]
    datawarehouse_cursor.executemany("INSERT INTO Ticket (ticket_no, passenger_id) VALUES (?, ?)", tickets.values.tolist())

    query = "SELECT * FROM tickets"
    tickets = pd.read_sql_query(query, sqlite_conn)
    tickets = tickets[["ticket_no", "passenger_id", "book_ref"]]
    
    print("5. Bookings")
    # book_date becomes a unique Date into Date table
    query = "SELECT * FROM bookings"
    bookings = pd.read_sql_query(query, sqlite_conn)
    bookings = bookings[["book_ref", "book_date", "total_amount"]]
    print("Parsing dates...")
    bookings["book_date"] = bookings["book_date"].apply(lambda x: parse_date(x))
    # For each date, insert into Date table and apply the ID to the booking as date
    print("Inserting bookings and dates...")
    for index, row in bookings.iterrows():
        date = row["book_date"]
        # Insert into Date table
        datawarehouse_cursor.execute("INSERT INTO Date (minute, hour, day, weekday, week, month, year) VALUES (?, ?, ?, ?, ?, ?, ?)", date)
        # Get the ID of the date
        date_id = datawarehouse_cursor.lastrowid
        # Get total number of tickets for this booking ref
        num_tickets = 0 # TODO
        # Insert into Booking table
        datawarehouse_cursor.execute("INSERT INTO Booking (book_ref, date, total_amount, num_tickets) VALUES (?, ?, ?, ?)", (row["book_ref"], date_id, row["total_amount"], num_tickets))

    # From flights create Flight and Flight_DIM
    print("6. Flights")
    query = "SELECT * FROM flights"
    flights = pd.read_sql_query(query, sqlite_conn)
    flights = flights[["flight_id", "flight_no", "scheduled_departure", "scheduled_arrival", "departure_airport", "arrival_airport", "status", "aircraft_code", "actual_departure", "actual_arrival"]]
    # parse dates
    print("Parsing dates...")
    flights["scheduled_departure_parsed"] = flights["scheduled_departure"].apply(lambda x: parse_date(x))
    flights["scheduled_arrival_parsed"] = flights["scheduled_arrival"].apply(lambda x: parse_date(x))
    # parse actual dates if not \\N
    flights["actual_departure_parsed"] = flights["actual_departure"].apply(lambda x: parse_date(x) if x != "\\N" else x)
    flights["actual_arrival_parsed"] = flights["actual_arrival"].apply(lambda x: parse_date(x) if x != "\\N" else x)
    # For each flight, insert into Flight and Flight_DIM
    print("Inserting flights and dates...")
    for index, row in flights.iterrows():
        scheduled_duration = pd.Timestamp(row["scheduled_arrival"]) - pd.Timestamp(row["scheduled_departure"])
        scheduled_duration = scheduled_duration.seconds
        actual_departure = row["actual_departure"]
        actual_arrival = row["actual_arrival"]
        actual_duration = 0
        # Only calculate actual duration if actual_departure and actual_arrival are not null
        if actual_departure != "\\N" and actual_arrival != "\\N":
            actual_duration = pd.Timestamp(actual_arrival) - pd.Timestamp(actual_departure)
            actual_duration = actual_duration.seconds
        datawarehouse_cursor.execute("INSERT INTO Flight_DIM (flight_id, flight_no, status, scheduled_duration, actual_duration) VALUES (?, ?, ?, ?, ?)",
            (row["flight_id"], row["flight_no"], row["status"], scheduled_duration, actual_duration)
        )
        # Insert Dates into Date table and get IDs
        scheduled_departure_date = row["scheduled_departure_parsed"]
        scheduled_arrival_date = row["scheduled_arrival_parsed"]
        actual_departure_date = row["actual_departure_parsed"]
        actual_arrival_date = row["actual_arrival_parsed"]
        # Insert into Date table
        datawarehouse_cursor.execute("INSERT INTO Date (minute, hour, day, weekday, week, month, year) VALUES (?, ?, ?, ?, ?, ?, ?)", scheduled_departure_date)
        scheduled_departure_date_id = datawarehouse_cursor.lastrowid
        datawarehouse_cursor.execute("INSERT INTO Date (minute, hour, day, weekday, week, month, year) VALUES (?, ?, ?, ?, ?, ?, ?)", scheduled_arrival_date)
        scheduled_arrival_date_id = datawarehouse_cursor.lastrowid
        actual_departure_date_id = 0
        actual_arrival_date_id = 0
        # If actual_departure and actual_arrival are not null, insert into Date table and get IDs
        if actual_departure != "\\N" and actual_arrival != "\\N":
            datawarehouse_cursor.execute("INSERT INTO Date (minute, hour, day, weekday, week, month, year) VALUES (?, ?, ?, ?, ?, ?, ?)", actual_departure_date)
            actual_departure_date_id = datawarehouse_cursor.lastrowid
            datawarehouse_cursor.execute("INSERT INTO Date (minute, hour, day, weekday, week, month, year) VALUES (?, ?, ?, ?, ?, ?, ?)", actual_arrival_date)
            actual_arrival_date_id = datawarehouse_cursor.lastrowid
        # Insert into Flight table
        flight_revenue = 0 # TODO
        seat_occupancy = 0 # TODO
        qstr = "INSERT INTO Flight (flight_id, flight_no, status, scheduled_duration, actual_duration, flight_revenue, seat_occupancy"
        qstr += ", aircraft, sched_departure, sched_arrival, actual_departure, actual_arrival, dep_airport, arr_airport"
        qstr += ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        datawarehouse_cursor.execute(qstr, (
            row["flight_id"], row["flight_no"], row["status"], scheduled_duration, actual_duration, flight_revenue, seat_occupancy,
            row["aircraft_code"], scheduled_departure_date_id, scheduled_arrival_date_id, actual_departure_date_id, actual_arrival_date_id,
            row["departure_airport"], row["arrival_airport"]
        ))

    # Create Boarding_Pass
    print("7. Boarding Pass")
    query = "SELECT * FROM boarding_passes"
    boarding_passes = pd.read_sql_query(query, sqlite_conn)
    boarding_passes = boarding_passes[["ticket_no", "flight_id", "boarding_no", "seat_no"]]
    query = "SELECT * FROM flights"
    flights = pd.read_sql_query(query, sqlite_conn)
    flights = flights[["flight_id", "scheduled_departure", "scheduled_arrival", "departure_airport", "arrival_airport", "aircraft_code", "actual_departure", "actual_arrival"]]

    # For each boarding pass, get the ticket info and insert into Boarding_Pass
    print("Inserting boarding passes...")
    for index, row in boarding_passes.iterrows():
        ticket_no = row["ticket_no"]
        ticket_info = tickets.loc[tickets["ticket_no"] == ticket_no].iloc[0]
        book_ref = ticket_info["book_ref"]
        # Get the flight info
        flight_id = row["flight_id"]
        flight_info = flights.loc[flights["flight_id"] == flight_id].iloc[0]
        scheduled_departure_date = flight_info["scheduled_departure_parsed"]
        scheduled_arrival_date = flight_info["scheduled_arrival_parsed"]
        actual_departure_date = flight_info["actual_departure_parsed"]
        actual_arrival_date = flight_info["actual_arrival_parsed"]
        # Insert into Boarding_Pass
        amount = 0 # TODO
        qstr = "INSERT INTO Boarding_Pass (amount, ticket, seat, boarding_number, sched_departure, sched_arrival, actual_departure, actual_arrival, dep_airport, arr_airport, flight, aircraft)"
        qstr += " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        datawarehouse_cursor.execute(qstr, (
            amount, ticket_no, row["seat_no"], row["boarding_no"], scheduled_departure_date, scheduled_arrival_date, actual_departure_date, actual_arrival_date,
            flight_info["departure_airport"], flight_info["arrival_airport"], flight_id, flight_info["aircraft_code"]
        ))

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
