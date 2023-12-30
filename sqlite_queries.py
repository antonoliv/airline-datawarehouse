# Python script that runs a few queries on the database datawarehouse.sqlite

# Queries:

# 1) For each aircraft, get:
# 1.1) Number of on time flights
# 1.2) Number of late flights
# 1.3) Number of cancelled flights

# 2) Average boarding passes per time unit
# 2.1) Number of boarding passes for each day of the week? (Edit: Boarding passes dont have a date, so this is not possible??)

# 3) Arrival airport average revenue
# 3.1) $ for each day of the week, for each arrival airport

# 4) fare_condition seats per airport
# 4.1) Number of First class seats for each arrival airport
# 4.2) Number of Business class seats for each arrival airport
# 4.3) Number of Economy class seats for each arrival airport

# 5) Number of Boarding passes per booking (not per ticket)
# 5.1) Avg. boarding passes, per booking, for each day of the week



# ---------------------------------------- Main Screen ---------------------------------------- #
import sqlite3
import pandas as pd
import numpy as np

# Connect to database
conn = sqlite3.connect('datawarehouse.sqlite')
cur = conn.cursor()


# Ask user what query they want to run
# Print results of query

# debug off = 0, else debug = query number
debug = 1

if debug == 0:
    print("Please select one of the following queries to run:")
    print("-----------------------------------------------")
    print("1 -- Get, for each aircraft, get the number of on time flights, late flights, and cancelled flights")
    print("2 -- Get the average number of boarding passes for each day of the week")
    print("3 -- Get the average revenue for each arrival airport")
    print("4 -- Get, for each arrival airport, the number of First, Business, and Economy class seats.")
    print("5 -- Get the average number of boarding passes per booking for each day of the week")

    # Get user input
    user_input = input("\nEnter the number of the query you want to run: ")

else:
    user_input = str(debug)

# Run query based on user input
if user_input == "1": # Query 1 | Todo: Check if 3rd column is correct
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("Running query 1...")

    query = '''
    SELECT 
        aircraft, 
        COUNT(CASE WHEN sched_departure <= actual_departure AND (status = "Arrived" OR status = "On Time") THEN 1 END) AS on_time_flights,
        COUNT(CASE WHEN sched_departure > actual_departure AND (status = "Arrived" OR status = "On Time") THEN 1 END) AS late_flights,
        COUNT(CASE WHEN status = "Cancelled" THEN 1 END) AS cancelled_flights
    FROM Flight
    GROUP BY aircraft
    '''

    # Execute query and store results in a dataframe, then print the dataframe
    df = pd.read_sql_query(query, conn)
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("------- Query 1 Results -------")
    print(df.to_string(index=False))



