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
debug = 0

if debug == 0:
    print("Please select one of the following queries to run:")
    print("-----------------------------------------------")
    print("1 -- Get, for each aircraft, get the number of on time flights, late flights, and cancelled flights")
    print("2 -- Get the average number of boarding passes, for each day of the week")
    print("3 -- Get the average revenue for each arrival airport, for each day of the week")
    print("4 -- Get, for each arrival airport, the number of First, Business, and Economy class seats.")
    print("5 -- Get the average number of boarding passes per booking for each day of the week")

    # Get user input
    user_input = input("\nEnter the number of the query you want to run: ")

else:
    user_input = str(debug)


# Query 1 | Todo: Check if 3rd column is correct
if user_input == "1":
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("Running query 1...")

    # Todo: Could use aircraft name instead of aircraft id ?? No need to complicate things imo
    query = '''
    SELECT 
        aircraft, 
        COUNT(CASE WHEN sched_departure <= actual_departure AND (status = "Arrived" OR status = "On Time") THEN 1 END) AS on_time_flights,
        COUNT(CASE WHEN sched_departure > actual_departure AND (status = "Arrived" OR status = "On Time") THEN 1 END) AS late_flights,
        COUNT(CASE WHEN status = "Cancelled" THEN 1 END) AS cancelled_flights
    FROM Flight
    GROUP BY aircraft
    '''

    # Execute, store, and print query
    df = pd.read_sql_query(query, conn)
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("------- Query 1 Results -------")
    print(df.to_string(index=False))

# Todo: Query 2 | Note: How do we get the day of the week from a boarding pass? Boarding passes dont have a date ??
elif user_input == "2":
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("Running query 2...")

    query = ''

    # Execute, store, and print query
    df = pd.read_sql_query(query, conn)
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("------- Query 2 Results -------")
    print(df.to_string(index=False))

# Query 3 | Note: Not sure if the query is correct, because boarding passes dont have amount
elif user_input == "3":
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("Running query 3...")

    query = '''
    SELECT 
        d.weekday,
        b.arr_airport,
        SUM(b.amount) AS total_amount
    FROM 
        Boarding_Pass b
    JOIN 
        Date d ON b.sched_departure = d.date_id
    GROUP BY 
        b.arr_airport, d.weekday
    ORDER BY 
        d.weekday, b.arr_airport;
    '''

    # Execute, store, and print query
    df = pd.read_sql_query(query, conn)
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("------- Query 3 Results -------")
    # Change all 0's to Monday, 1's to Tuesday, etc.
    df['weekday'] = df['weekday'].replace([0,1,2,3,4,5,6],['Mon','Tue','Wed','Thu','Fri','Sat','Sun'])
    print(df.to_string(index=False))

# Todo: Query 4
elif user_input == "4":
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("Running query 4...")


    # Todo | Note: What is the time period? Eg: First class tickets in airport X... ...in the last 2 years? ... average per day of week?
    # Todo: fix this, this is wrong.
    query = '''
    SELECT 
        b.arr_airport,
        SUM(CASE WHEN a.fare_condition = 'First' THEN 1 ELSE 0 END) AS first_class_seats,
        SUM(CASE WHEN a.fare_condition = 'Business' THEN 1 ELSE 0 END) AS business_class_seats,
        SUM(CASE WHEN a.fare_condition = 'Economy' THEN 1 ELSE 0 END) AS economy_class_seats
    FROM 
        Boarding_Pass b
    JOIN 
        Aircraft_Seat a ON b.seat = a.seat_id
    GROUP BY 
        b.arr_airport;
    '''

    # Execute, store, and print query
    df = pd.read_sql_query(query, conn)
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("------- Query 4 Results -------")
    print(df.to_string(index=False))

# Todo: Query 5 | Note: Not sure if the query is correct, because boarding passes dont have amount.
# Also, i think this is the "tickets per booking", and not "boarding passes per booking"
elif user_input == "5":
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("Running query 5...")

    query = '''
    SELECT
        d.weekday,
        AVG(b.num_tickets) AS avg_boarding_passes_per_booking
    FROM
        Booking b
    JOIN
        Date d ON b.date = d.date_id
    GROUP BY
        d.weekday
    ORDER BY
        d.weekday;
    '''

    # Execute, store, and print query
    df = pd.read_sql_query(query, conn)
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("------- Query 5 Results -------")
    # Change all 0's to Monday, 1's to Tuesday, etc.
    df['weekday'] = df['weekday'].replace([0, 1, 2, 3, 4, 5, 6], ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
    print(df.to_string(index=False))