# Python script that runs a few queries on the database datawarehouse.sqlite

# Queries:

# 1) For each aircraft, get:
# 1.1) Number of on time flights
# 1.2) Number of late flights
# 1.3) Number of cancelled flights

# 2) Average boarding passes per time unit
# 2.1) Number of boarding passes for each day of the week?

# 3) Arrival airport average revenue
# 3.1) $ for each day of the week, for each arrival airport

# 4) fare_condition seats per airport
# 4.1) Number of First class seats for each arrival airport
# 4.2) Number of Business class seats for each arrival airport
# 4.3) Number of Economy class seats for each arrival airport

# 5) Number of tickets per booking
# 5.1) Avg. tickets, per booking, for each day of the week



# ---------------------------------------- Main Screen ---------------------------------------- #
import sqlite3
import pandas as pd
import openpyxl

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
    print("4 -- Get, for each arrival airport, the number of First, Business, and Economy class seats, for each month and for each day of the week")
    print("5 -- Get the average number of boarding passes per booking for each day of the week")

    # Get user input
    user_input = input("\nEnter the number of the query you want to run: ")

else:
    user_input = str(debug)


# ----------- Query 1 -----------
# Todo: Check if 3rd column is correct
if user_input == "1":
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

    # Execute, store, and print query
    df = pd.read_sql_query(query, conn)
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("------- Query 1 Results -------")
    print(df.to_string(index=False))

    # Export to csv and excel file
    # df.to_csv("query1-results.csv")
    df.to_excel("query1-results.xlsx")

# ----------- Query 2 -----------
# Todo: | Note: Not sure if this is correct. Number seems too low
elif user_input == "2":
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("Running query 2...")

    # Total number of boarding passes for each day of the week
    query1 = '''
    SELECT 
        d.weekday,
        COUNT(*) AS num_boarding_passes
    FROM 
        Boarding_Pass b
    JOIN 
        Date d ON b.sched_departure = d.date_id
    GROUP BY 
        d.weekday
    ORDER BY 
        d.weekday;
    '''

    # Execute, store, and print query
    df1 = pd.read_sql_query(query1, conn)
    # Change all 0's to Monday, 1's to Tuesday, etc.
    df1['weekday'] = df1['weekday'].replace([0, 1, 2, 3, 4, 5, 6], ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("------- Query 2 Results -------")
    print("Total number of boarding passes for each day of the week")
    print(df1.to_string(index=False))

    # Average number of boarding passes for each day of the week
    query2 = '''
    SELECT 
        weekday,
        AVG(num_boarding_passes) AS average_boarding_passes
    FROM (
        SELECT 
            d.weekday AS weekday,
            COUNT(*) AS num_boarding_passes
        FROM 
            Boarding_Pass b
        JOIN 
            Date d ON b.sched_departure = d.date_id
        GROUP BY 
            b.sched_departure
    ) AS counts
    GROUP BY 
        weekday
    ORDER BY 
        weekday;
    '''

    # Execute, store, and print query
    df2 = pd.read_sql_query(query2, conn)
    # Change all 0's to Monday, 1's to Tuesday, etc.
    df2['weekday'] = df2['weekday'].replace([0, 1, 2, 3, 4, 5, 6], ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
    print("--------------------------")
    print("Average number of boarding passes for each day of the week")
    print(df2.to_string(index=False))


    # Export to csv and excel file. Query 1 results are in sheet 1, query 2 results are in sheet 2
    with pd.ExcelWriter('query2-results.xlsx') as writer:
        df1.to_excel(writer, sheet_name='Query 1')
        df2.to_excel(writer, sheet_name='Query 2')

# ----------- Query 3 -----------
# | Note: Not sure if the query is correct, because boarding passes dont have amount
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

    # Export to csv and excel file
    # df.to_csv("query3-results.csv")
    df.to_excel("query3-results.xlsx")

# ----------- Query 4 -----------
elif user_input == "4":
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print("Running query 4... (This query takes a while to run)")

    # Query Version 1: For each month of the year
    query1 = '''
    SELECT 
        b.arr_airport,
        d.month,
        SUM(CASE WHEN a.fare_condition = 'Economy' THEN 1 ELSE 0 END) AS economy_class_seats,
        SUM(CASE WHEN a.fare_condition = 'Business' THEN 1 ELSE 0 END) AS business_class_seats,
        SUM(CASE WHEN a.fare_condition = 'Comfort' THEN 1 ELSE 0 END) AS comfort_class_seats
        
    FROM 
        Boarding_Pass b
    JOIN 
        Aircraft_Seat a ON b.seat = a.seat_no
    JOIN 
        Date d ON b.sched_departure = d.date_id
    GROUP BY 
        d.month, b.arr_airport
    ORDER BY 
        b.arr_airport, d.month;
    '''
    df1 = pd.read_sql_query(query1, conn)
    df1['month'] = df1['month'].replace([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])


    # Query Version 2: For each day of the week
    query2 = '''
    SELECT
        b.arr_airport,
        d.weekday,
        SUM(CASE WHEN a.fare_condition = 'Economy' THEN 1 ELSE 0 END) AS economy_class_seats,
        SUM(CASE WHEN a.fare_condition = 'Business' THEN 1 ELSE 0 END) AS business_class_seats,
        SUM(CASE WHEN a.fare_condition = 'Comfort' THEN 1 ELSE 0 END) AS comfort_class_seats
    FROM
        Boarding_Pass b
    JOIN
        Aircraft_Seat a ON b.seat = a.seat_no
    JOIN
        Date d ON b.sched_departure = d.date_id
    GROUP BY
        d.weekday, b.arr_airport
    ORDER BY
        b.arr_airport, d.weekday;
    '''

    print("Still running......")
    df2 = pd.read_sql_query(query2, conn)
    df2['weekday'] = df2['weekday'].replace([0, 1, 2, 3, 4, 5, 6], ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])

    # Ask user if they want to run query for each month or each day of the week
    print("Do you want to print the query for each month or each day of the week?")
    print("1 -- For each month")
    print("2 -- For each day of the week")
    user_input2 = input("\nEnter the number of the query you want to run: ")
    if user_input2 == "1":
        print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
        print("------- Query 4 Results -------")
        print(df1.to_string(index=False))
    elif user_input2 == "2":
        print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
        print("------- Query 4 Results -------")
        print(df2.to_string(index=False))
    else:
        print("Invalid input")
        exit()


    # Export to csv and excel file. Query 1 results are in sheet 1, query 2 results are in sheet 2
    with pd.ExcelWriter('query4-results.xlsx') as writer:
        df1.to_excel(writer, sheet_name='Query 1')
        df2.to_excel(writer, sheet_name='Query 2')

# ----------- Query 5 -----------
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

    # Export to csv and excel file
    # df.to_csv("query5-results.csv")
    df.to_excel("query5-results.xlsx")

