DROP TABLE IF EXISTS Date;
DROP TABLE IF EXISTS Aircraft;
DROP TABLE IF EXISTS Airport;
DROP TABLE IF EXISTS Flight;
DROP TABLE IF EXISTS Booking;
DROP TABLE IF EXISTS Ticket;
DROP TABLE IF EXISTS Aircraft_Seat;
DROP TABLE IF EXISTS Flight_DIM;
DROP TABLE IF EXISTS Boarding_Pass;

CREATE TABLE Date (
    date_id INTEGER PRIMARY KEY AUTOINCREMENT, 
    minute INTEGER, 
    hour INTEGER, 
    day INTEGER, 
    weekday INTEGER, 
    week INTEGER, 
    month INTEGER,
    year INTEGER
);

CREATE TABLE Aircraft (
    aircraft_code TEXT PRIMARY KEY,
    model TEXT, 
    range INTEGER
);

CREATE TABLE Airport (
    airport_code TEXT PRIMARY KEY, 
    name TEXT, 
    latitude REAL, 
    longitude REAL,
    city TEXT,
    timezone TEXT
);

CREATE TABLE Flight (
    flight_id INTEGER PRIMARY KEY, 
    flight_no INTEGER,
    status TEXT,
    scheduled_duration REAL,
    actual_duration REAL,
    flight_revenue REAL,
    seat_occupancy REAL,
    aircraft TEXT,
    sched_departure INTEGER,
    sched_arrival INTEGER,
    actual_departure INTEGER,
    actual_arrival INTEGER,
    dep_airport TEXT, 
    arr_airport TEXT,
    FOREIGN KEY (dep_airport) REFERENCES Airport(airport_code),
    FOREIGN KEY (arr_airport) REFERENCES Airport(airport_code),
    FOREIGN KEY (aircraft) REFERENCES Aircraft(aircraft_code),
    FOREIGN KEY (sched_departure) REFERENCES Date(date_id),
    FOREIGN KEY (sched_arrival) REFERENCES Date(date_id),
    FOREIGN KEY (actual_departure) REFERENCES Date(date_id),
    FOREIGN KEY (actual_arrival) REFERENCES Date(date_id)
);

CREATE TABLE Booking (
    book_ref TEXT PRIMARY KEY, 
    total_amount REAL, 
    date INTEGER,
    num_tickets INTEGER,
    FOREIGN KEY (date) REFERENCES Date(date_id)
);

CREATE TABLE Ticket (
    ticket_no TEXT PRIMARY KEY, 
    passenger_id TEXT
);

CREATE TABLE Aircraft_Seat (
    seat_id INTEGER PRIMARY KEY AUTOINCREMENT,
    seat_no TEXT,
    fare_condition TEXT,
    aircraft_code TEXT,
    model TEXT,
    range INTEGER,
    FOREIGN KEY (aircraft_code) REFERENCES Aircraft(aircraft_code)
);

CREATE TABLE Flight_DIM (
    flight_id INTEGER PRIMARY KEY, 
    flight_no INTEGER,
    status TEXT,
    scheduled_duration REAL,
    actual_duration REAL
);

CREATE TABLE Boarding_Pass (
    bpass_id INTEGER PRIMARY KEY AUTOINCREMENT, 
    amount REAL,
    ticket TEXT,
    seat INTEGER,
    boarding_number INTEGER,
    sched_departure INTEGER,
    sched_arrival INTEGER,
    actual_departure INTEGER,
    actual_arrival INTEGER,
    dep_airport TEXT, 
    arr_airport TEXT,
    flight INTEGER,
    aircraft TEXT,
    FOREIGN KEY (dep_airport) REFERENCES Airport(airport_code),
    FOREIGN KEY (arr_airport) REFERENCES Airport(airport_code),
    FOREIGN KEY (aircraft) REFERENCES Aircraft(aircraft_code),
    FOREIGN KEY (sched_departure) REFERENCES Date(date_id),
    FOREIGN KEY (sched_arrival) REFERENCES Date(date_id),
    FOREIGN KEY (actual_departure) REFERENCES Date(date_id),
    FOREIGN KEY (actual_arrival) REFERENCES Date(date_id),
    FOREIGN KEY (ticket) REFERENCES Ticket(ticket_no),
    FOREIGN KEY (seat) REFERENCES Aircraft_Seat(seat_id),
    FOREIGN KEY (flight) REFERENCES Flight_DIM(flight_id)
);
