CREATE TABLE Date (
    date_id INTEGER PRIMARY KEY AUTOINCREMENT, 
    minute INTEGER, 
    hour INTEGER, 
    day INTEGER, 
    weekday TEXT, 
    week INT, 
    month TEXT, 
    year INT,
    date_value DATE UNIQUE
);

CREATE TABLE Aircraft (
    aircraft_code TEXT PRIMARY KEY, 
    model TEXT, 
    range INTEGER
);

CREATE TABLE Location (
    location_id INTEGER PRIMARY KEY AUTOINCREMENT, 
    latitude INTEGER, 
    longitude INTEGER,
    city TEXT,
    timezone TEXT
);

CREATE TABLE Airport (
    airport_code TEXT PRIMARY KEY, 
    name TEXT, 
    location INTEGER UNIQUE,
    FOREIGN KEY (location) REFERENCES Location(location_id)
);

CREATE TABLE Route (
    route_no INTEGER PRIMARY KEY, 
    dep_airport TEXT, 
    arr_airport TEXT,
    FOREIGN KEY (dep_airport) REFERENCES Airport(airport_code),
    FOREIGN KEY (arr_airport) REFERENCES Airport(airport_code)
);

CREATE TABLE Flight (
    flight_id INTEGER PRIMARY KEY, 
    route TEXT, 
    aircraft TEXT,
    sched_departure INTEGER,
    sched_arrival INTEGER,
    actual_departure INTEGER,
    actual_arrival INTEGER,
    FOREIGN KEY (route) REFERENCES Route(route_no),
    FOREIGN KEY (aircraft) REFERENCES Aircraft(aircraft_code),
    FOREIGN KEY (sched_departure) REFERENCES Date(date_id),
    FOREIGN KEY (sched_arrival) REFERENCES Date(date_id),
    FOREIGN KEY (actual_departure) REFERENCES Date(date_id),
    FOREIGN KEY (actual_arrival) REFERENCES Date(date_id)
);

CREATE TABLE Booking (
    booking_ref TEXT PRIMARY KEY, 
    amount REAL, 
    date TEXT,
    FOREIGN KEY (date) REFERENCES Date(date_id)
);

CREATE TABLE Ticket (
    ticket_no TEXT PRIMARY KEY, 
    passenger_id TEXT, 
    booking TEXT,
    FOREIGN KEY (booking) REFERENCES Booking(booking_ref)
);


CREATE TABLE Seat (
    seat_id INTEGER PRIMARY KEY AUTOINCREMENT,
    seat_no TEXT,
    fare_condition TEXT,
    aircraft TEXT,
    FOREIGN KEY (aircraft) REFERENCES Aircraft(aircraft_code)
);

CREATE TABLE Boarding_Pass (
    ticket TEXT PRIMARY KEY,
    seat INTEGER,
    boarding_number INTEGER,
    FOREIGN KEY (ticket) REFERENCES Ticket(ticket_no),
    FOREIGN KEY (seat) REFERENCES Seat(seat_id)
);


CREATE TABLE Flight_Ticket (
    ticket TEXT,
    flight INTEGER,
    amount REAL,
    fare_condition TEXT,
    PRIMARY KEY (ticket, flight),
    FOREIGN KEY (ticket) REFERENCES Ticket(ticket_no),
    FOREIGN KEY (flight) REFERENCES Flight(flight_id)
);






