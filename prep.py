import sqlite3
import pandas


conn = sqlite3.connect("dataset/travel.sqlite")

cursor = conn.cursor()

cursor.execute("SELECT * FROM tickets")

rows = cursor.fetchall()