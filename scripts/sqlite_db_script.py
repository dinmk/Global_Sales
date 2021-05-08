# Author : Dinesh Murugesan (dinesh714@gmail.com)
# This Script is to create Database for ETL process
#!/usr/bin/env python3
import os
import sqlite3

try:   
        sqliteConnection = sqlite3.connect("/Users/dineshmk/airflow_learning/db/airflow.db")
        cursor = sqliteConnection.cursor()
        print("Database created and Successfully Connected to SQLite")
        sqlite_select_Query = "select sqlite_version();"
        cursor.execute(sqlite_select_Query)
        record = cursor.fetchall()
        print("SQLite Database Version is: ", record)
        cursor.close()

except sqlite3.Error as error:
        print("Error while connecting to sqlite", error)
finally:
        if (sqliteConnection):
         sqliteConnection.close()
        print("The SQLite connection is closed")
