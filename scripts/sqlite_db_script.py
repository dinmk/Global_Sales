# Author : Dinesh Murugesan (dinesh714@gmail.com)
# This Script is to create Database for ETL process
import os
import sqlite3

try:
        home_dir = os.environ['AIRFLOW_HOME']
        path = home_dir + ("/db/airflow.db")
        path = path.replace("/","//")
        sqliteConnection = sqlite3.connect(path)
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
