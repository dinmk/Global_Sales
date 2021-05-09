#Author : Dinesh Murugesan (dinesh714@gmail.com)
# This Script is to import CSV/XLSX file into Staging table in Database for ETL process
# Assumption : The input csv file Global_Superstore_Returns_2016.csv & Global_Superstore_Orders_2016.xlsx is in same foler

import os
import sqlite3
import csv

home_dir = os.environ['AIRFLOW_HOME']
path = home_dir + ("/db/airflow.db")
path = path.replace("/","//")
con = sqlite3.connect(path)
cur = con.cursor()


cur.execute("""CREATE TABLE IF NOT EXISTS tbl_stg_returns(tbl_stg_returns_flag varchar,tbl_stg_returns_order_id varchar,tbl_stg_returns_region_name varchar)""")
cur.execute("""DELETE FROM tbl_stg_returns""")
with open('/Users/dineshmk/airflow_learning/data/Global_Superstore_Returns_2016.csv', 'r',encoding='utf-8') as tbl_stg_returns_table:
        dr = csv.DictReader(tbl_stg_returns_table, delimiter=',')
        to_db = [(i['Returned'], i['Order ID'], i["Region"]) for i in dr]

        cur.executemany("INSERT INTO tbl_stg_returns VALUES (?,?,?);", to_db)
        print("The SQLite Table tbl_stg_returns is populated from the input CSV file")
        con.commit()
        cur.close()
