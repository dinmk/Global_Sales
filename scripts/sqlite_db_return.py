#Author : Dinesh Murugesan (dinesh714@gmail.com)
# This Script is to load staging table to Final table for reporting.
# Also view creation for extreme reporting
import os
import sqlite3

home_dir = os.environ['AIRFLOW_HOME']
path = home_dir + ("/db/airflow.db")
path = path.replace("/","//")
con = sqlite3.connect(path)
cur = con.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS tbl_dw_returns(
        tbl_dw_returns_returned                  varchar,
        tbl_dw_returns_order_id                  varchar PRIMARY KEY,
        tbl_dw_returns_region                    varchar,
        tbl_dw_returns_dw_inserted_by   varchar DEFAULT 'DWH_PROCESS',
        tbl_dw_returns_dw_updated_by    varchar,
        tbl_dw_returns_dw_inserted_time date  DEFAULT CURRENT_TIMESTAMP,
        tbl_dw_returns_dw_updated_time  date)""")


cur.execute("""INSERT INTO tbl_dw_returns(
        tbl_dw_returns_returned              ,     
        tbl_dw_returns_order_id              ,
        tbl_dw_returns_region                       
                                         ) 
        SELECT DISTINCT 
        tbl_stg_returns_flag                      ,
        tbl_stg_returns_order_id                  ,
        tbl_stg_returns_region_name              
        FROM tbl_stg_returns
        WHERE TRUE ON CONFLICT(tbl_dw_returns_order_id) DO UPDATE SET
           tbl_dw_returns_returned = excluded.tbl_dw_returns_returned,
           tbl_dw_returns_region = excluded.tbl_dw_returns_region,
           tbl_dw_returns_dw_updated_by = 'DWH_PROCESS',
           tbl_dw_returns_dw_updated_time = CURRENT_TIMESTAMP;""")

print("The SQLite Table tbl_dw_returns load is done ")
con.commit()


cur.execute("""UPDATE tbl_dw_fact_sales AS t1
               SET tbl_dw_fact_sales_return_flag = (SELECT tbl_dw_returns_returned FROM tbl_dw_returns AS t2 WHERE t2.tbl_dw_returns_order_id = tbl_dw_fact_sales_order_id)
               WHERE (tbl_dw_fact_sales_order_id,tbl_dw_fact_sales_region) in (SELECT tbl_dw_returns_order_id,tbl_dw_returns_region FROM tbl_dw_returns AS t2 WHERE t2.tbl_dw_returns_order_id = tbl_dw_fact_sales_order_id AND t2.tbl_dw_returns_region=tbl_dw_fact_sales_region )""") 

print("The SQLite Table tbl_dw_fact_sales for returns load is done ")
con.commit()
cur.close()
