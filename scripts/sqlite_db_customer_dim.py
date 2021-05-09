#Author : Dinesh Murugesan (dinesh714@gmail.com)
# This Script is to load Dimension table
import os
import sqlite3

home_dir = os.environ['AIRFLOW_HOME']
path = home_dir + ("/db/airflow.db")
path = path.replace("/","//")
con = sqlite3.connect(path)
cur = con.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS tbl_dw_customer_dim(
        tbl_dw_customer_dim_customer_id      varchar PRIMARY KEY,
        tbl_dw_customer_dim_segment          varchar,
        tbl_dw_customer_dim_customer_name    varchar,
        tbl_dw_customer_dim_dw_inserted_by   varchar DEFAULT 'DWH_PROCESS',
        tbl_dw_customer_dim_dw_updated_by    varchar,
        tbl_dw_customer_dim_dw_inserted_time date  DEFAULT CURRENT_TIMESTAMP,
        tbl_dw_customer_dim_dw_updated_time  date)""")


cur.execute("""INSERT INTO tbl_dw_customer_dim(
        tbl_dw_customer_dim_customer_id   ,
        tbl_dw_customer_dim_segment       ,
        tbl_dw_customer_dim_customer_name
                                         )
        SELECT DISTINCT tbl_stg_orders_customer_id,
        tbl_stg_orders_segment           ,
        tbl_stg_orders_customer_name
        FROM tbl_stg_orders
        WHERE TRUE ON CONFLICT(tbl_dw_customer_dim_customer_id) DO UPDATE SET
           tbl_dw_customer_dim_segment =  excluded.tbl_dw_customer_dim_segment ,
           tbl_dw_customer_dim_customer_name =  excluded.tbl_dw_customer_dim_customer_name ,
           tbl_dw_customer_dim_dw_updated_by = 'DWH_PROCESS',
           tbl_dw_customer_dim_dw_updated_time = CURRENT_TIMESTAMP;""")

print("The SQLite Table tbl_dw_customer_dim load is done ")
con.commit()
cur.close()
