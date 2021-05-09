#Author : Dinesh Murugesan (dinesh714@gmail.com)
# This Script is to load Dimension table 
import os
import sqlite3

home_dir = os.environ['AIRFLOW_HOME']
path = home_dir + ("/db/airflow.db")
path = path.replace("/","//")
con = sqlite3.connect(path)
cur = con.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS tbl_dw_people_dim(
        tbl_dw_people_dim_person_id                  integer primary key,
        tbl_dw_people_dim_person                     varchar,
        tbl_dw_people_dim_region                     varchar unique,
        tbl_dw_people_dim_dw_inserted_by   varchar DEFAULT 'DWH_PROCESS',
        tbl_dw_people_dim_dw_updated_by    varchar,
        tbl_dw_people_dim_dw_inserted_time date  DEFAULT CURRENT_TIMESTAMP,
        tbl_dw_people_dim_dw_updated_time  date)""")


cur.execute("""INSERT INTO tbl_dw_people_dim (
        tbl_dw_people_dim_person ,
        tbl_dw_people_dim_region          )
        SELECT DISTINCT
        tbl_stg_people_person            ,
        tbl_stg_people_region
        FROM tbl_stg_people
        WHERE TRUE ON CONFLICT(tbl_dw_people_dim_region) DO UPDATE SET
           tbl_dw_people_dim_person = excluded.tbl_dw_people_dim_person,
           tbl_dw_people_dim_dw_updated_by = 'DWH_PROCESS',
           tbl_dw_people_dim_dw_updated_time = CURRENT_TIMESTAMP;""")

print("The SQLite Table tbl_dw_people_dim load is done ")
con.commit()
cur.close()
