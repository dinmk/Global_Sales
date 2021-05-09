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
cur.execute("""CREATE TABLE IF NOT EXISTS tbl_dw_market_dim(
        tbl_dw_market_dim_market_key         integer PRIMARY KEY,
        tbl_dw_market_dim_person_id          integer ,
        tbl_dw_market_dim_postal             varchar ,
        tbl_dw_market_dim_city               varchar ,
        tbl_dw_market_dim_state              varchar ,
        tbl_dw_market_dim_country            varchar ,
        tbl_dw_market_dim_region             varchar ,
        tbl_dw_market_dim_market             varchar ,
        tbl_dw_market_dim_dw_inserted_by   varchar DEFAULT 'DWH_PROCESS',
        tbl_dw_market_dim_dw_updated_by    varchar,
        tbl_dw_market_dim_dw_inserted_time date  DEFAULT CURRENT_TIMESTAMP,
        tbl_dw_market_dim_dw_updated_time  date,
        CONSTRAINT pk_tbl_dw_market_dim UNIQUE (tbl_dw_market_dim_postal,tbl_dw_market_dim_city,tbl_dw_market_dim_state,tbl_dw_market_dim_country,tbl_dw_market_dim_region,tbl_dw_market_dim_market), 
        FOREIGN KEY(tbl_dw_market_dim_person_id ) REFERENCES tbl_dw_people(tbl_dw_people_person_id))""")


cur.execute("""INSERT INTO tbl_dw_market_dim(
        tbl_dw_market_dim_person_id          ,
        tbl_dw_market_dim_postal             ,
        tbl_dw_market_dim_city               ,     
        tbl_dw_market_dim_state              ,
        tbl_dw_market_dim_country            , 
        tbl_dw_market_dim_region             ,   
        tbl_dw_market_dim_market 
                                         ) 
        SELECT DISTINCT 
        tbl_dw_people_dim_person_id        ,
        CASE WHEN tbl_stg_orders_postal IS NULL THEN 'NA' ELSE tbl_stg_orders_postal END tbl_stg_orders_postal             ,
        CASE WHEN tbl_stg_orders_city IS NULL THEN 'NA' ELSE tbl_stg_orders_city END tbl_stg_orders_city                   ,
        CASE WHEN tbl_stg_orders_state IS NULL THEN 'NA' ELSE tbl_stg_orders_state END tbl_stg_orders_state                ,
        CASE WHEN tbl_stg_orders_country IS NULL THEN 'NA' ELSE tbl_stg_orders_country END tbl_stg_orders_country          ,
        CASE WHEN tbl_stg_orders_region IS NULL THEN 'NA' ELSE tbl_stg_orders_region END tbl_stg_orders_region             ,
        CASE WHEN tbl_stg_orders_market IS NULL THEN 'NA' ELSE tbl_stg_orders_market END tbl_stg_orders_market 
        FROM tbl_stg_orders AS A INNER JOIN tbl_dw_people_dim AS B
        ON A.tbl_stg_orders_region = B.tbl_dw_people_dim_region
        WHERE TRUE ON CONFLICT(tbl_dw_market_dim_postal,tbl_dw_market_dim_city,tbl_dw_market_dim_state,tbl_dw_market_dim_country,tbl_dw_market_dim_region,tbl_dw_market_dim_market) DO UPDATE SET
        tbl_dw_market_dim_dw_updated_by = 'DWH_PROCESS',
        tbl_dw_market_dim_dw_updated_time = CURRENT_TIMESTAMP;""")

print("The SQLite Table tbl_dw_market_dim load is done ")
con.commit()
cur.close()
