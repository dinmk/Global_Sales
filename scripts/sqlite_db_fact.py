#Author : Dinesh Murugesan (dinesh714@gmail.com)
# This Script is to load Fact table to Final table for reporting.
import os
import sqlite3
home_dir = os.environ['AIRFLOW_HOME']
path = home_dir + ("/db/airflow.db")
path = path.replace("/","//")
con = sqlite3.connect(path)
cur = con.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS tbl_dw_fact_sales(
        tbl_dw_fact_sales_customer_id            varchar,
        tbl_dw_fact_sales_product_id             varchar,
        tbl_dw_fact_sales_order_id               varchar,
        tbl_dw_fact_sales_order_date             date,
        tbl_dw_fact_sales_ship_date              date,
        tbl_dw_fact_sales_ship_mode              varchar,
        tbl_dw_fact_sales_price                  decimal,
        tbl_dw_fact_sales_quantity               integer,
        tbl_dw_fact_sales_discount               decimal,
        tbl_dw_fact_sales_profit                 decimal,
        tbl_dw_fact_sales_ship_cost              deciaml,
        tbl_dw_fact_sales_order_prio             varchar,
        tbl_dw_fact_sales_market_key             integer,
        tbl_dw_fact_sales_region                 varchar,
        tbl_dw_fact_sales_return_flag            varchar,
        tbl_dw_fact_sales_dw_inserted_by   varchar DEFAULT 'DWH_PROCESS',
        tbl_dw_fact_sales_dw_updated_by    varchar,
        tbl_dw_fact_sales_dw_inserted_time date  DEFAULT CURRENT_TIMESTAMP,
        tbl_dw_fact_sales_dw_updated_time  date,
        CONSTRAINT pk_tbl_dw_fact_sales UNIQUE (tbl_dw_fact_sales_order_id,tbl_dw_fact_sales_product_id)  )""")

cur.execute("""CREATE INDEX IF NOT EXISTS idx_tbl_dw_fact_sales ON tbl_dw_fact_sales (tbl_dw_fact_sales_customer_id)""")

cur.execute("""INSERT INTO tbl_dw_fact_sales(
                tbl_dw_fact_sales_customer_id           ,
                tbl_dw_fact_sales_product_id            ,
                tbl_dw_fact_sales_order_id              ,
                tbl_dw_fact_sales_order_date            ,
                tbl_dw_fact_sales_ship_date             ,
                tbl_dw_fact_sales_ship_mode             ,
                tbl_dw_fact_sales_price                 ,
                tbl_dw_fact_sales_quantity              ,
                tbl_dw_fact_sales_discount              ,
                tbl_dw_fact_sales_profit                ,
                tbl_dw_fact_sales_ship_cost             ,
                tbl_dw_fact_sales_order_prio            ,
                tbl_dw_fact_sales_market_key            ,
                tbl_dw_fact_sales_region                ,
                tbl_dw_fact_sales_return_flag
                                         )
        SELECT * FROM ( SELECT DISTINCT
        tbl_stg_orders_customer_id         ,
        tbl_stg_orders_product_id          ,
        tbl_stg_orders_order_id            ,
        date(tbl_stg_orders_order_date)    ,
        date(tbl_stg_orders_ship_date)     ,
        tbl_stg_orders_ship_mode           ,
        tbl_stg_orders_sales               ,
        tbl_stg_orders_quantity            ,
        tbl_stg_orders_discount            ,
        tbl_stg_orders_profit              ,
        tbl_stg_orders_shipping_cost       ,
        tbl_stg_orders_order_priority      ,
        tbl_dw_market_dim_market_key       ,
        tbl_stg_orders_region              ,
        'No' AS tbl_stg_orders_return_flag
        FROM tbl_stg_orders AS A,tbl_dw_market_dim B
        WHERE ifnull(A.tbl_stg_orders_postal,'NA')  = B.tbl_dw_market_dim_postal
        AND   ifnull(A.tbl_stg_orders_city,'NA')    = B.tbl_dw_market_dim_city
        AND   ifnull(A.tbl_stg_orders_state,'NA')   = B.tbl_dw_market_dim_state
        AND   ifnull(A.tbl_stg_orders_country,'NA') = B.tbl_dw_market_dim_country
        AND   ifnull(A.tbl_stg_orders_region,'NA')  = B.tbl_dw_market_dim_region
        AND   ifnull(A.tbl_stg_orders_market,'NA')  = B.tbl_dw_market_dim_market )
        WHERE TRUE ON CONFLICT(tbl_dw_fact_sales_product_id,tbl_dw_fact_sales_order_id) DO UPDATE SET
           tbl_dw_fact_sales_customer_id = excluded.tbl_dw_fact_sales_customer_id,
           tbl_dw_fact_sales_order_id = excluded.tbl_dw_fact_sales_order_id,
           tbl_dw_fact_sales_order_date = excluded.tbl_dw_fact_sales_order_date,
           tbl_dw_fact_sales_ship_date = excluded.tbl_dw_fact_sales_ship_date,
           tbl_dw_fact_sales_ship_mode = excluded.tbl_dw_fact_sales_ship_mode,
           tbl_dw_fact_sales_price = excluded.tbl_dw_fact_sales_price,
           tbl_dw_fact_sales_quantity = excluded.tbl_dw_fact_sales_quantity,
           tbl_dw_fact_sales_discount = excluded.tbl_dw_fact_sales_discount,
           tbl_dw_fact_sales_profit = excluded.tbl_dw_fact_sales_profit,
           tbl_dw_fact_sales_ship_cost = excluded.tbl_dw_fact_sales_ship_cost,
           tbl_dw_fact_sales_order_prio = excluded.tbl_dw_fact_sales_order_prio,
           tbl_dw_fact_sales_dw_updated_by = 'DWH_PROCESS',
           tbl_dw_fact_sales_dw_updated_time = CURRENT_TIMESTAMP;""")

print("The SQLite Table tbl_dw_fact_sales load is done ")
con.commit()
cur.close()
