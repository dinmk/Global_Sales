#Author : Dinesh Murugesan (dinesh714@gmail.com)
# This Script is to load staging table to Final table for reporting.
# Also view creation for extreme reporting

import sqlite3

con = sqlite3.connect('/Users/dineshmk/airflow_learning/db/airflow.db')
cur = con.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS tbl_dw_date_dim(
        tbl_dw_date_dim_calendar_date      date UNIQUE NOT NULL,
        tbl_dw_date_dim_dayofweek          INT NOT NULL,
        tbl_dw_date_dim_weekday            TEXT NOT NULL,
        tbl_dw_date_dim_quarter            INT NOT NULL,
        tbl_dw_date_dim_year               INT NOT NULL,
        tbl_dw_date_dim_month              INT NOT NULL,
        tbl_dw_date_dim_day                INT NOT NULL,
        tbl_dw_date_dim_dw_inserted_by     varchar DEFAULT 'DWH_PROCESS',
        tbl_dw_date_dim_dw_updated_by      varchar,
        tbl_dw_date_dim_dw_inserted_time   date  DEFAULT CURRENT_TIMESTAMP,
        tbl_dw_date_dim_dw_updated_time    date)""")


cur.execute("""INSERT OR IGNORE INTO tbl_dw_date_dim(
                              tbl_dw_date_dim_calendar_date     ,
                              tbl_dw_date_dim_dayofweek         ,
                              tbl_dw_date_dim_weekday           ,
                              tbl_dw_date_dim_quarter           ,
                              tbl_dw_date_dim_year              ,
                              tbl_dw_date_dim_month             ,
                              tbl_dw_date_dim_day
                                                          )

SELECT *
FROM (
          WITH RECURSIVE dates(d) AS (
                  VALUES('1980-01-01')
                      UNION ALL
                          SELECT date(d, '+1 day')
                              FROM dates
                                  WHERE d < '2039-01-01'
                                    )
            SELECT d AS tbl_dw_date_dim_calendar_date,
                (CAST(strftime('%w', d) AS INT) + 6) % 7 AS tbl_dw_date_dim_dayofweek,
                    CASE
                          (CAST(strftime('%w', d) AS INT) + 6) % 7
                                WHEN 0 THEN 'Monday'
                                      WHEN 1 THEN 'Tuesday'
                                            WHEN 2 THEN 'Wednesday'
                                                  WHEN 3 THEN 'Thursday'
                                                        WHEN 4 THEN 'Friday'
                                                              WHEN 5 THEN 'Saturday'
                                                                    ELSE 'Sunday'
                                                                        END AS tbl_dw_date_dim_weekday,
                                                                            CASE
                                                                                  WHEN CAST(strftime('%m', d) AS INT) BETWEEN 1 AND 3 THEN 1
                                                                                        WHEN CAST(strftime('%m', d) AS INT) BETWEEN 4 AND 6 THEN 2
                                                                                              WHEN CAST(strftime('%m', d) AS INT) BETWEEN 7 AND 9 THEN 3
                                                                                                    ELSE 4
                                                                                                        END AS tbl_dw_date_dim_quarter,
                                                                                                            CAST(strftime('%Y', d) AS INT) AS tbl_dw_date_dim_year,
                                                                                                                CAST(strftime('%m', d) AS INT) AS tbl_dw_date_dim_month,
                                                                                                                    CAST(strftime('%d', d) AS INT) AS tbl_dw_date_dim_day
                                                                                                                      FROM dates
                                                                                                                      );""")

print("The SQLite Table tbl_dw_date_dim load is done ")
con.commit()
cur.close()
