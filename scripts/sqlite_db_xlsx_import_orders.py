import os
import sqlite3
from sqlite3 import Error
import pandas as pd

home_dir = os.environ['AIRFLOW_HOME']
DB_FILE_PATH = home_dir + ("/db/airflow.db")
XL_FILE_PATH = home_dir + ("/data/Global_Superstore_Orders_2016.xlsx")


def connect_to_db(db_file):
    """
    Connect to an SQlite database, if db file does not exist it will be created
    :param db_file: absolute or relative path of db file
    :return: sqlite3 connection
    """
    sqlite3_conn = None

    try:
        sqlite3_conn = sqlite3.connect(db_file)
        return sqlite3_conn

    except Error as err:
        print(err)

        if sqlite3_conn is not None:
            sqlite3_conn.close()


def insert_values_to_table(table_name, xl_file):
    """
    Open a csv file with pandas, store its content in a pandas data frame, change the data frame headers to the table
    column names and insert the data to the table
    :param table_name: table name in the database to insert the data into
    :param xl_file: path of the xl file to process
    :return: None
    """

    conn = connect_to_db(DB_FILE_PATH)

    if conn is not None:
        c = conn.cursor()

        # Create table if it is not exist
        c.execute('CREATE TABLE IF NOT EXISTS ' + table_name +
                  '(tbl_stg_orders_row_id           INTEGER,'
                  'tbl_stg_orders_order_id          VARCHAR,'
                  'tbl_stg_orders_order_date        DATE,'
                  'tbl_stg_orders_ship_date         DATE,'
                  'tbl_stg_orders_ship_mode         VARCHAR,'
                  'tbl_stg_orders_customer_id       VARCHAR,'
                  'tbl_stg_orders_customer_name     VARCHAR,'
                  'tbl_stg_orders_segment           VARCHAR,'
                  'tbl_stg_orders_postal            INTEGER,'
                  'tbl_stg_orders_city              VARCHAR,'
                  'tbl_stg_orders_state             VARCHAR,'
                  'tbl_stg_orders_country           VARCHAR,'
                  'tbl_stg_orders_region            VARCHAR,'
                  'tbl_stg_orders_market            VARCHAR,'
                  'tbl_stg_orders_product_id        VARCHAR,'
                  'tbl_stg_orders_category          VARCHAR,'
                  'tbl_stg_orders_sub_category      VARCHAR,'
                  'tbl_stg_orders_product_name      VARCHAR,'
                  'tbl_stg_orders_sales             DECIMAL,'
                  'tbl_stg_orders_quantity          INTEGER,'
                  'tbl_stg_orders_discount          DECIMAL,'
                  'tbl_stg_orders_profit            DECIMAL,'
                  'tbl_stg_orders_shipping_cost     DECIMAL,'
                  'tbl_stg_orders_order_priority    VARCHAR)'
                  )

        df = pd.read_excel(xl_file,sheet_name='Orders')

        df.columns = get_column_names_from_db_table(c, table_name)

        df.to_sql(name=table_name, con=conn, if_exists='append', index=False)

        conn.close()
        print('SQL insert process finished')
    else:
        print('Connection to database failed')


def get_column_names_from_db_table(sql_cursor, table_name):
    """
    Scrape the column names from a database table to a list
    :param sql_cursor: sqlite cursor
    :param table_name: table name to get the column names from
    :return: a list with table column names
    """

    table_column_names = 'PRAGMA table_info(' + table_name + ');'
    sql_cursor.execute(table_column_names)
    table_column_names = sql_cursor.fetchall()

    column_names = list()

    for name in table_column_names:
        column_names.append(name[1])

    return column_names


if __name__ == '__main__':
    insert_values_to_table('tbl_stg_orders', XL_FILE_PATH)
