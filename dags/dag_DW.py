# Author : Dinesh Murugesan (dinesh714@gmail.com)
# This Script is to setup Dag for ETL process


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
                  'owner': 'airflow',
                              'depends_on_past': False,
                                            'start_date': datetime(2021, 4, 30),
                                                            'retries': 0
                                                                            }

with DAG(dag_id='DW-ORDERS_RETURNS', default_args=default_args, catchup=False, schedule_interval= '@once') as dag:

        DB_creation = BashOperator(task_id='Sqlite_DB_Setup',bash_command="python3 $AIRFLOW_HOME/scripts/sqlite_db_script.py")
        Csv_Returns_to_db_import = BashOperator(task_id='Sqlite_DB_Import_Returns_from_Source',bash_command="python3 $AIRFLOW_HOME/scripts/sqlite_db_csv_import_returns.py")
        xlsx_orders_to_db_import = BashOperator(task_id='Sqlite_DB_Import_Orders_from_Source',bash_command="python3 $AIRFLOW_HOME/scripts/sqlite_db_xlsx_import_orders.py")
        xlsx_people_to_db_import = BashOperator(task_id='Sqlite_DB_Import_People_from_Source',bash_command="python3 $AIRFLOW_HOME/scripts/sqlite_db_xlsx_import_people.py")
        customer_dim_load = BashOperator(task_id='Customer_Dimension_Load',bash_command="python3 $AIRFLOW_HOME/scripts/sqlite_db_customer_dim.py")
        product_dim_load =  BashOperator(task_id='Product_Dimension_Load',bash_command="python3 $AIRFLOW_HOME/scripts/sqlite_db_product_dim.py")
        date_dim_load =  BashOperator(task_id='Date_Dimension_Load',bash_command="python3 $AIRFLOW_HOME/scripts/sqlite_db_date_dim.py")
        people_dim_load = BashOperator(task_id='Person_Dimension_Load',bash_command="python3 $AIRFLOW_HOME/scripts/sqlite_db_people_dim.py")
        market_dim_load = BashOperator(task_id='Market_Dimension_Load',bash_command="python3 $AIRFLOW_HOME/scripts/sqlite_db_market_dim.py")
        fact_sales_load = BashOperator(task_id='Fact_Sales_Load',bash_command="python3 $AIRFLOW_HOME/scripts/sqlite_db_fact.py")
        return_load     = BashOperator(task_id='Fact_Returns_Load',bash_command="python3 $AIRFLOW_HOME/scripts/sqlite_db_return.py")
        DB_report = BashOperator(task_id='Sqlite_DB_Report',bash_command="python3 $AIRFLOW_HOME/scripts/sqlite_db_report.py")
        DB_creation >> xlsx_orders_to_db_import >> xlsx_people_to_db_import  >> Csv_Returns_to_db_import >> [customer_dim_load,product_dim_load,date_dim_load] >> people_dim_load >> market_dim_load  >> fact_sales_load  >> return_load >> DB_report
