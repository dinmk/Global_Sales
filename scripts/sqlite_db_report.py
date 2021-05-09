# Author : Dinesh Murugesan (dinesh714@gmail.com)
# This Script is for reporting.
import os
import sqlite3

home_dir = os.environ['AIRFLOW_HOME']
path = home_dir + ("/db/airflow.db")
path = path.replace("/","//")
con = sqlite3.connect(path)
cur = con.cursor()

print("1. Sales Over Time")

cur.execute("""select A.tbl_dw_fact_sales_order_date,sum(A.tbl_dw_fact_sales_quantity)
        from tbl_dw_fact_sales A
        where A.tbl_dw_fact_sales_return_flag = 'No'
        group by A.tbl_dw_fact_sales_order_date
        order by A.tbl_dw_fact_sales_order_date desc;""")


rows = cur.fetchall()

for row in rows:
        print(row)

        cur.close()



        cur = con.cursor()

        print("2. Sales Per Product Category last year")

        cur.execute("""select C.tbl_dw_date_dim_year,B.tbl_dw_product_dim_category,sum(tbl_dw_fact_sales_price)
                    from tbl_dw_fact_sales A,tbl_dw_product_dim B,tbl_dw_date_dim C
                    where A.tbl_dw_fact_sales_product_id = B.tbl_dw_product_dim_product_id
                    and A.tbl_dw_fact_sales_return_flag = 'No'
                    and A.tbl_dw_fact_sales_order_date = C.tbl_dw_date_dim_calendar_date
                    and C.tbl_dw_date_dim_year=strftime('%Y', datetime('now'))-1
                    group  by C.tbl_dw_date_dim_year,B.tbl_dw_product_dim_category
                    order by B.tbl_dw_product_dim_category,C.tbl_dw_date_dim_year;""")


        rows = cur.fetchall()

        for row in rows:
                print(row)

                cur.close()


                cur = con.cursor()

                print("3. Top Selling Regions last year?")

                cur.execute("""select tbl_dw_date_dim_year,tbl_dw_market_dim_region,sales,RANK() OVER (ORDER BY  sales desc )
        from  (
		select C.tbl_dw_date_dim_year,B.tbl_dw_market_dim_region,sum(tbl_dw_fact_sales_price)  sales
                    from tbl_dw_fact_sales A,tbl_dw_market_dim B,tbl_dw_date_dim C
                    where A.tbl_dw_fact_sales_market_key = B.tbl_dw_market_dim_market_key
                    and A.tbl_dw_fact_sales_return_flag = 'No'
                    and A.tbl_dw_fact_sales_order_date = C.tbl_dw_date_dim_calendar_date
                    and C.tbl_dw_date_dim_year=strftime('%Y', datetime('now'))-1
                    group  by C.tbl_dw_date_dim_year,B.tbl_dw_market_dim_region );""")


                rows = cur.fetchall()

                for row in rows:
                        print(row)

                        cur.close()



                        cur = con.cursor()

                        print("4. Top Selling States last year?")

                        cur.execute("""
	select tbl_dw_date_dim_year,tbl_dw_market_dim_state,sales,RANK() OVER (ORDER BY  sales desc )
  from  (
		select C.tbl_dw_date_dim_year,B.tbl_dw_market_dim_state,sum(tbl_dw_fact_sales_price)  sales
                    from tbl_dw_fact_sales A,tbl_dw_market_dim B,tbl_dw_date_dim C
                    where A.tbl_dw_fact_sales_market_key = B.tbl_dw_market_dim_market_key
                    and A.tbl_dw_fact_sales_return_flag = 'No'
                    and A.tbl_dw_fact_sales_order_date = C.tbl_dw_date_dim_calendar_date
                    and C.tbl_dw_date_dim_year=strftime('%Y', datetime('now'))-1
                    group  by C.tbl_dw_date_dim_year,B.tbl_dw_market_dim_state );""")


                        rows = cur.fetchall()

                        for row in rows:
                                print(row)

                                cur.close()


cur = con.cursor()

print("5. Month on Month Change in Sales last year?")

cur.execute("""select  B.tbl_dw_date_dim_month,sum(A.tbl_dw_fact_sales_price) sales
from tbl_dw_fact_sales A,tbl_dw_date_dim B
where  A.tbl_dw_fact_sales_order_date = B.tbl_dw_date_dim_calendar_date
and A.tbl_dw_fact_sales_return_flag = 'No'
and B.tbl_dw_date_dim_year=strftime('%Y', datetime('now'))-1
group  by B.tbl_dw_date_dim_month
order by B.tbl_dw_date_dim_month;""")


rows = cur.fetchall()

for row in rows:
        print(row)

        cur.close()



cur = con.cursor()

print("6. Return Rate per State last year?")

cur.execute("""select C.tbl_dw_date_dim_year,B.tbl_dw_market_dim_state,sum(tbl_dw_fact_sales_price)
                    from tbl_dw_fact_sales A,tbl_dw_market_dim B,tbl_dw_date_dim C
                    where A.tbl_dw_fact_sales_market_key= B.tbl_dw_market_dim_market_key
                    and A.tbl_dw_fact_sales_return_flag = 'Yes'
                    and A.tbl_dw_fact_sales_order_date = C.tbl_dw_date_dim_calendar_date
                    and C.tbl_dw_date_dim_year=strftime('%Y', datetime('now'))-8
                    group  by C.tbl_dw_date_dim_year,B.tbl_dw_market_dim_state;""")


rows = cur.fetchall()

for row in rows:
        print(row)

        cur.close()


cur = con.cursor()

print("7. Rank Salesperson by Sales last year?")

cur.execute("""select tbl_dw_date_dim_year,tbl_dw_people_dim_person,sales,RANK() OVER (ORDER BY  sales desc )
  from  (
		select D.tbl_dw_people_dim_person,tbl_dw_date_dim_year,sum(tbl_dw_fact_sales_price)  sales
                    from tbl_dw_fact_sales A,tbl_dw_market_dim B,tbl_dw_date_dim C,tbl_dw_people_dim D
                    where A.tbl_dw_fact_sales_market_key = B.tbl_dw_market_dim_market_key
                    and A.tbl_dw_fact_sales_return_flag = 'No'
                    and A.tbl_dw_fact_sales_order_date = C.tbl_dw_date_dim_calendar_date
                    and C.tbl_dw_date_dim_year=strftime('%Y', datetime('now'))-1
					and B.tbl_dw_market_dim_person_id = D.tbl_dw_people_dim_person_id
                    group  by D.tbl_dw_people_dim_person);""")


rows = cur.fetchall()

for row in rows:
        print(row)

        cur.close()



cur = con.cursor()
