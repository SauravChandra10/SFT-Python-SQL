import psycopg2  
from tabulate import tabulate  
  
db_config = {  
    "dbname": "fashion_db",  
    "user": "admin",  
    "password": "admin_password",  
    "host": "localhost",  
    "port": 5432  
}
  
query_sales_by_age_group = """  
SELECT  
  CASE  
    WHEN c.age < 26 THEN 'GenZ'  
    WHEN c.age BETWEEN 26 AND 50 THEN 'Millennials'  
    ELSE 'Rest'  
  END AS age_group,  
  COUNT(o.order_id) AS total_orders,  
  SUM(o.quantity) AS total_quantity_sold,  
  SUM(o.quantity * p.price) AS total_sales_value,  
  ROUND(AVG(o.quantity * p.price), 2) AS avg_order_value  
FROM customers c  
JOIN orders o ON c.customer_id = o.customer_id  
JOIN products p ON o.product_id = p.product_id  
GROUP BY age_group;  
"""  
  
def run_query(conn, query):  
    with conn.cursor() as cur:  
        cur.execute(query)  
        rows = cur.fetchall()  
        colnames = [desc[0] for desc in cur.description]  
    return colnames, rows  
  
def main():  
    conn = None  
    try:  
        conn = psycopg2.connect(**db_config)  
        print("Connected to the database successfully.\n")  

        colnames, rows = run_query(conn, query_sales_by_age_group)  
        print("Sales Performance by Age Group:")  
        print(tabulate(rows, headers=colnames, tablefmt="psql"))  
        
    except Exception as e:  
        print("Error connecting to the database or executing queries:", e)  
    finally:  
        if conn:  
            conn.close()  
            print("\nDatabase connection closed.")  
  
if __name__ == "__main__":  
    main()  