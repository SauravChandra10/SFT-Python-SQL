import psycopg2
from tabulate import tabulate

db_config = {
    "dbname": "fashion_db",
    "user": "admin",
    "password": "admin_password",
    "host": "localhost",
    "port": 5432
}

query_sales_by_city_category_month = """
SELECT
    c.city,
    p.category,
    DATE_TRUNC('month', o.order_date) AS order_month,
    SUM(o.quantity) AS total_quantity_sold,
    SUM(o.quantity * p.price) AS total_sales_value,
    (
      SELECT SUM(p2.inventory_count)
      FROM products p2
      WHERE p2.category = p.category
    ) AS current_inventory
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
GROUP BY c.city, p.category, DATE_TRUNC('month', o.order_date);
"""

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
        
        colnames1, rows1 = run_query(conn, query_sales_by_city_category_month)
        print("Sales Data by City, Category, and Month:")
        print(tabulate(rows1, headers=colnames1, tablefmt="psql"))
        
        print("\n")
        
        colnames2, rows2 = run_query(conn, query_sales_by_age_group)
        print("Sales Performance by Age Group:")
        print(tabulate(rows2, headers=colnames2, tablefmt="psql"))
        
    except Exception as e:
        print("Error connecting to the database or executing queries:", e)
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()