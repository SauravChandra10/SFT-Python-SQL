import psycopg2
import csv
import logging

logging.basicConfig(
    filename='logs.txt',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
        logging.info("Successfully executed query: %s", query.strip())
        return colnames, rows
    except Exception as e:
        logging.error("Error executing query: %s | Error: %s", query.strip(), str(e))
        raise

def save_results_to_csv(filename, data_sections):
    try:
        with open(filename, mode='w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for section_title, colnames, rows in data_sections:
                writer.writerow([section_title])
                writer.writerow(colnames)
                writer.writerows(rows)
                writer.writerow([])
        logging.info("Results successfully saved to CSV file: %s", filename)
    except Exception as e:
        logging.error("Error saving results to CSV: %s", str(e))
        raise

def main():
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        logging.info("Connected to the database successfully.")
        
        colnames1, rows1 = run_query(conn, query_sales_by_city_category_month)
        colnames2, rows2 = run_query(conn, query_sales_by_age_group)
        
        data_sections = [
            ("Sales Data by City, Category, and Month", colnames1, rows1),
            ("Sales Performance by Age Group", colnames2, rows2)
        ]
        save_results_to_csv("output.csv", data_sections)
        
    except Exception as e:
        logging.error("An error occurred in main: %s", str(e))
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()