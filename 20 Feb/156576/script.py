import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

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

def fetch_data(db_config, query):
    conn = psycopg2.connect(**db_config)
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()

def generate_heatmap(df):
    df.set_index('age_group', inplace=True)
    
    plt.figure(figsize=(8, 6))
    sns.heatmap(df, annot=True, cmap="YlGnBu", fmt=".2f")
    plt.title("Sales Performance by Age Group")
    plt.xlabel("Performance Metrics")
    plt.ylabel("Age Group")
    plt.tight_layout()
    plt.savefig("sales_heatmap.png")

def main():
    df = fetch_data(db_config, query_sales_by_age_group)
    generate_heatmap(df)

if __name__ == "__main__":
    main()