import psycopg2
from tabulate import tabulate

def main():
    conn_params = {
        "host": "localhost",
        "dbname": "fashion_retail_db",
        "user": "admin",
        "password": "admin_password",
        "port": "5432"
    }

    sql_query = """
    SELECT 
        s.supplier_name,
        tp.product_name,
        SUM(sh.quantity) AS total_quantity_shipped,
        MIN(sh.shipment_date) AS first_shipment_date,
        MAX(sh.shipment_date) AS last_shipment_date
    FROM shipments sh
    JOIN textile_products tp ON sh.product_id = tp.product_id
    JOIN suppliers s ON tp.supplier_id = s.supplier_id
    GROUP BY s.supplier_name, tp.product_name
    ORDER BY total_quantity_shipped DESC;
    """
    
    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        print("Connected to the database successfully.")

        cursor.execute(sql_query)
        results = cursor.fetchall()
        
        headers = [desc[0] for desc in cursor.description]
        
        print(tabulate(results, headers=headers, tablefmt="psql"))
    
    except Exception as error:
        print("Error executing query:", error)
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()