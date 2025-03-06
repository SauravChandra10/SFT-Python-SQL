import psycopg2
import pandas as pd
import logging

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("logs.txt", mode="w"),
            logging.StreamHandler()
        ]
    )
    logging.info("Starting the query script.")

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
        logging.info("Connected to the database successfully.")

        cursor = conn.cursor()
        cursor.execute(sql_query)
        logging.info("SQL query executed successfully.")

        results = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        logging.info("Retrieved %d rows.", len(results))

        df = pd.DataFrame(results, columns=headers)
        output_csv = "output.csv"
        df.to_csv(output_csv, index=False)
        logging.info("Results saved to CSV file: %s", output_csv)

    except Exception as error:
        logging.error("Error executing query: %s", error)

    finally:
        if cursor:
            cursor.close()
            logging.info("Cursor closed.")
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()