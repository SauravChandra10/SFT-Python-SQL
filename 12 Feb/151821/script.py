import psycopg2
from psycopg2.extras import DictCursor
from tabulate import tabulate
import csv
import logging

def main():
    logging.basicConfig(
        filename='logs.txt',
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s'
    )
    
    conn_params = {
        "host": "localhost",
        "database": "products_db",
        "user": "admin",
        "password": "admin_password",
        "port": 5432
    }
    
    try:
        logging.info("Connecting to the database...")
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor(cursor_factory=DictCursor)
        logging.info("Database connection established.")
        
        query1 = """
        SELECT
            a.category,
            COUNT(f.feedback_id) AS total_feedback,
            AVG(f.rating) AS average_rating,
            SUM(CASE WHEN f.rating >= 4 THEN 1 ELSE 0 END) AS positive_feedback,
            SUM(CASE WHEN f.rating <= 2 THEN 1 ELSE 0 END) AS negative_feedback
        FROM feedback f
        JOIN appliances a ON f.appliance_id = a.appliance_id
        JOIN customers c ON f.customer_id = c.customer_id
        GROUP BY a.category
        ORDER BY average_rating DESC;
        """
        logging.info("Executing query 1...")
        cursor.execute(query1)
        results1 = cursor.fetchall()
        headers1 = [desc[0] for desc in cursor.description]
        rows1 = [[row[col] for col in headers1] for row in results1]
        logging.info("Query 1 executed successfully, %d rows retrieved.", len(rows1))
        
        
        with open("output1.csv", "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers1)
            writer.writerows(rows1)
        logging.info("Query 1 results saved to output1.csv.")
        
        query2 = """
        WITH appliance_ratings AS (
            SELECT
                a.appliance_id,
                a.product_name,
                a.category,
                AVG(f.rating) AS avg_rating,
                COUNT(f.feedback_id) AS review_count
            FROM appliances a
            JOIN feedback f ON a.appliance_id = f.appliance_id
            GROUP BY a.appliance_id, a.product_name, a.category
        )
        SELECT
            category,
            appliance_id,
            product_name,
            avg_rating,
            review_count
        FROM (
            SELECT
                category,
                appliance_id,
                product_name,
                avg_rating,
                review_count,
                RANK() OVER (PARTITION BY category ORDER BY avg_rating DESC) AS rank_in_category
            FROM appliance_ratings
        ) ranked
        WHERE rank_in_category = 1;
        """
        logging.info("Executing query 2...")
        cursor.execute(query2)
        results2 = cursor.fetchall()
        headers2 = [desc[0] for desc in cursor.description]
        rows2 = [[row[col] for col in headers2] for row in results2]
        logging.info("Query 2 executed successfully, %d rows retrieved.", len(rows2))
        
        
        with open("output2.csv", "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers2)
            writer.writerows(rows2)
        logging.info("Query 2 results saved to output2.csv.")
    
    except Exception as e:
        logging.error("An error occurred: %s", str(e))
        print("An error occurred:", e)
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
