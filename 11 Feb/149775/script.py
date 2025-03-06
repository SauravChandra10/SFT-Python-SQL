import psycopg2
import logging
import csv

def main():
    logging.basicConfig(
        filename='logs.txt',
        level=logging.INFO,
        format='%(asctime)s:%(levelname)s:%(message)s'
    )
    
    logging.info("Script started.")
    
    try:
        conn = psycopg2.connect(
            dbname="precious_metals_trade",
            user="admin",
            password="admin_password",
            host="localhost",
            port="5432"
        )
        logging.info("Database connection established.")
    except Exception as e:
        logging.error("Error connecting to the database: %s", e)
        return
    
    try:
        cursor = conn.cursor()

        sql_query = """
        SELECT s.shard_location, m.metal_name,
               COUNT(t.transaction_id) AS transaction_count,
               SUM(t.quantity) AS total_quantity,
               AVG(t.price) AS average_price
        FROM transactions t
        JOIN shards s ON t.shard_id = s.shard_id
        JOIN metals m ON t.metal_id = m.metal_id
        GROUP BY s.shard_location, m.metal_name;
        """
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        logging.info("SQL query executed successfully. Rows fetched: %d", len(rows))
    except Exception as e:
        logging.error("Error executing SQL query: %s", e)
        cursor.close()
        conn.close()
        return

    try:
        with open("output.csv", mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(rows)
        logging.info("Results successfully written to output.csv.")
    except Exception as e:
        logging.error("Error writing results to CSV file: %s", e)
    finally:
        cursor.close()
        conn.close()
        logging.info("Database connection closed.")
        logging.info("Script ended.")

if __name__ == "__main__":
    main()