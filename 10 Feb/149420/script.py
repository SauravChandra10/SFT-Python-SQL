import psycopg2
import logging
import csv

logging.basicConfig(
    filename="logs.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    logging.info("Script started.")

    conn_params = {
        "host": "localhost",
        "database": "avionicsdiagnosticdb",
        "user": "admin",
        "password": "admin_password"
    }
    
    connection = None
    cursor = None

    try:
        logging.info("Connecting to the database...")
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()
        logging.info("Database connection established.")

        sql_query = """
        SELECT 
            dl.system_component,
            ca.name AS algorithm_name,
            COUNT(*) AS num_compressions,
            SUM(ch.original_size) AS total_original_size,
            SUM(ch.compressed_size) AS total_compressed_size,
            AVG(1.0 * ch.compressed_size / ch.original_size) AS avg_compression_ratio
        FROM diagnostic_logs dl
        JOIN compression_history ch ON dl.log_id = ch.log_id
        JOIN compression_algorithms ca ON ch.algorithm_id = ca.algorithm_id
        GROUP BY dl.system_component, ca.name;
        """

        logging.info("Executing SQL query.")
        cursor.execute(sql_query)
        results = cursor.fetchall()
        logging.info(f"Query executed successfully. Rows fetched: {len(results)}")

        headers = [
            "System Component",
            "Compression Algorithm",
            "Num Compressions",
            "Total Original Size",
            "Total Compressed Size",
            "Avg Compression Ratio"
        ]

        csv_file = "output.csv"
        logging.info(f"Writing results to CSV file '{csv_file}'.")
        with open(csv_file, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(results)
        logging.info("CSV file written successfully.")

    except Exception as e:
        logging.error("An error occurred while processing the query.", exc_info=True)

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            logging.info("Database connection closed.")
        logging.info("Script finished.")

if __name__ == "__main__":
    main()