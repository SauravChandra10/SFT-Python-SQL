import psycopg2
import logging
import csv

def main():
    logging.basicConfig(
        level=logging.INFO,
        filename='logs.txt',
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    conn_params = {
        "dbname": "pharmacovigilance_db",
        "user": "admin",
        "password": "admin_password",
        "host": "localhost",
        "port": "5432"
    }

    query = """
    SELECT
        d.manufacturer,
        COUNT(r.report_id) AS total_reports,
        SUM(CASE WHEN r.seriousness = 'serious' THEN 1 ELSE 0 END) AS serious_reports,
        AVG(p.age) AS average_age
    FROM reports r
    JOIN drugs d ON r.drug_id = d.drug_id
    JOIN patients p ON r.patient_id = p.patient_id
    GROUP BY d.manufacturer;
    """

    try:
        logging.info("Connecting to the database.")
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        logging.info("Executing query.")
        cur.execute(query)
        rows = cur.fetchall()
        logging.info("Query executed successfully. Fetched %d rows.", len(rows))

        headers = ["Manufacturer", "Total Reports", "Serious Reports", "Average Age"]

        output_file = "output.csv"
        logging.info("Writing results to %s.", output_file)
        with open(output_file, mode='w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            writer.writerows(rows)
        logging.info("Results written to %s successfully.", output_file)

    except Exception as e:
        logging.error("An error occurred: %s", e)

    finally:
        if 'cur' in locals() and cur:
            cur.close()
            logging.info("Cursor closed.")
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == '__main__':
    main()
