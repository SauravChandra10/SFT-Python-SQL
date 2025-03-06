import psycopg2
import csv
import logging

def main():
    logging.basicConfig(
        level=logging.INFO,
        filename="logs.txt",
        filemode="a",
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    conn_params = {
        "dbname": "industrial_design_platform",
        "user": "admin",
        "password": "admin_password",
        "host": "localhost",
        "port": "5432"
    }
    
    try:
        logging.info("Connecting to the database.")
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        query = """
        SELECT
            u.email,
            u.full_name,
            COUNT(s.session_id) AS total_sessions,
            MAX(s.created_at) AS last_login,
            c.last_updated AS credential_last_updated
        FROM users u
        LEFT JOIN sessions s ON u.user_id = s.user_id
        LEFT JOIN credentials c ON u.user_id = c.user_id
        GROUP BY u.user_id, u.email, u.full_name, c.last_updated;
        """

        logging.info("Executing query.")
        cur.execute(query)
        rows = cur.fetchall()

        headers = ['Email', 'Full Name', 'Total Sessions', 'Last Login', 'Credential Last Updated']
        output_file = "output.csv"
        
        with open(output_file, mode="w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            writer.writerows(rows)
        
        logging.info("Query executed successfully and results saved to %s.", output_file)

    except Exception as e:
        logging.error("An error occurred: %s", e)
    finally:
        try:
            if cur:
                cur.close()
                logging.info("Cursor closed.")
        except NameError:
            logging.warning("Cursor was not created.")
        try:
            if conn:
                conn.close()
                logging.info("Database connection closed.")
        except NameError:
            logging.warning("Connection was not created.")

if __name__ == '__main__':
    main()