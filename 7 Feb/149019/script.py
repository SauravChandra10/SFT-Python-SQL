import psycopg2
import csv
import logging

logging.basicConfig(filename="logs.txt", level=logging.INFO, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

DB_CONFIG = {
    "dbname": "community_feedback",
    "user": "admin",
    "password": "admin_password",
    "host": "localhost",
    "port": "5432"
}

SQL_QUERY = """
SELECT  
    w.workshop_id,  
    w.title AS workshop_title,  
    w.location,  
    w.date,  
    w.organizer,  
    COALESCE(ROUND(AVG(f.rating)::NUMERIC, 2), 0.00) AS avg_rating,  
    COALESCE(COUNT(f.feedback_id), 0) AS total_feedbacks  
FROM workshops w  
LEFT JOIN feedback f ON w.workshop_id = f.workshop_id  
GROUP BY w.workshop_id  
ORDER BY avg_rating DESC, total_feedbacks DESC;  
"""

OUTPUT_FILE = "output.csv"

def fetch_workshop_feedback():
    try:
        logging.info("Connecting to the database...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        logging.info("Executing SQL query...")
        cur.execute(SQL_QUERY)
        results = cur.fetchall()

        headers = ["Workshop ID", "Title", "Location", "Date", "Organizer", "Avg Rating", "Total Feedbacks"]

        if results:
            logging.info(f"Retrieved {len(results)} records. Writing to {OUTPUT_FILE}...")
            with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow(headers)
                writer.writerows(results)
            logging.info("CSV file saved successfully.")
        else:
            logging.warning("No records found matching the criteria.")

        cur.close()
        conn.close()
        logging.info("Database connection closed.")

    except Exception as e:
        logging.error(f"Error occurred: {e}")

if __name__ == "__main__":
    fetch_workshop_feedback()