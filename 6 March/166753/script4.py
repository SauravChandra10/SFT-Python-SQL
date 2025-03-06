import psycopg2
import schedule
import time
import logging
from datetime import datetime

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"logs_{timestamp}.txt"

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

DB_NAME = "player_load_manager_db"
DB_USER = "admin"
DB_PASSWORD = "admin_password"
DB_HOST = "localhost"
DB_PORT = "5432"

def get_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def update_workload():
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        update_query = """
            UPDATE PerformanceStats
            SET workload_score = (minutes_played * 0.5) +
                                 (points_scored * 0.5) +
                                 (rebounds * 0.1) +
                                 (assists * 0.1);
        """
        cursor.execute(update_query)
        conn.commit()
        logging.info("Workload scores updated successfully.")
    except Exception as e:
        logging.error("An error occurred: %s", e)
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    update_workload()

    schedule.every(10).seconds.do(update_workload)
    logging.info("Scheduler started. Press Ctrl+C to exit.")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Scheduler stopped.")