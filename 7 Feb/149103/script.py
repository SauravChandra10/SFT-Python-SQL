import psycopg2
from tabulate import tabulate
import logging

logging.basicConfig(
    filename="logs.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

DB_CONFIG = {
    "dbname": "insurance_actuarial",
    "user": "admin",
    "password": "admin_password",
    "host": "localhost",
    "port": "5432"
}

SQL_QUERY = """
WITH change_diffs AS (
    SELECT
        model_id,
        change_date,
        EXTRACT(EPOCH FROM (change_date - LAG(change_date) OVER (PARTITION BY model_id ORDER BY change_date))) AS diff_seconds
    FROM model_changes
),
model_change_stats AS (
    SELECT 
        model_id,
        COUNT(*) AS total_changes,
        MIN(change_date) AS first_change,
        MAX(change_date) AS last_change,
        COALESCE(AVG(diff_seconds), 0) AS avg_seconds_between_changes
    FROM change_diffs
    GROUP BY model_id
)
SELECT 
    am.model_id,
    am.model_name,
    COALESCE(mcs.total_changes, 0) AS total_changes,
    mcs.first_change,
    mcs.last_change,
    COALESCE(mcs.avg_seconds_between_changes, 0) AS avg_seconds_between_changes
FROM actuarial_models am
LEFT JOIN model_change_stats mcs ON am.model_id = mcs.model_id
"""

def fetch_change_stats():
    try:
        logging.info("Attempting to connect to the database...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        logging.info("Database connection established.")

        logging.info("Executing SQL query to aggregate change statistics...")
        cur.execute(SQL_QUERY)
        results = cur.fetchall()
        logging.info(f"Query executed successfully, retrieved {len(results)} rows.")

        headers = ["Model ID", "Model Name", "Total Changes", "First Change", "Last Change", "Avg Seconds Between Changes"]
        table = tabulate(results, headers=headers, tablefmt="grid")
        print(table)
        logging.info("Results printed in formatted table.")

        cur.close()
        conn.close()
        logging.info("Database connection closed.")

    except Exception as e:
        logging.error(f"Error occurred: {e}")
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_change_stats()
