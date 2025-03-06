import psycopg2
import logging
from tabulate import tabulate

LOG_FILE = "logs.txt"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DB_CONFIG = {
    "dbname": "ship_assembly_tracking_db",
    "user": "admin",
    "password": "admin_password",
    "host": "localhost",
    "port": "5432"
}

QUERY_1 = """
WITH latest_progress AS (
    SELECT
        ap.ship_id,
        s.name AS ship_name,
        c.name AS component_name,
        ap.status,
        ap.start_time,
        ap.end_time,
        ap.worker_assigned,
        ROW_NUMBER() OVER (PARTITION BY ap.ship_id ORDER BY ap.start_time DESC) AS row_num
    FROM assembly_progress ap
    JOIN ships s ON ap.ship_id = s.ship_id
    JOIN components c ON ap.component_id = c.component_id
)
SELECT ship_name, component_name, status, start_time, end_time, worker_assigned
FROM latest_progress
WHERE row_num = 1;
"""

QUERY_2 = """
WITH assembly_durations AS (
    SELECT
        c.category,
        c.name AS component_name,
        ap.ship_id,
        s.name AS ship_name,
        ap.start_time,
        ap.end_time,
        EXTRACT(EPOCH FROM (ap.end_time - ap.start_time)) / 3600 AS assembly_hours
    FROM assembly_progress ap
    JOIN components c ON ap.component_id = c.component_id
    JOIN ships s ON ap.ship_id = s.ship_id
    WHERE ap.status = 'Completed' AND ap.end_time IS NOT NULL
),
category_avg AS (
    SELECT
        category,
        AVG(assembly_hours) AS avg_time
    FROM assembly_durations
    GROUP BY category
)
SELECT
    ad.ship_name,
    ad.component_name,
    ad.category,
    ad.assembly_hours,
    ca.avg_time,
    CASE 
        WHEN ad.assembly_hours > ca.avg_time * 1.5 THEN 'Bottleneck'
        ELSE 'Normal'
    END AS status
FROM assembly_durations ad
JOIN category_avg ca ON ad.category = ca.category
ORDER BY ad.assembly_hours DESC;
"""

def execute_query(query, conn, query_name):
    with conn.cursor() as cur:
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        logging.info(f"Successfully executed {query_name}")
    return columns, rows

def main():
    try:
        logging.info("Attempting to connect to the database.")
        conn = psycopg2.connect(**DB_CONFIG)
        logging.info("Database connection established.")

        print("\n--- Latest Assembly Progress ---\n")
        columns, rows = execute_query(QUERY_1, conn, "QUERY_1")
        print(tabulate(rows, headers=columns, tablefmt="psql"))

        print("\n--- Assembly Bottlenecks ---\n")
        columns, rows = execute_query(QUERY_2, conn, "QUERY_2")
        print(tabulate(rows, headers=columns, tablefmt="psql"))

    except Exception as e:
        logging.error(f"Error: {e}")
        print("Error:", e)
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()