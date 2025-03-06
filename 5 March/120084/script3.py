import psycopg2
from tabulate import tabulate
from datetime import datetime
import logging

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
log_filename = f"logs_{timestamp}.txt"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

connection_params = {
    'dbname': 'community_resource_db',
    'user': 'admin',
    'password': 'admin_password',
    'host': 'localhost',
    'port': '5432'
}

query = """
WITH ProgramAllocations AS (
  SELECT
    p.program_id,
    p.program_name,
    r.resource_name,
    a.allocated_quantity,
    a.allocation_date,
    SUM(a.allocated_quantity) OVER (
      PARTITION BY p.program_id
      ORDER BY a.allocation_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
  FROM nonprofit.Programs p
  JOIN nonprofit.Allocations a ON p.program_id = a.program_id
  JOIN nonprofit.Resources r ON a.resource_id = r.resource_id
)
SELECT
  program_id,
  program_name,
  resource_name,
  allocated_quantity,
  allocation_date,
  running_total,
  RANK() OVER (
    PARTITION BY program_id
    ORDER BY running_total DESC
  ) AS allocation_rank
FROM ProgramAllocations;
"""

def execute_query(query, conn):
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            colnames = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        logging.info(f"Query executed successfully. Fetched {len(rows)} rows.")
        return colnames, rows
    except Exception as e:
        logging.error(f"An error occurred while executing the query: {e}")
        return None, None

def main():
    try:
        conn = psycopg2.connect(**connection_params)
        logging.info("Connection to the database was successful.")
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        return

    headers, result = execute_query(query, conn)
    if result is not None:
        logging.info("Printing query results.")
        print("\n--- Program Allocations Analysis ---")
        print(tabulate(result, headers=headers, tablefmt="psql"))
    else:
        logging.warning("No results to display.")

    conn.close()
    logging.info("Database connection closed.")

if __name__ == '__main__':
    logging.info("Script started.")
    main()
    logging.info("Script finished.")