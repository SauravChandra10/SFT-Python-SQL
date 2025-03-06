import psycopg2
import csv
from datetime import datetime

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
            colnames = [desc for desc in cur.description]
            rows = cur.fetchall()
        return colnames, rows
    except Exception as e:
        print("An error occurred while executing the query:", e)
        return None, None

def main():
    try:
        conn = psycopg2.connect(**connection_params)
        print("Connection to the database was successful.")
    except Exception as e:
        print("Error connecting to the database:", e)
        return

    headers, result = execute_query(query, conn)
    
    if result is not None:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        output_file = f"output_{timestamp}.csv"
        
        try:
            with open(output_file, "w", newline="") as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(headers)
                csvwriter.writerows(result)
            print(f"Results have been saved in {output_file}")
        except Exception as e:
            print("Error writing to CSV file:", e)

    conn.close()

if __name__ == '__main__':
    main()