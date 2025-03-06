import psycopg2
import csv
import logging

logging.basicConfig(
    filename='logs.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

conn_params = {
    "host": "localhost",
    "dbname": "insurance_db",
    "user": "admin",
    "password": "admin_password",
    "port": 5432
}

def run_query(query):
    try:
        logging.info("Connecting to the database.")
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        logging.info("Executing query:\n%s", query)
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        logging.info("Query executed successfully, fetched %d rows.", len(rows))
        return rows, columns
    except Exception as e:
        logging.error("Error executing query: %s", e)
        return [], []
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")

def write_to_csv(filename, columns, rows):
    try:
        with open(filename, mode='w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)
            writer.writerows(rows)
        logging.info("Results saved to %s successfully.", filename)
    except Exception as e:
        logging.error("Error writing to CSV file %s: %s", filename, e)

query1 = """
WITH maint AS (
  SELECT 
    aircraft_id,
    COUNT(*) AS maintenance_count,
    AVG(maintenance_cost) AS avg_maintenance_cost
  FROM aircraft_maintenance
  GROUP BY aircraft_id
),
comp AS (
  SELECT 
    aircraft_id,
    COUNT(*) AS component_count
  FROM component_lifecycle
  GROUP BY aircraft_id
)
SELECT 
  COALESCE(m.aircraft_id, c.aircraft_id) AS aircraft_id,
  COALESCE(m.maintenance_count, 0) AS maintenance_count,
  m.avg_maintenance_cost,
  COALESCE(c.component_count, 0) AS component_count
FROM maint m
FULL OUTER JOIN comp c 
  ON m.aircraft_id = c.aircraft_id;
"""

query2 = """
WITH maint AS (
  SELECT 
    aircraft_id,
    SUM(downtime_hours) AS total_downtime
  FROM aircraft_maintenance
  GROUP BY aircraft_id
),
comp AS (
  SELECT 
    aircraft_id,
    AVG(replacement_cost) AS avg_replacement_cost,
    AVG(expected_lifetime_hours) AS avg_lifetime_hours
  FROM component_lifecycle
  GROUP BY aircraft_id
),
reliab AS (
  SELECT 
    aircraft_id,
    AVG(measured_value) AS avg_measured_value
  FROM system_reliability
  GROUP BY aircraft_id
)
SELECT 
  COALESCE(m.aircraft_id, c.aircraft_id, r.aircraft_id) AS aircraft_id,
  m.total_downtime,
  c.avg_replacement_cost,
  r.avg_measured_value,
  c.avg_lifetime_hours
FROM maint m
FULL OUTER JOIN comp c 
  ON m.aircraft_id = c.aircraft_id
FULL OUTER JOIN reliab r 
  ON COALESCE(m.aircraft_id, c.aircraft_id) = r.aircraft_id;
"""

rows1, columns1 = run_query(query1)
if rows1 or columns1:
    write_to_csv('output1.csv', columns1, rows1)
else:
    logging.warning("No results for Query 1 to write to CSV.")

rows2, columns2 = run_query(query2)
if rows2 or columns2:
    write_to_csv('output2.csv', columns2, rows2)
else:
    logging.warning("No results for Query 2 to write to CSV.")
