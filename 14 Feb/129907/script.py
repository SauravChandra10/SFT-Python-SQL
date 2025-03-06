import psycopg2
from tabulate import tabulate

conn_params = {
    "host": "localhost",
    "dbname": "insurance_db",
    "user": "admin",
    "password": "admin_password",
    "port": 5432
}

def run_query(query):
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return rows, columns
    except Exception as e:
        print("Error executing query:", e)
        return [], []
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

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
print("Query 1 Results:")
print(tabulate(rows1, headers=columns1, tablefmt="psql"))

rows2, columns2 = run_query(query2)
print("\nQuery 2 Results:")
print(tabulate(rows2, headers=columns2, tablefmt="psql"))