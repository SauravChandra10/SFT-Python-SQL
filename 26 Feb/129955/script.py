import psycopg2
from tabulate import tabulate

conn_params = {
    "dbname": "irrigation_system",
    "user": "admin",
    "password": "admin_password",
    "host": "localhost",
    "port": "5432"
}

query1 = """
WITH avg_moisture AS (
    SELECT 
        site_id, 
        AVG(moisture) AS avg_moisture
    FROM sensor_data
    GROUP BY site_id
)
SELECT 
    fs.site_id,
    fs.site_name,
    am.avg_moisture,
    ir.schedule_id,
    ir.scheduled_time,
    ir.duration,
    ir.water_volume,
    ir.status
FROM farming_sites fs
JOIN avg_moisture am ON fs.site_id = am.site_id
JOIN irrigation_schedule ir ON fs.site_id = ir.site_id
WHERE am.avg_moisture < 30.0
ORDER BY am.avg_moisture ASC;
"""

query2 = """
WITH daily_sensor AS (
    SELECT
        site_id,
        DATE(measurement_timestamp) AS measurement_date,
        AVG(moisture) AS avg_moisture,
        AVG(temperature) AS avg_temperature
    FROM sensor_data
    GROUP BY site_id, DATE(measurement_timestamp)
),
daily_irrigation AS (
    SELECT
        site_id,
        DATE(scheduled_time) AS irrigation_date,
        COUNT(*) AS irrigation_count,
        SUM(water_volume) AS total_water_volume
    FROM irrigation_schedule
    GROUP BY site_id, DATE(scheduled_time)
)
SELECT
    fs.site_id,
    fs.site_name,
    ds.measurement_date,
    ds.avg_moisture,
    ds.avg_temperature,
    COALESCE(di.irrigation_count, 0) AS irrigation_count,
    COALESCE(di.total_water_volume, 0) AS total_water_volume
FROM farming_sites fs
JOIN daily_sensor ds ON fs.site_id = ds.site_id
LEFT JOIN daily_irrigation di 
    ON fs.site_id = di.site_id 
    AND ds.measurement_date = di.irrigation_date
ORDER BY ds.measurement_date ASC, fs.site_id;
"""

def execute_query(conn, query):
    with conn.cursor() as cur:
        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    return colnames, rows

def main():
    try:
        conn = psycopg2.connect(**conn_params)
        print("Connected to the database.")

        print("\nQuery 1: Average moisture below 30 with irrigation details")
        columns1, results1 = execute_query(conn, query1)
        print(tabulate(results1, headers=columns1, tablefmt="psql"))

        print("\nQuery 2: Daily aggregation analysis")
        columns2, results2 = execute_query(conn, query2)
        print(tabulate(results2, headers=columns2, tablefmt="psql"))

    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()