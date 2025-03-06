import psycopg2
from tabulate import tabulate

def main():
    conn_params = {
        "dbname": "water_quality_db",
        "user": "admin",
        "password": "admin_password",
        "host": "localhost",
        "port": "5432"
    }

    sql_query = """
    WITH industry_summary AS (
        SELECT 
            location_id,
            industry_type,
            AVG(emission_level) AS avg_emission
        FROM industrial_activity
        GROUP BY location_id, industry_type
    ),
    water_summary AS (
        SELECT 
            location_id,
            AVG(measurement_value) AS avg_measurement
        FROM water_quality
        GROUP BY location_id
    )
    SELECT 
        l.site_name,
        l.region,
        i.industry_type,
        i.avg_emission,
        w.avg_measurement,
        (i.avg_emission / NULLIF(w.avg_measurement, 0)) AS emission_to_quality_ratio
    FROM industry_summary i
    JOIN water_summary w ON i.location_id = w.location_id
    JOIN locations l ON l.location_id = i.location_id;
    """

    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        cursor.execute(sql_query)
        results = cursor.fetchall()

        headers = [desc[0] for desc in cursor.description]

        print(tabulate(results, headers=headers, tablefmt="psql"))
    
    except Exception as error:
        print("Error executing query:", error)
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
