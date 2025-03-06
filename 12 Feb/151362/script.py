#!/usr/bin/env python3
import psycopg2
import pandas as pd
import logging
import os

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler("logs.txt", mode="w"),
            logging.StreamHandler()
        ]
    )

    logging.info("Starting the database query script.")

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

    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(**conn_params)
        logging.info("Connected to the database successfully.")
        cursor = conn.cursor()

        cursor.execute(sql_query)
        logging.info("SQL query executed successfully.")
        results = cursor.fetchall()

        headers = [desc[0] for desc in cursor.description]
        logging.info(f"Retrieved {len(results)} rows from the database.")

        df = pd.DataFrame(results, columns=headers)

        output_csv = "output.csv"
        df.to_csv(output_csv, index=False)
        logging.info(f"Results saved to CSV file: {output_csv}")

    except Exception as error:
        logging.error("Error executing query: %s", error)

    finally:
        if cursor:
            cursor.close()
            logging.info("Cursor closed.")
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()