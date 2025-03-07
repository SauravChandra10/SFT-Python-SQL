import psycopg2
import pandas as pd
from tabulate import tabulate
import datetime

DB_PARAMS = {
    "dbname": "freight_fleet_management",
    "user": "admin",
    "password": "admin_password",
    "host": "localhost",
    "port": "5432"
}

THRESHOLDS = {
    "temperature": 90,
    "oil_pressure": 30,
    "tire_pressure_low": 30,
    "tire_pressure_high": 45,
    "maintenance_overdue_days": 60
}

def connect_to_database():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print("Successfully connected to the database.")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def get_maintenance_alerts_query():
    query = """
    SELECT
        v.vehicle_id,
        v.registration_number,
        v.vehicle_type,
        v.model,
        v.status,
        v.last_maintenance_date,
        CURRENT_DATE - v.last_maintenance_date AS days_since_maintenance,
        v.total_mileage,
        sr.temperature,
        sr.oil_pressure,
        sr.tire_pressure,
        CASE
            WHEN sr.temperature > %(temperature)s THEN 'High Engine Temperature'
            WHEN sr.oil_pressure < %(oil_pressure)s THEN 'Low Oil Pressure'
            WHEN sr.tire_pressure < %(tire_pressure_low)s OR sr.tire_pressure > %(tire_pressure_high)s THEN 'Irregular Tire Pressure'
            WHEN (CURRENT_DATE - v.last_maintenance_date) > %(maintenance_overdue_days)s THEN 'Maintenance Overdue'
            ELSE 'Normal'
        END AS alert_type
    FROM
        vehicles v
    JOIN
        sensors s ON v.vehicle_id = s.vehicle_id
    JOIN
        sensor_readings sr ON s.sensor_id = sr.sensor_id
    WHERE
        sr.timestamp = (
            SELECT MAX(sr2.timestamp)
            FROM sensor_readings sr2
            JOIN sensors s2 ON sr2.sensor_id = s2.sensor_id
            WHERE s2.vehicle_id = v.vehicle_id
        )
        AND (
            sr.temperature > %(temperature)s OR
            sr.oil_pressure < %(oil_pressure)s OR
            sr.tire_pressure < %(tire_pressure_low)s OR sr.tire_pressure > %(tire_pressure_high)s OR
            (CURRENT_DATE - v.last_maintenance_date) > %(maintenance_overdue_days)s
        )
    ORDER BY
        alert_type,
        days_since_maintenance DESC;
    """
    return query

def main():
    conn = connect_to_database()
    if not conn:
        return

    try:
        cursor = conn.cursor()
        print("\nRunning maintenance alert analysis...")

        query = get_maintenance_alerts_query()
        cursor.execute(query, THRESHOLDS)

        column_names = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()

        if not results:
            print("No maintenance alerts found. All vehicles are operating within normal parameters.")
        else:
            df = pd.DataFrame(results, columns=column_names)
            print(f"\nMAINTENANCE ALERTS - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Found {len(df)} vehicles requiring attention:\n")
            display_columns = ['registration_number', 'vehicle_type', 'model',
                               'days_since_maintenance', 'temperature',
                               'oil_pressure', 'tire_pressure', 'alert_type']
            print(tabulate(df[display_columns],
                           headers='keys',
                           tablefmt='pretty',
                           showindex=False))

            alert_counts = df['alert_type'].value_counts()
            print("\nAlert Summary:")
            for alert, count in alert_counts.items():
                print(f"- {alert}: {count} vehicles")

    except Exception as e:
        print(f"Error during analysis: {e}")
    finally:
        if conn:
            if 'cursor' in locals():
                cursor.close()
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()