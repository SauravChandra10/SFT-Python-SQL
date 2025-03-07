import psycopg2
import pandas as pd
from tabulate import tabulate
import datetime
import os

DB_PARAMS = {
    "dbname": "freight_fleet_management",
    "user": "admin",
    "password": "admin_password",
    "host": "localhost",
    "port": "5432"
}

def connect_to_database():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print("Successfully connected to the database.")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def get_maintenance_alerts():
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
            WHEN sr.temperature > 90 THEN 'High Engine Temperature'
            WHEN sr.oil_pressure < 30 THEN 'Low Oil Pressure'
            WHEN sr.tire_pressure < 30 OR sr.tire_pressure > 45 THEN 'Irregular Tire Pressure'
            WHEN (CURRENT_DATE - v.last_maintenance_date) > 60 THEN 'Maintenance Overdue'
            ELSE 'Normal'
        END AS alert_type
    FROM 
        vehicles v
    JOIN 
        sensors s ON v.vehicle_id = s.vehicle_id
    JOIN 
        sensor_readings sr ON s.sensor_id = sr.sensor_id
    WHERE 
        -- Get the latest sensor reading for each vehicle
        sr.timestamp = (
            SELECT MAX(sr2.timestamp) 
            FROM sensor_readings sr2 
            JOIN sensors s2 ON sr2.sensor_id = s2.sensor_id 
            WHERE s2.vehicle_id = v.vehicle_id
        )
        AND (
            -- Apply maintenance alert conditions
            sr.temperature > 90
            OR sr.oil_pressure < 30
            OR sr.tire_pressure < 30 OR sr.tire_pressure > 45
            OR (CURRENT_DATE - v.last_maintenance_date) > 60
        )
    ORDER BY 
        alert_type,
        days_since_maintenance DESC;
    """
    return query

def display_paginated_results(df, page_size=10):
    if df.empty:
        print("No maintenance alerts found. All vehicles are operating within normal parameters.")
        return

    display_columns = ['registration_number', 'vehicle_type', 'model', 
                      'days_since_maintenance', 'temperature', 
                      'oil_pressure', 'tire_pressure', 'alert_type']
    
    total_records = len(df)
    total_pages = (total_records + page_size - 1) // page_size
    
    current_page = 1
    while True:
        os.system('cls' if os.name == 'nt' else 'clear')  # Clear console
        
        start_idx = (current_page - 1) * page_size
        end_idx = min(start_idx + page_size, total_records)
        
        page_data = df.iloc[start_idx:end_idx]
        
        print(f"\nMAINTENANCE ALERTS - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Found {total_records} vehicles requiring attention (Page {current_page} of {total_pages}):\n")
        
        print(tabulate(page_data[display_columns], 
                      headers='keys', 
                      tablefmt='pretty', 
                      showindex=False))
        
        if total_pages > 1:
            print("\nNavigation:")
            if current_page > 1:
                print("  [P] Previous page")
            if current_page < total_pages:
                print("  [N] Next page")
            print("  [S] Show summary")
            print("  [Q] Quit")
            
            choice = input("\nEnter choice: ").strip().upper()
            
            if choice == 'P' and current_page > 1:
                current_page -= 1
            elif choice == 'N' and current_page < total_pages:
                current_page += 1
            elif choice == 'S':
                show_summary(df)
                input("\nPress Enter to continue...")
            elif choice == 'Q':
                break
        else:
            show_summary(df)
            print("\nPress Enter to quit...")
            input()
            break

def show_summary(df):
    print("\nAlert Summary:")
    alert_counts = df['alert_type'].value_counts()
    for alert, count in alert_counts.items():
        print(f"- {alert}: {count} vehicles")

def main():
    conn = connect_to_database()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        print("\nRunning maintenance alert analysis...")
        cursor.execute(get_maintenance_alerts())
        
        column_names = [desc[0] for desc in cursor.description]
        
        results = cursor.fetchall()
        
        df = pd.DataFrame(results, columns=column_names)
        
        display_paginated_results(df)
        
        output_file = f"maintenance_alerts_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
        df.to_csv(output_file, index=False)
        print(f"\nDetailed results saved to {output_file}")
            
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