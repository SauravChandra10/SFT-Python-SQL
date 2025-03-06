import psycopg2
from tabulate import tabulate

def fetch_and_display():
    try:
        conn = psycopg2.connect(
            dbname="secure_port_management",
            user="admin",
            password="admin_password",
            host="localhost"
        )
        cursor = conn.cursor()

        query = """
            SELECT 
                sm.manifest_id,
                sm.vessel_name,
                sm.departure_port,
                sm.arrival_port,
                sm.departure_date,
                sm.arrival_date,
                au.username,
                au.email,
                au.role
            FROM shipping_manifest sm
            CROSS JOIN authorized_users au
            JOIN role_permissions rp ON au.role = rp.role
            WHERE rp.can_view_manifests = TRUE
            ORDER BY sm.departure_date DESC, au.role;
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        headers = ["Manifest ID", "Vessel Name", "Departure Port", "Arrival Port", "Departure Date", "Arrival Date", "Username", "Email", "Role"]
        print(tabulate(rows, headers=headers, tablefmt="grid"))

    except psycopg2.Error as e:
        print("Database error:", e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    fetch_and_display()
