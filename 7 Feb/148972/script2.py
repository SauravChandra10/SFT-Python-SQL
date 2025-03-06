import psycopg2
import csv
import logging
from tabulate import tabulate

logging.basicConfig(filename="logs.txt", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_and_store():
    try:
        conn = psycopg2.connect(
            dbname="secure_port_management",
            user="admin",
            password="admin_password",
            host="localhost"
        )
        cursor = conn.cursor()

        query = """
        WITH authorized_roles AS (
            SELECT role  
            FROM role_permissions  
            WHERE can_view_manifests = TRUE  
        )  
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
        WHERE au.role IN (SELECT role FROM authorized_roles)
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        if rows:
            headers = ["Manifest ID", "Vessel Name", "Departure Port", "Arrival Port", "Departure Date", "Arrival Date", "Username", "Email", "Role"]

            with open("output.csv", mode="w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(headers)
                writer.writerows(rows)

            logging.info("Query executed successfully. Results saved to output.csv.")

            print(tabulate(rows, headers=headers, tablefmt="grid"))

        else:
            logging.info("Query executed successfully, but no records were found.")
            print("No records found.")

    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    fetch_and_store()