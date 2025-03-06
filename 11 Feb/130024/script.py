import psycopg2
from tabulate import tabulate

def main():
    conn_params = {
        "dbname": "pharmacovigilance_db",
        "user": "admin",
        "password": "admin_password",
        "host": "localhost",
        "port": "5432"
    }

    query = """
    SELECT
        d.manufacturer,
        COUNT(r.report_id) AS total_reports,
        SUM(CASE WHEN r.seriousness = 'serious' THEN 1 ELSE 0 END) AS serious_reports,
        AVG(p.age) AS average_age
    FROM reports r
    JOIN drugs d ON r.drug_id = d.drug_id
    JOIN patients p ON r.patient_id = p.patient_id
    GROUP BY d.manufacturer;
    """

    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        cur.execute(query)
        rows = cur.fetchall()

        headers = ["Manufacturer", "Total Reports", "Serious Reports", "Average Age"]

        print(tabulate(rows, headers=headers, tablefmt="psql"))
    
    except Exception as e:
        print("An error occurred:", e)
    
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == '__main__':
    main()