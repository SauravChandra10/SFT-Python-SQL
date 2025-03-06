import psycopg2
import csv

def main():
    conn_params = {
        "host": "localhost",
        "database": "fashion_retailer_db",
        "user": "admin",
        "password": "admin_password",
        "port": "5432"
    }

    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        query = """
        SELECT  
            s.supplier_id,  
            s.supplier_name,  
            COUNT(c.contract_id) AS total_contracts,  
            SUM(CASE WHEN c.contract_status = 'Active' THEN 1 ELSE 0 END) AS active_contracts,  
            MIN(c.contract_date) AS first_contract_date,  
            MAX(c.contract_date) AS latest_contract_date,  
            AVG(EXTRACT(EPOCH FROM (ce.encryption_timestamp - c.contract_date::timestamp)) / 3600) AS avg_encryption_delay_hours  
        FROM suppliers s  
        JOIN contracts c ON s.supplier_id = c.supplier_id  
        JOIN contract_encryption ce ON c.contract_id = ce.contract_id  
        GROUP BY s.supplier_id, s.supplier_name  
        ORDER BY total_contracts DESC;
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        column_names = [desc[0] for desc in cursor.description]

        with open("output.csv", "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(column_names)
            writer.writerows(rows)

        print("Results have been successfully written to output.csv")

    except Exception as e:
        print("An error occurred:", e)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()