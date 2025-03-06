import psycopg2
from tabulate import tabulate

def main():
    conn_params = {
        "dbname": "aerospace_procurement_db",
        "user": "admin",
        "password": "admin_password",
        "host": "localhost",
        "port": "5432"
    }

    query = """
    SELECT  
      s.supplier_id,  
      s.supplier_name,  
      s.location,  
      s.rating,  
      COUNT(p.procurement_id) AS num_procurements,  
      SUM(p.cost) AS total_procurement_cost,  
      AVG(p.cost) AS avg_procurement_cost,  
      AVG(ic.check_score) AS avg_check_score,  
      SUM(CASE WHEN ic.check_passed = 'No' THEN 1 ELSE 0 END) AS failed_checks,  
      MAX(p.procurement_date) AS last_procurement_date,
      MAX(ic.check_date) AS last_check_date

    FROM  
      suppliers s  
    LEFT JOIN  
      procurements p ON s.supplier_id = p.supplier_id  
    LEFT JOIN  
      integrity_checks ic ON s.supplier_id = ic.supplier_id  
    GROUP BY  
      s.supplier_id, s.supplier_name, s.location, s.rating  
    HAVING  
      AVG(ic.check_score) < 80  
      OR SUM(CASE WHEN ic.check_passed = 'No' THEN 1 ELSE 0 END) > 0  
    ORDER BY  
      avg_check_score ASC, total_procurement_cost DESC;
    """

    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        cur.execute(query)
        results = cur.fetchall()

        headers = [desc[0] for desc in cur.description]

        print(tabulate(results, headers=headers, tablefmt="psql"))

    except Exception as e:
        print("An error occurred:", e)

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()