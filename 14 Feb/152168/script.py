import psycopg2
import logging
import csv

def main():
    logging.basicConfig(
        filename='logs.txt',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
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
      SUM(CASE WHEN ic.check_passed = 'No' THEN 1 ELSE 0 END) AS failed_checks
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

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        logging.info("Connected to the database successfully.")

        cur.execute(query)
        results = cur.fetchall()
        headers = [desc[0] for desc in cur.description]
        logging.info(f"Query executed successfully. Fetched {len(results)} rows.")

        output_file = "output.csv"
        with open(output_file, mode="w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            writer.writerows(results)
        logging.info(f"Results successfully saved to {output_file}.")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)

    finally:
        if cur:
            cur.close()
            logging.info("Database cursor closed.")
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()