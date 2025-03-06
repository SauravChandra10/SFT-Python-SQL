import psycopg2
import logging
import csv

def main():
    logging.basicConfig(
        filename='logs.txt',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logging.info("Script started")

    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="regulatory_compliance_db",
            user="admin",
            password="admin_password"
        )
        logging.info("Connected to the database successfully.")
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        return

    cur = conn.cursor()

    query = """
    WITH query_details AS (
      SELECT 
        q.query_id,
        q.execution_time,
        q.cache_hits,
        q.cache_misses,
        ROUND((q.cache_hits * 100.0) / NULLIF(q.cache_hits + q.cache_misses, 0), 2) AS cache_hit_rate,
        c.regulation,
        c.issue_found,
        c.severity,
        e.event_type,
        e.event_time,
        e.description
      FROM query_cache_stats q
      LEFT JOIN compliance_data c ON q.query_id = c.query_id
      LEFT JOIN monitoring_events e ON q.query_id = e.query_id
    )
    SELECT 
      regulation,
      COUNT(query_id) AS total_queries,
      AVG(execution_time) AS avg_execution_time,
      AVG(cache_hit_rate) AS avg_cache_hit_rate,
      SUM(CASE WHEN issue_found THEN 1 ELSE 0 END) AS total_compliance_issues
    FROM query_details
    GROUP BY regulation;
    """

    try:
        cur.execute(query)
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description]
        logging.info("Query executed successfully.")
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        cur.close()
        conn.close()
        return

    try:
        with open('output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(rows)
        logging.info("Results written to output.csv successfully.")
    except Exception as e:
        logging.error(f"Error writing to CSV file: {e}")

    cur.close()
    conn.close()
    logging.info("Database connection closed.")
    logging.info("Script completed.")

if __name__ == "__main__":
    main()