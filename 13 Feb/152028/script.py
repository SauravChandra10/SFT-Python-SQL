import psycopg2
from tabulate import tabulate

def main():
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="regulatory_compliance_db",
        user="admin",
        password="admin_password"
    )

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
      WHERE c.issue_found = TRUE AND c.severity = 'High'
    )
    SELECT  
      regulation,  
      COUNT(query_id) AS total_queries,  
      AVG(execution_time) AS avg_execution_time,  
      AVG(cache_hit_rate) AS avg_cache_hit_rate,  
      SUM(CASE WHEN issue_found THEN 1 ELSE 0 END) AS total_compliance_issues  
    FROM query_details  
    GROUP BY regulation  
    ORDER BY avg_cache_hit_rate DESC;
    """

    try:
        cur.execute(query)
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description]

        print(tabulate(rows, headers, tablefmt="psql"))
    except Exception as e:
        print("Error executing query:", e)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()