import psycopg2
from tabulate import tabulate

def run_query(connection, query):
    with connection.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    return columns, results

def main():
    conn_params = {
        'dbname': 'metal_processing_db',
        'user': 'admin',
        'password': 'admin_password',
        'host': 'localhost',
        'port': 5432
    }

    try:
        conn = psycopg2.connect(**conn_params)
        print("Connected to the database successfully.\n")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return

    query1 = """
    SELECT  
        rm.metal_type,  
        COUNT(DISTINCT pb.batch_id) AS completed_batches,  
        AVG(EXTRACT(EPOCH FROM (pb.end_time - pb.start_time)) / 3600) AS avg_processing_time_hours,  
        MIN(EXTRACT(EPOCH FROM (pb.end_time - pb.start_time)) / 3600) AS min_processing_time_hours,  
        MAX(EXTRACT(EPOCH FROM (pb.end_time - pb.start_time)) / 3600) AS max_processing_time_hours,  
        SUM(CASE WHEN ml.event_type = 'Error' THEN 1 ELSE 0 END) AS error_count  
    FROM raw_materials rm  
    JOIN processing_batches pb ON rm.material_id = pb.material_id  
    LEFT JOIN machine_logs ml ON pb.batch_id = ml.batch_id  
    WHERE pb.status = 'Completed'  
    AND pb.start_time >= '2025-01-01'  
    AND pb.start_time < '2026-01-01'  
    GROUP BY rm.metal_type  
    ORDER BY rm.metal_type;
    """

    query2 = """
    SELECT 
        rm.supplier,
        COUNT(pb.batch_id) AS total_batches,
        COUNT(CASE WHEN pb.status = 'Failed' THEN 1 END) AS failed_batches,
        ROUND(100.0 * COUNT(CASE WHEN pb.status = 'Failed' THEN 1 END) / COUNT(pb.batch_id), 2) AS failure_percentage,
        ROUND(AVG(EXTRACT(EPOCH FROM (pb.end_time - pb.start_time)) / 3600)::numeric, 2) AS avg_processing_hours
    FROM raw_materials rm
    JOIN processing_batches pb ON rm.material_id = pb.material_id
    WHERE pb.status IN ('Completed', 'Failed')
    GROUP BY rm.supplier;
    """

    columns, results = run_query(conn, query1)
    print("Query 1 Results:")
    print(tabulate(results, headers=columns, tablefmt="psql"))
    print("\n")

    columns, results = run_query(conn, query2)
    print("Query 2 Results:")
    print(tabulate(results, headers=columns, tablefmt="psql"))
    print("\n")

    conn.close()

if __name__ == '__main__':
    main()