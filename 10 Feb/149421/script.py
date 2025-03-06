import psycopg2  
from tabulate import tabulate  
  
def main():  
    conn_params = {  
        "host": "localhost",  
        "database": "avionicsdiagnosticdb",  
        "user": "admin",  
        "password": "admin_password"  
    }  
    
    try:  
        connection = psycopg2.connect(**conn_params)  
        cursor = connection.cursor()  
        
        sql_query = """  
        SELECT  
            dl.system_component,  
            ca.name AS algorithm_name,  
            COUNT(*) AS num_compressions,  
            SUM(ch.original_size) AS total_original_size,  
            SUM(ch.compressed_size) AS total_compressed_size,  
            AVG(1.0 * ch.compressed_size / ch.original_size) AS avg_compression_ratio,
            MIN(ch.compression_timestamp) AS first_compression,
            MAX(ch.compression_timestamp) AS last_compression  
        FROM diagnostic_logs dl  
        JOIN compression_history ch ON dl.log_id = ch.log_id  
        JOIN compression_algorithms ca ON ch.algorithm_id = ca.algorithm_id  
        GROUP BY dl.system_component, ca.name
        ORDER BY dl.system_component, avg_compression_ratio; 
        """  
        
        cursor.execute(sql_query)  
        results = cursor.fetchall()  
        
        headers = [  
            "System Component",  
            "Compression Algorithm",  
            "Num Compressions",  
            "Total Original Size",  
            "Total Compressed Size",  
            "Avg Compression Ratio"  
        ]  
        
        print(tabulate(results, headers, tablefmt="psql"))  
    
    except Exception as error:  
        print("Error occurred:", error)  
    
    finally:  
        if connection:  
            cursor.close()  
            connection.close()  
  
if __name__ == "__main__":  
    main()  