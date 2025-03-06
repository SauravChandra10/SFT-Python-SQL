import psycopg2
from psycopg2.extras import DictCursor
from tabulate import tabulate

def main():
    conn_params = {
        "host": "localhost",
        "database": "products_db",
        "user": "admin",
        "password": "admin_password",
        "port": 5432
    }
    
    try:
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor(cursor_factory=DictCursor)
        
        query1 = """
        SELECT 
            a.category,
            COUNT(f.feedback_id) AS total_feedback,
            AVG(f.rating) AS average_rating,
            SUM(CASE WHEN f.rating >= 4 THEN 1 ELSE 0 END) AS positive_feedback,
            SUM(CASE WHEN f.rating <= 2 THEN 1 ELSE 0 END) AS negative_feedback
        FROM feedback f
        JOIN appliances a ON f.appliance_id = a.appliance_id
        JOIN customers c ON f.customer_id = c.customer_id
        GROUP BY a.category
        ORDER BY average_rating DESC;
        """

        cursor.execute(query1)
        results1 = cursor.fetchall()

        headers1 = [desc[0] for desc in cursor.description]
        rows1 = [[row[col] for col in headers1] for row in results1]
        
        print("Aggregated Feedback Analysis by Category:")
        print(tabulate(rows1, headers=headers1, tablefmt="psql"))
        
        query2 = """
        WITH appliance_ratings AS (
            SELECT
                a.appliance_id,
                a.product_name,
                a.category,
                AVG(f.rating) AS avg_rating,
                COUNT(f.feedback_id) AS review_count
            FROM appliances a
            JOIN feedback f ON a.appliance_id = f.appliance_id
            GROUP BY a.appliance_id, a.product_name, a.category
        )
        SELECT 
            category,
            appliance_id,
            product_name,
            avg_rating,
            review_count
        FROM (
            SELECT
                category,
                appliance_id,
                product_name,
                avg_rating,
                review_count,
                RANK() OVER (PARTITION BY category ORDER BY avg_rating DESC) AS rank_in_category
            FROM appliance_ratings
        ) ranked
        WHERE rank_in_category = 1;
        """
        cursor.execute(query2)
        results2 = cursor.fetchall()
        headers2 = [desc[0] for desc in cursor.description]
        rows2 = [[row[col] for col in headers2] for row in results2]
        
        print("\nTop-Rated Appliance(s) Per Category:")
        print(tabulate(rows2, headers=headers2, tablefmt="psql"))
    
    except Exception as e:
        print("An error occurred:", e)
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    main()
