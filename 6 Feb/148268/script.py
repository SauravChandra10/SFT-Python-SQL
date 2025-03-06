import psycopg2
from psycopg2.extras import DictCursor
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": "automotive_production",
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": "localhost",
    "port": "5432"
}

def get_role_permissions():
    query = """
    SELECT  
        p.role,  
        p.can_view,  
        p.can_edit,  
        p.can_delete,  
        COALESCE(  
            (SELECT SUM(ps.quantity)  
            FROM production_schedules ps  
            JOIN users u ON ps.last_modified_by = u.user_id  
            WHERE u.role = p.role),  
            0  
        ) AS total_units_produced  
    FROM permissions p  
    ORDER BY total_units_produced DESC NULLS LAST; 
    """

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

            print(f"{'Role':<15}{'Can View':<10}{'Can Edit':<10}{'Can Delete':<12}{'Total Units Produced'}")
            for row in results:
                print(f"{row['role']:<15}{row['can_view']:<10}{row['can_edit']:<10}{row['can_delete']:<12}{row['total_units_produced']}")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    get_role_permissions()