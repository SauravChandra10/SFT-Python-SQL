import psycopg2
from tabulate import tabulate

DB_CONFIG = {
    'dbname': 'fitness_app_db',
    'user': 'admin',
    'password': 'admin_password',
    'host': 'localhost',
    'port': '5432'
}

sql_query1 = """
SELECT 
    DATE_TRUNC('month', w.workout_date) AS workout_month,
    wt.type_name,
    COUNT(w.workout_id) AS workout_count,
    AVG(w.duration) AS average_duration,
    SUM(w.calories_burned) AS total_calories_burned
FROM workouts w
JOIN workout_types wt ON w.workout_type_id = wt.type_id
GROUP BY workout_month, wt.type_name
ORDER BY workout_count DESC;
"""

sql_query2 = """
WITH ranked_workouts AS (
    SELECT 
        u.username,
        wt.type_name,
        COUNT(*) AS workout_count,
        RANK() OVER (PARTITION BY u.user_id ORDER BY COUNT(*) DESC) AS rank_per_user
    FROM workouts w
    JOIN users u ON w.user_id = u.user_id
    JOIN workout_types wt ON w.workout_type_id = wt.type_id
    GROUP BY u.user_id, u.username, wt.type_name
)
SELECT 
    username,
    type_name,
    workout_count
FROM ranked_workouts
WHERE rank_per_user <= 3
ORDER BY username;
"""

def execute_query(query, connection):
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
    return colnames, rows

def main():
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        print("Connected to the database successfully.")

        colnames1, rows1 = execute_query(sql_query1, connection)
        print("\nQuery 1 Results: Monthly Trends by Workout Type")
        print(tabulate(rows1, headers=colnames1, tablefmt="psql"))

        colnames2, rows2 = execute_query(sql_query2, connection)
        print("\nQuery 2 Results: Top 3 Workout Types for Each User")
        print(tabulate(rows2, headers=colnames2, tablefmt="psql"))

    except Exception as e:
        print("An error occurred:", e)
    finally:
        if connection:
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()