import psycopg2
from tabulate import tabulate

DB_CONFIG = {
    "dbname": "community_feedback",
    "user": "admin",
    "password": "admin_password",
    "host": "localhost",
    "port": "5432"
}

SQL_QUERY = """
SELECT 
    w.workshop_id,
    w.title AS workshop_title,
    w.location,
    w.date,
    w.organizer,
    COALESCE(ROUND(AVG(f.rating)::NUMERIC, 2), 0.00) AS avg_rating,
    COALESCE(COUNT(f.feedback_id), 0) AS total_feedbacks
FROM workshops w
LEFT JOIN feedback f ON w.workshop_id = f.workshop_id
GROUP BY w.workshop_id
ORDER BY avg_rating DESC, total_feedbacks DESC;
"""

def fetch_workshop_feedback():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute(SQL_QUERY)
        results = cur.fetchall()

        headers = ["Workshop ID", "Title", "Location", "Date", "Organizer", "Avg Rating", "Total Feedbacks"]

        print(tabulate(results, headers=headers, tablefmt="grid"))

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_workshop_feedback()