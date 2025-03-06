import psycopg2
import csv

DATABASE_CONFIG = {
    "dbname": "digital_art_collab",
    "user": "admin",
    "password": "admin_password",
    "host": "localhost",
    "port": "5432"
}

QUERY = """
WITH session_durations AS (
    SELECT
        c.artwork_id,
        a.title,
        COUNT(DISTINCT c.artist_id) AS total_collaborators,
        ROUND(AVG(EXTRACT(EPOCH FROM (COALESCE(c.session_end, NOW()) - c.session_start)) / 3600), 2) AS avg_session_duration_hours,
        SUM(CASE WHEN c.session_end IS NULL THEN 1 ELSE 0 END) AS active_sessions
    FROM collaborations c
    JOIN artworks a ON c.artwork_id = a.artwork_id
    GROUP BY c.artwork_id, a.title
)
SELECT 
    artwork_id,
    title,
    total_collaborators,
    avg_session_duration_hours,
    active_sessions
FROM session_durations
ORDER BY total_collaborators DESC, avg_session_duration_hours DESC;
"""

def fetch_and_save_to_csv():
    try:
        with psycopg2.connect(**DATABASE_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(QUERY)
                results = cur.fetchall()

                csv_filename = "output.csv"

                with open(csv_filename, mode="w", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)
                    
                    writer.writerow(["artwork_id", "title", "total_collaborators", "avg_session_duration_hours", "active_sessions"])
                    
                    writer.writerows(results)

                print(f"Data successfully saved to {csv_filename}")

    except psycopg2.Error as e:
        print("Database error:", e)

if __name__ == "__main__":
    fetch_and_save_to_csv()