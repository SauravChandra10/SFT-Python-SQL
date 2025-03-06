import psycopg2
from tabulate import tabulate

DB_CONFIG = {
    "dbname": "insurance_actuarial",
    "user": "admin",
    "password": "admin_password",
    "host": "localhost",
    "port": "5432"
}

SQL_QUERY = """
WITH change_diffs AS (
    SELECT
        model_id,
        change_date,
        EXTRACT(EPOCH FROM (change_date - LAG(change_date) OVER (PARTITION BY model_id ORDER BY change_date))) AS diff_seconds
    FROM model_changes
),
model_change_stats AS (
    SELECT 
        model_id,
        COUNT(*) AS total_changes,
        MIN(change_date) AS first_change,
        MAX(change_date) AS last_change,
        COALESCE(AVG(diff_seconds), 0) AS avg_seconds_between_changes
    FROM change_diffs
    GROUP BY model_id
)
SELECT 
    am.model_id,
    am.model_name,
    COALESCE(mcs.total_changes, 0) AS total_changes,
    mcs.first_change,
    mcs.last_change,
    COALESCE(mcs.avg_seconds_between_changes, 0) AS avg_seconds_between_changes
FROM actuarial_models am
LEFT JOIN model_change_stats mcs ON am.model_id = mcs.model_id
"""

def fetch_change_stats():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute(SQL_QUERY)
        results = cur.fetchall()

        headers = ["Model ID", "Model Name", "Total Changes", "First Change", "Last Change", "Avg Seconds Between Changes"]

        print(tabulate(results, headers=headers, tablefmt="grid"))

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_change_stats()
