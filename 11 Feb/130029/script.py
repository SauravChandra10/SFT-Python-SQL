import psycopg2
from tabulate import tabulate

def main():
    conn_params = {
        "dbname": "industrial_design_platform",
        "user": "admin",
        "password": "admin_password",
        "host": "localhost",
        "port": "5432"
    }
    
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        query = """
        SELECT
            u.email,
            u.full_name,
            COUNT(s.session_id) AS total_sessions,
            MAX(s.created_at) AS last_login,
            c.last_updated AS credential_last_updated
        FROM users u
        LEFT JOIN sessions s ON u.user_id = s.user_id
        LEFT JOIN credentials c ON u.user_id = c.user_id
        GROUP BY u.user_id, u.email, u.full_name, c.last_updated;
        """

        cur.execute(query)
        rows = cur.fetchall()

        headers = ['Email', 'Full Name', 'Total Sessions', 'Last Login', 'Credential Last Updated']
        
        print(tabulate(rows, headers=headers, tablefmt='psql'))

    except Exception as e:
        print("An error occurred:", e)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    main()
