import psycopg2
from tabulate import tabulate

DB_HOST = "localhost"
DB_NAME = "hr_talent_db"
DB_USER = "admin"
DB_PASS = "admin_password"

def run_query(query):
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()
        return colnames, rows
    except Exception as e:
        print("Error executing query:", e)
        return None, None

def main():
    query1 = """
    SELECT 
        c.first_name,
        c.last_name,
        c.experience_years,
        s.skill_name,
        s.proficiency_level,
        w.company_name,
        w.job_title,
        EXTRACT(YEAR FROM AGE(w.end_date, w.start_date)) AS years_at_company
    FROM 
        candidates c
    JOIN 
        candidate_skills s ON c.candidate_id = s.candidate_id
    JOIN 
        work_experience w ON c.candidate_id = w.candidate_id
    WHERE 
        c.experience_years >= 5
        AND s.proficiency_level = 'Expert'
    ORDER BY 
        years_at_company DESC, 
        c.last_name;
    """

    query2 = """
    WITH candidate_tenure AS (
        SELECT 
            candidate_id,
            SUM(EXTRACT(YEAR FROM AGE(COALESCE(end_date, CURRENT_DATE), start_date))) AS total_tenure
        FROM work_experience
        GROUP BY candidate_id
    )
    SELECT 
        cs.skill_name,
        COUNT(DISTINCT cs.candidate_id) AS candidate_count,
        ROUND(AVG(c.experience_years), 2) AS avg_reported_experience,
        ROUND(AVG(ct.total_tenure), 2) AS avg_computed_tenure
    FROM candidate_skills cs
    JOIN candidates c ON cs.candidate_id = c.candidate_id
    JOIN candidate_tenure ct ON c.candidate_id = ct.candidate_id
    GROUP BY cs.skill_name
    ORDER BY avg_computed_tenure DESC;
    """

    print("Experienced Candidates with Expert Skills:")
    cols1, rows1 = run_query(query1)
    if cols1 and rows1:
        print(tabulate(rows1, headers=cols1, tablefmt="psql"))
    else:
        print("No data returned for Query 1.")

    print("\nSkill-based Analysis:")
    cols2, rows2 = run_query(query2)
    if cols2 and rows2:
        print(tabulate(rows2, headers=cols2, tablefmt="psql"))
    else:
        print("No data returned for Query 2.")

if __name__ == '__main__':
    main()
