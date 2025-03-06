import psycopg2
import csv
import logging

logging.basicConfig(
    filename='logs.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DB_HOST = "localhost"
DB_NAME = "hr_talent_db"
DB_USER = "admin"
DB_PASS = "admin_password"

def run_query(query):
    try:
        logging.info("Connecting to the database.")
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        logging.info("Executing query:\n%s", query.strip())
        cur.execute(query)
        rows = cur.fetchall()

        colnames = [desc[0] for desc in cur.description]
        logging.info("Query executed successfully, retrieved %d rows.", len(rows))
        cur.close()
        conn.close()
        logging.info("Database connection closed.")
        return colnames, rows
    except Exception as e:
        logging.error("Error executing query: %s", e)
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
    logging.info("Starting execution of Query 1")
    cols1, rows1 = run_query(query1)
    if cols1 and rows1:
        with open("output1.csv", "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(cols1)
            writer.writerows(rows1)
        logging.info("Saved Query 1 results to output1.csv.")
    else:
        logging.warning("No data returned for Query 1.")

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
    logging.info("Starting execution of Query 2")
    cols2, rows2 = run_query(query2)
    if cols2 and rows2:
        with open("output2.csv", "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(cols2)
            writer.writerows(rows2)
        logging.info("Saved Query 2 results to output2.csv.")
    else:
        logging.warning("No data returned for Query 2.")

if __name__ == '__main__':
    logging.info("Script started.")
    main()
    logging.info("Script finished.")