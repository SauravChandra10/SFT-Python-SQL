import psycopg2
from tabulate import tabulate
import logging

logging.basicConfig(
    filename='logs.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def execute_query(query):
    logging.info("Attempting to connect to the database.")
    try:
        conn = psycopg2.connect(
            dbname="legaltech_db",
            user="admin",
            password="admin_password",
            host="localhost",
            port="5432"
        )
        logging.info("Database connection established.")
        cursor = conn.cursor()
        logging.info("Executing query:\n%s", query.strip())
        cursor.execute(query)
        results = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        logging.info("Query executed successfully. Fetched %d rows.", len(results))
        cursor.close()
        conn.close()
        logging.info("Database connection closed.")
        return results, colnames
    except Exception as e:
        logging.error("Error executing query: %s", e)
        return None, None

def main():
    logging.info("Script execution started.")
    
    query1 = """
    WITH duplicate_groups AS (
      SELECT
        document_hash,
        MIN(document_id) AS canonical_document_id,
        COUNT(*) AS total_docs
      FROM legal_documents
      GROUP BY document_hash
      HAVING COUNT(*) > 1
    ),
    duplicate_events AS (
      SELECT
        original_document_id,
        COUNT(*) AS event_count
      FROM document_duplicates
      GROUP BY original_document_id
    )
    SELECT
      dg.document_hash,
      dg.canonical_document_id,
      ld.document_title,
      lc.case_name,
      dg.total_docs,
      COALESCE(de.event_count, 0) AS duplicate_events_logged
    FROM duplicate_groups dg
    JOIN legal_documents ld ON dg.canonical_document_id = ld.document_id
    JOIN legal_cases lc ON ld.case_id = lc.case_id
    LEFT JOIN duplicate_events de ON dg.canonical_document_id = de.original_document_id;
    """

    query2 = """
    WITH duplicate_hashes AS (
      SELECT document_hash
      FROM legal_documents
      GROUP BY document_hash
      HAVING COUNT(*) > 1
    )
    SELECT DISTINCT lc.case_id,
           lc.case_name,
           lc.case_status,
           lc.filed_date
    FROM legal_cases lc
    JOIN legal_documents ld ON lc.case_id = ld.case_id
    WHERE ld.document_hash NOT IN (SELECT document_hash FROM duplicate_hashes);
    """

    logging.info("Executing Query 1: Duplicate Groups with Canonical Document Details.")
    results1, colnames1 = execute_query(query1)
    if results1 is not None:
        print("Duplicate Groups with Canonical Document Details:")
        print(tabulate(results1, headers=colnames1, tablefmt="psql"))
        logging.info("Query 1 results printed successfully.")
    else:
        print("Failed to retrieve duplicate group data.")
        logging.error("Query 1 failed to retrieve data.")

    logging.info("Executing Query 2: Cases Without Duplicates.")
    results2, colnames2 = execute_query(query2)
    if results2 is not None:
        print("\nCases Without Duplicates:")
        print(tabulate(results2, headers=colnames2, tablefmt="psql"))
        logging.info("Query 2 results printed successfully.")
    else:
        print("Failed to retrieve non-duplicate case data.")
        logging.error("Query 2 failed to retrieve data.")

    logging.info("Script execution finished.")

if __name__ == "__main__":
    main()