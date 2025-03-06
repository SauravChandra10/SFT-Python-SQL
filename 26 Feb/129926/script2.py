import psycopg2
from tabulate import tabulate
import logging

def main():
    # Configure logging to output to logs.txt
    logging.basicConfig(
        filename='logs.txt',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logging.info("Script started.")
    
    try:
        # Connect to the database
        conn = psycopg2.connect(
            dbname="env_advocacy_db",
            user="admin",
            password="admin_password",
            host="localhost",
            port="5432"
        )
        logging.info("Connected to the database.")
        
        cur = conn.cursor()
        logging.info("Database cursor created.")
        
        # Query 1: Supporters segmentation query
        query_supporters = """
        WITH supporter_stats AS (
          SELECT 
            s.supporter_id,
            s.name,
            COUNT(i.interaction_id) AS interaction_count,
            AVG(i.engagement_level) AS avg_engagement
          FROM supporters s
          LEFT JOIN interactions i 
            ON s.supporter_id = i.supporter_id
          GROUP BY s.supporter_id, s.name
        ),
        ranked_supporters AS (
          SELECT 
            supporter_id,
            name,
            interaction_count,
            avg_engagement,
            NTILE(3) OVER (ORDER BY avg_engagement DESC) AS engagement_rank
          FROM supporter_stats
        )
        SELECT 
          supporter_id,
          name,
          interaction_count,
          ROUND(avg_engagement::numeric, 2) AS avg_engagement,
          CASE engagement_rank
            WHEN 1 THEN 'High Engagement'
            WHEN 2 THEN 'Moderate Engagement'
            WHEN 3 THEN 'Low Engagement'
          END AS engagement_segment
        FROM ranked_supporters
        ORDER BY avg_engagement DESC;
        """
        
        logging.info("Executing supporters segmentation query.")
        cur.execute(query_supporters)
        supporters_data = cur.fetchall()
        supporters_headers = [desc[0] for desc in cur.description]
        logging.info(f"Supporters query executed; {len(supporters_data)} rows fetched.")
        
        print("Supporters Segmentation:")
        print(tabulate(supporters_data, headers=supporters_headers, tablefmt="psql"))
        
        # Query 2: Campaign performance query
        query_campaigns = """
        WITH campaign_summary AS (
          SELECT
            c.campaign_id,
            c.campaign_name,
            COUNT(i.interaction_id) AS total_interactions,
            COUNT(DISTINCT i.supporter_id) AS unique_supporters,
            AVG(i.engagement_level) AS avg_engagement,
            SUM(CASE WHEN i.engagement_level >= 4 THEN 1 ELSE 0 END) AS high_engagement_count
          FROM campaigns c
          LEFT JOIN interactions i 
            ON c.campaign_id = i.campaign_id
          GROUP BY c.campaign_id, c.campaign_name
        )
        SELECT
          campaign_id,
          campaign_name,
          total_interactions,
          unique_supporters,
          ROUND(avg_engagement::numeric, 2) AS avg_engagement,
          high_engagement_count,
          CASE 
            WHEN total_interactions > 0 
            THEN ROUND((high_engagement_count::numeric / total_interactions) * 100, 2)
            ELSE 0 
          END AS high_engagement_percentage
        FROM campaign_summary
        ORDER BY total_interactions DESC;
        """
        
        logging.info("Executing campaigns performance query.")
        cur.execute(query_campaigns)
        campaigns_data = cur.fetchall()
        campaigns_headers = [desc[0] for desc in cur.description]
        logging.info(f"Campaigns query executed; {len(campaigns_data)} rows fetched.")
        
        print("\nCampaign Performance:")
        print(tabulate(campaigns_data, headers=campaigns_headers, tablefmt="psql"))
        
        cur.close()
        logging.info("Cursor closed.")
        
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        print("An error occurred:", e)
        
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")
        logging.info("Script ended.")

if __name__ == "__main__":
    main()
