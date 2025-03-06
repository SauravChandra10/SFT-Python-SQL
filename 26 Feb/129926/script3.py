import psycopg2
import csv
import logging

def main():
    logging.basicConfig(
        filename='logs.txt',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logging.info("Script started.")
    
    try:
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
        
        with open("output1.csv", mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(supporters_headers)
            writer.writerows(supporters_data)
        logging.info("Supporters data saved to output1.csv.")
        
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
        FROM campaign_summary;
        """
        
        logging.info("Executing campaigns performance query.")
        cur.execute(query_campaigns)
        campaigns_data = cur.fetchall()
        campaigns_headers = [desc[0] for desc in cur.description]
        logging.info(f"Campaigns query executed; {len(campaigns_data)} rows fetched.")
        
        with open("output2.csv", mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(campaigns_headers)
            writer.writerows(campaigns_data)
        logging.info("Campaigns data saved to output2.csv.")
        
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