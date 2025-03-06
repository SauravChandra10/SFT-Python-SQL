import psycopg2
from psycopg2 import Error
from tabulate import tabulate
import logging

logging.basicConfig(
    filename='logs.txt',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def execute_query(query, connection):
    try:
        cursor = connection.cursor()
        logging.info("Executing query: %s", query.strip().split('\n')[0])
        cursor.execute(query)
        results = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        logging.info("Query executed successfully, fetched %d rows", len(results))
        return headers, results
    except Exception as e:
        logging.error("Error executing query: %s", e)
        raise

def main():
    connection = None
    try:
        connection = psycopg2.connect(
            host='localhost',
            database='fashion_trends',
            user='admin',
            password='admin_password'
        )
        logging.info("Connected to the PostgreSQL database.")
        print("Connected to the PostgreSQL database.\n")
        
        query1 = """
        WITH TrendEngagement AS (
            SELECT
                f.trend_id,
                f.trend_name,
                f.season,
                f.year,
                f.popularity_score,
                SUM(s.likes + s.shares + s.comments) AS total_social_engagement,
                AVG(s.engagement_score) AS avg_engagement_score
            FROM fashion_trends f
            JOIN social_media_impact s ON f.trend_id = s.trend_id
            GROUP BY f.trend_id
        ),
        ColorUsage AS (
            SELECT
                c.trend_id,
                SUM(c.usage_frequency) AS total_color_usage
            FROM color_palettes c
            GROUP BY c.trend_id
        )
        SELECT
            te.trend_name,
            te.season,
            te.year,
            te.popularity_score,
            te.total_social_engagement,
            te.avg_engagement_score,
            cu.total_color_usage,
            (te.popularity_score * 0.4 + te.avg_engagement_score * 0.4 + cu.total_color_usage * 0.2) AS trend_influence_score
        FROM TrendEngagement te
        JOIN ColorUsage cu ON te.trend_id = cu.trend_id
        ORDER BY trend_influence_score DESC
        LIMIT 5;
        """
        headers1, results1 = execute_query(query1, connection)
        print("Most Influential Fashion Trends:")
        print(tabulate(results1, headers=headers1, tablefmt="psql"))
        logging.info("Query 1 results printed successfully.")
        print("\n")
        
        query2 = """
        WITH HighEngagementTrends AS (
            SELECT 
                f.trend_id,
                f.trend_name,
                f.season,
                f.year,
                AVG(s.engagement_score) AS avg_engagement_score,
                SUM(s.likes + s.shares + s.comments) AS total_social_engagement
            FROM fashion_trends f
            JOIN social_media_impact s ON f.trend_id = s.trend_id
            GROUP BY f.trend_id, f.trend_name, f.season, f.year
            HAVING AVG(s.engagement_score) > 90
        ),
        ColorCombinationImpact AS (
            SELECT
                cp.trend_id,
                cp.primary_color,
                cp.secondary_color,
                cp.tertiary_color,
                SUM(cp.usage_frequency) AS total_usage_frequency
            FROM color_palettes cp
            GROUP BY cp.trend_id, cp.primary_color, cp.secondary_color, cp.tertiary_color
        )
        SELECT
            h.trend_name,
            h.season,
            h.year,
            h.avg_engagement_score,
            h.total_social_engagement,
            c.primary_color,
            c.secondary_color,
            c.tertiary_color,
            c.total_usage_frequency
        FROM HighEngagementTrends h
        JOIN ColorCombinationImpact c ON h.trend_id = c.trend_id
        ORDER BY h.avg_engagement_score DESC, c.total_usage_frequency DESC
        LIMIT 10;
        """
        headers2, results2 = execute_query(query2, connection)
        print("Best Color Combinations for High Engagement Trends:")
        print(tabulate(results2, headers=headers2, tablefmt="psql"))
        logging.info("Query 2 results printed successfully.")
    
    except (Exception, Error) as error:
        logging.error("Error while connecting to PostgreSQL or executing queries: %s", error)
        print("Error while connecting to PostgreSQL:", error)
    
    finally:
        if connection:
            connection.close()
            logging.info("PostgreSQL connection closed.")
            print("\nPostgreSQL connection closed.")

if __name__ == "__main__":
    main()