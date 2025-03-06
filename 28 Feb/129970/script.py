import psycopg2
from tabulate import tabulate

def run_query(conn, query, description):
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]
    print(f"\n{description}:\n")
    print(tabulate(results, headers=headers, tablefmt="psql"))
    cursor.close()

def main():
    conn_params = {
        "host": "localhost",
        "dbname": "retailer_social_posts",
        "user": "admin",
        "password": "admin_password"
    }
    
    try:
        conn = psycopg2.connect(**conn_params)
        print("Connected to the PostgreSQL database!")
    except Exception as e:
        print("Error connecting to the database:", e)
        return
    
    query1 = """
    WITH RankedSentiments AS (
        SELECT 
            p.post_id,
            p.sentiment_score,
            pr.product_name,
            r.retailer_name,
            ROW_NUMBER() OVER (PARTITION BY pr.category ORDER BY p.sentiment_score DESC) AS sentiment_rank,
            pr.category
        FROM social_media_posts p
        JOIN product_mentions pm ON p.post_id = pm.post_id
        JOIN products pr ON pm.product_id = pr.product_id
        JOIN retailers r ON pr.retailer_id = r.retailer_id
    )
    SELECT 
        post_id,
        sentiment_score,
        product_name,
        retailer_name,
        category
    FROM RankedSentiments
    WHERE sentiment_rank = 1;
    """
    
    query2 = """
    WITH monthly_sentiment AS (
        SELECT 
            pr.category,
            DATE_TRUNC('month', p.post_date) AS month,
            AVG(p.sentiment_score) AS avg_sentiment
        FROM social_media_posts p
        JOIN product_mentions pm ON p.post_id = pm.post_id
        JOIN products pr ON pm.product_id = pr.product_id
        GROUP BY pr.category, DATE_TRUNC('month', p.post_date)
    )
    SELECT
        category,
        month,
        avg_sentiment,
        LAG(avg_sentiment) OVER (PARTITION BY category ORDER BY month) AS previous_month_sentiment,
        avg_sentiment - LAG(avg_sentiment) OVER (PARTITION BY category ORDER BY month) AS month_over_month_change
    FROM monthly_sentiment
    ORDER BY category, month;
    """
    
    query3 = """
    WITH monthly_sentiment AS (
        SELECT 
            r.retailer_name,
            DATE_TRUNC('month', p.post_date) AS month,
            AVG(p.sentiment_score) AS avg_sentiment
        FROM social_media_posts p
        JOIN product_mentions pm ON p.post_id = pm.post_id
        JOIN products pr ON pm.product_id = pr.product_id
        JOIN retailers r ON pr.retailer_id = r.retailer_id
        GROUP BY r.retailer_name, DATE_TRUNC('month', p.post_date)
    )
    SELECT
        retailer_name,
        month,
        avg_sentiment,
        LAG(avg_sentiment) OVER (PARTITION BY retailer_name ORDER BY month) AS previous_month_sentiment,
        avg_sentiment - LAG(avg_sentiment) OVER (PARTITION BY retailer_name ORDER BY month) AS month_over_month_change
    FROM monthly_sentiment
    ORDER BY retailer_name, month;
    """
    
    run_query(conn, query1, "Query 1: Top Post by Sentiment within each Product Category")
    run_query(conn, query2, "Query 2: Monthly Average Sentiment Score by Product Category with MoM Change")
    run_query(conn, query3, "Query 3: Monthly Average Sentiment Score by Retailer with MoM Change")
    
    conn.close()
    print("\nConnection closed.")

if __name__ == '__main__':
    main()