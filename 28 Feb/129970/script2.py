import psycopg2
from psycopg2 import sql
from tabulate import tabulate
from datetime import datetime, timedelta

def generate_query(conn, query_type, **params):
    if query_type == 'extreme_sentiment':
        start_date = params.get('start_date')
        end_date = params.get('end_date')
        query = sql.SQL("""
            SELECT 
                pr.product_name,
                MIN(p.sentiment_score) AS min_sentiment,
                MAX(p.sentiment_score) AS max_sentiment
            FROM social_media_posts p
            JOIN product_mentions pm ON p.post_id = pm.post_id
            JOIN products pr ON pm.product_id = pr.product_id
            WHERE p.post_date BETWEEN {start_date} AND {end_date}
            GROUP BY pr.product_name;
        """).format(
            start_date=sql.Literal(start_date),
            end_date=sql.Literal(end_date)
        )
        return query.as_string(conn)
    elif query_type == 'positive_trend':
        threshold = params.get('threshold', 0.7)
        query = sql.SQL("""
            SELECT 
                pr.category,
                COUNT(*) AS positive_count
            FROM social_media_posts p
            JOIN product_mentions pm ON p.post_id = pm.post_id
            JOIN products pr ON pm.product_id = pr.product_id
            WHERE p.sentiment_score > {threshold}
            GROUP BY pr.category;
        """).format(
            threshold=sql.Literal(threshold)
        )
        return query.as_string(conn)
    else:
        raise ValueError("Unsupported query type.")

def main():
    conn_params = {
        "host": "localhost",
        "dbname": "retailer_social_posts",
        "user": "admin",
        "password": "admin_password"
    }
    
    try:
        conn = psycopg2.connect(**conn_params)
        print("Connected to the PostgreSQL database successfully!")
    except Exception as e:
        print("Error connecting to the database:", e)
        return
    
    end_date = datetime.now().date()
    start_date = (end_date - timedelta(days=30)).isoformat()
    threshold = 0.7
    
    extreme_query = generate_query(conn, 'extreme_sentiment', start_date=start_date, end_date=end_date)
    positive_query = generate_query(conn, 'positive_trend', threshold=threshold)
    
    cursor = conn.cursor()
    
    cursor.execute(extreme_query)
    extreme_results = cursor.fetchall()
    extreme_headers = [desc[0] for desc in cursor.description]
    print("Extreme Sentiment Query Results:")
    print(tabulate(extreme_results, headers=extreme_headers, tablefmt="psql"))
    
    cursor.execute(positive_query)
    positive_results = cursor.fetchall()
    positive_headers = [desc[0] for desc in cursor.description]
    print("\nPositive Trend Query Results:")
    print(tabulate(positive_results, headers=positive_headers, tablefmt="psql"))
    
    cursor.close()
    conn.close()
    print("\nConnection closed.")

if __name__ == '__main__':
    main()
