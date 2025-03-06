import psycopg2
from psycopg2.extras import DictCursor
from tabulate import tabulate
import csv

conn_params = {
    'dbname': 'market_research_db',
    'user': 'admin',
    'password': 'admin_password',
    'host': 'localhost',
    'port': 5432
}

def run_query(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()

def main():
    try:
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor(cursor_factory=DictCursor)
        
        query1 = """
        WITH interaction_intervals AS (
            SELECT
                user_id,
                platform_id,
                interaction_type,
                interaction_timestamp,
                EXTRACT(EPOCH FROM (interaction_timestamp - LAG(interaction_timestamp) OVER (PARTITION BY user_id ORDER BY interaction_timestamp)))/60 AS interval_minutes
            FROM interactions
        )
        SELECT
            p.platform_name,
            i.interaction_type,
            COUNT(*) AS total_interactions,
            ROUND(AVG(i.interval_minutes) FILTER (WHERE i.interval_minutes IS NOT NULL), 2) AS avg_time_between_interactions,
            MIN(i.interaction_timestamp) AS first_interaction,
            MAX(i.interaction_timestamp) AS last_interaction
        FROM interaction_intervals i
        JOIN platforms p ON i.platform_id = p.platform_id
        GROUP BY p.platform_name, i.interaction_type;
        """
        
        results1 = run_query(cursor, query1)
        keys1 = ['platform_name', 'interaction_type', 'total_interactions', 'avg_time_between_interactions', 'first_interaction', 'last_interaction']
        headers1 = ['Platform', 'Interaction Type', 'Total Interactions', 'Avg Time (min)', 'First Interaction', 'Last Interaction']
        data1 = [[row[k] for k in keys1] for row in results1]
        
        print("Query 1 Results:")
        print(tabulate(data1, headers=headers1, tablefmt='psql'))
        
        with open('output1.csv', 'w', newline='') as file1:
            writer = csv.writer(file1)
            writer.writerow(headers1)
            writer.writerows(data1)
        
        query2 = """
        WITH daily_counts AS (
            SELECT
                 platform_id,
                 DATE(interaction_timestamp) AS interaction_date,
                 COUNT(*) AS daily_interactions
            FROM interactions
            GROUP BY platform_id, DATE(interaction_timestamp)
        ),
        daily_stats AS (
            SELECT
                 platform_id,
                 interaction_date,
                 daily_interactions,
                 AVG(daily_interactions) OVER (
                     PARTITION BY platform_id
                     ORDER BY interaction_date
                     ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                 ) AS moving_avg_7d
            FROM daily_counts
        )
        SELECT
            p.platform_name,
            d.interaction_date,
            d.daily_interactions,
            ROUND(d.moving_avg_7d, 2) AS moving_avg_7d
        FROM daily_stats d
        JOIN platforms p ON d.platform_id = p.platform_id
        ORDER BY p.platform_name, d.interaction_date;
        """
        
        results2 = run_query(cursor, query2)
        keys2 = ['platform_name', 'interaction_date', 'daily_interactions', 'moving_avg_7d']
        headers2 = ['Platform', 'Date', 'Daily Interactions', '7-Day Moving Avg']
        data2 = [[row[k] for k in keys2] for row in results2]
        
        print("\nQuery 2 Results:")
        print(tabulate(data2, headers=headers2, tablefmt='psql'))
        
        with open('output2.csv', 'w', newline='') as file2:
            writer = csv.writer(file2)
            writer.writerow(headers2)
            writer.writerows(data2)
        
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if connection:
            cursor.close()
            connection.close()

if __name__ == '__main__':
    main()