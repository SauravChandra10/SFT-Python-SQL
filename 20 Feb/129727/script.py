import psycopg2
from tabulate import tabulate

DB_PARAMS = {
    "host": "localhost",
    "database": "ecotourism_db",
    "user": "admin",
    "password": "admin_password"
}

query1 = """
SELECT 
  i.itinerary_id,
  u.first_name,
  u.last_name,
  i.itinerary_name,
  i.start_date,
  i.end_date,
  ARRAY_AGG(d.name) AS destination_names,
  AVG(d.eco_rating) AS avg_eco_rating
FROM itineraries i
JOIN users u ON i.user_id = u.user_id
JOIN LATERAL unnest(i.destination_ids) AS dest_id ON true
JOIN destinations d ON d.destination_id = dest_id
GROUP BY i.itinerary_id, u.first_name, u.last_name, i.itinerary_name, i.start_date, i.end_date
ORDER BY i.itinerary_id;
"""

query2 = """
SELECT
  u.user_id,
  u.first_name,
  u.last_name,
  COUNT(DISTINCT i.itinerary_id) AS total_itineraries,
  COUNT(d.destination_id) AS total_destinations,
  AVG(d.eco_rating) AS overall_avg_eco_rating,
  ARRAY_AGG(DISTINCT d.country) AS countries_visited
FROM users u
JOIN itineraries i ON u.user_id = i.user_id
JOIN LATERAL unnest(i.destination_ids) AS dest_id ON true
JOIN destinations d ON d.destination_id = dest_id
GROUP BY u.user_id, u.first_name, u.last_name
ORDER BY total_itineraries DESC;
"""

def execute_query(cursor, query):
    cursor.execute(query)
    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]
    return headers, rows

def main():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        with conn.cursor() as cur:
            headers1, result1 = execute_query(cur, query1)
            print("Itinerary Details:")
            print(tabulate(result1, headers=headers1, tablefmt="psql"))
            print("\n")

            headers2, result2 = execute_query(cur, query2)
            print("User Aggregated Data:")
            print(tabulate(result2, headers=headers2, tablefmt="psql"))
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()