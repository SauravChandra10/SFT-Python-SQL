import psycopg2
from tabulate import tabulate

def main():
    conn = psycopg2.connect(
        dbname="precious_metals_trade",
        user="admin",
        password="admin_password",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    sql_query = """
    SELECT s.shard_location, m.metal_name,
           COUNT(t.transaction_id) AS transaction_count,
           SUM(t.quantity) AS total_quantity,
           AVG(t.price) AS average_price
    FROM transactions t
    JOIN shards s ON t.shard_id = s.shard_id
    JOIN metals m ON t.metal_id = m.metal_id
    GROUP BY s.shard_location, m.metal_name;
    """

    cursor.execute(sql_query)
    rows = cursor.fetchall()

    headers = [desc[0] for desc in cursor.description]
 
    print(tabulate(rows, headers=headers, tablefmt="psql"))

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
