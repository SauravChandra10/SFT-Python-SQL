import psycopg2
import gzip
import logging
import os

log_file_path = os.path.join(os.getcwd(), 'logs.txt')
logging.basicConfig(filename=log_file_path, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

DB_CONFIG = {
    "dbname": "insurance_fraud_db",
    "user": "admin",
    "password": "admin_password",
    "host": "localhost",
    "port": "5432"
}

def compress_data(data):
    text_data = f"{data['transaction_type']}|{data['location']}|{data['status']}"
    return gzip.compress(text_data.encode('utf-8'))

def decompress_data(compressed_data):
    return gzip.decompress(compressed_data).decode('utf-8')

def fetch_uncompressed_transactions():
    query = """
    SELECT t.transaction_id, t.transaction_type, t.location, t.status
    FROM transactions t
    LEFT JOIN compressed_transactions c ON t.transaction_id = c.transaction_id
    WHERE c.transaction_id IS NULL;
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            transactions = [dict(zip(columns, row)) for row in cursor.fetchall()]
            logging.info(f"Fetched {len(transactions)} uncompressed transactions.")
            return transactions
    except Exception as e:
        logging.error(f"Error fetching transactions: {e}")
        return []

def insert_compressed_data(compressed_entries):
    insert_query = """
    INSERT INTO compressed_transactions (transaction_id, compressed_data, compression_algorithm)
    VALUES (%s, %s, 'gzip')
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cursor:
            cursor.executemany(insert_query, compressed_entries)
            conn.commit()
            logging.info(f"Inserted {len(compressed_entries)} compressed transactions.")
    except Exception as e:
        logging.error(f"Error inserting compressed data: {e}")

def decompress_transactions():
    query = """
    SELECT t.transaction_id, t.policy_id, t.customer_id, c.compressed_data
    FROM compressed_transactions c
    JOIN transactions t ON t.transaction_id = c.transaction_id;
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cursor:
            cursor.execute(query)
            decompressed_entries = []
            for row in cursor.fetchall():
                transaction_id, policy_id, customer_id, compressed_data = row
                decompressed_text = decompress_data(compressed_data)
                transaction_type, location, status = decompressed_text.split('|')
                decompressed_entries.append({
                    "transaction_id": transaction_id,
                    "policy_id": policy_id,
                    "customer_id": customer_id,
                    "transaction_type": transaction_type,
                    "location": location,
                    "status": status
                })
            logging.info(f"Retrieved {len(decompressed_entries)} decompressed transactions.")
            return decompressed_entries
    except Exception as e:
        logging.error(f"Error retrieving compressed transactions: {e}")
        return []

def main():
    transactions = fetch_uncompressed_transactions()
    if not transactions:
        logging.info("No new transactions to compress.")
    else:
        compressed_entries = [
            (txn["transaction_id"], psycopg2.Binary(compress_data(txn)))
            for txn in transactions
        ]
        insert_compressed_data(compressed_entries)

    decompressed_data = decompress_transactions()
    if decompressed_data:
        logging.info("Decompressed Transactions:")
        for txn in decompressed_data:
            logging.info(txn)

if __name__ == "__main__":
    main()
