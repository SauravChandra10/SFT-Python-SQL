import psycopg2
import sys

def run_integrity_checks(conn):
    errors = []
    cursor = conn.cursor()

    check_query1 = """
    SELECT  
        i.inventory_id,  
        p.product_name,  
        i.batch_number,  
        i.quantity,  
        i.expiry_date,  
        p.approval_date,  
        s.supplier_name,  
        CASE  
            WHEN i.quantity < 0 THEN 'Negative Quantity'  
            WHEN i.expiry_date < p.approval_date THEN 'Expiry Date Before Approval'  
        END AS integrity_issue  
    FROM Inventory i  
    JOIN Products p ON i.product_id = p.product_id  
    JOIN Suppliers s ON i.supplier_id = s.supplier_id  
    WHERE i.quantity < 0 OR i.expiry_date < p.approval_date;  
    """
    cursor.execute(check_query1)
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            inv_id, product_name, batch_number, quantity, expiry_date, approval_date, supplier_name, issue = row
            errors.append(
                f"Inventory ID {inv_id}: {issue} (Product: {product_name}, Batch: {batch_number}, Quantity: {quantity}, Expiry: {expiry_date}, Approval: {approval_date}, Supplier: {supplier_name})"
            )

    cursor.execute("SELECT product_id FROM Products WHERE product_name IS NULL;")
    if cursor.fetchone():
        errors.append("There are records in Products with null product_name.")

    cursor.execute("SELECT supplier_id FROM Suppliers WHERE supplier_name IS NULL;")
    if cursor.fetchone():
        errors.append("There are records in Suppliers with null supplier_name.")

    cursor.execute("SELECT inventory_id FROM Inventory WHERE expiry_date IS NULL;")
    if cursor.fetchone():
        errors.append("There are records in Inventory with null expiry_date.")

    duplicate_query = """
    SELECT 
        product_id, 
        batch_number, 
        COUNT(*) AS count
    FROM Inventory
    GROUP BY product_id, batch_number
    HAVING COUNT(*) > 1;
    """
    cursor.execute(duplicate_query)
    dup_rows = cursor.fetchall()
    if dup_rows:
        for row in dup_rows:
            product_id, batch_number, count = row
            errors.append(f"Duplicate batch number '{batch_number}' found for product_id {product_id}, appears {count} times.")

    cursor.close()
    return errors

def main():
    conn_params = {
        'dbname': 'pharma_inventory_db',
        'user': 'admin',
        'password': 'admin_password',
        'host': 'localhost',
        'port': '5432'
    }

    try:
        conn = psycopg2.connect(**conn_params)
        print("Connected to PostgreSQL database successfully.")
    except psycopg2.Error as e:
        print("Error connecting to the database:", e)
        sys.exit(1)

    print("Running data integrity checks...")
    errors = run_integrity_checks(conn)

    if errors:
        print("Data integrity issues found:")
        for error in errors:
            print("  -", error)
        print("Aborting deployment due to data integrity issues.")
        conn.close()
        sys.exit(1)
    else:
        print("No data integrity issues detected. Proceeding with deployment!")

    conn.close()

if __name__ == '__main__':
    main()