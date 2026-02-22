import os
import time
import random
from psycopg_pool import ConnectionPool
from datetime import timedelta

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "root")
DB_NAME = os.getenv("DB_NAME", "restaurant_db")

def get_product_data(conn):
    products = {}
    with conn.cursor() as cur:
        cur.execute("SELECT product_id, price, cost FROM products")
        for product_id, price, cost in cur.fetchall():
            products[product_id] = {"price": price, "cost": cost}

    return products

def generate_order_item(products):
    product_id = random.randint(1, 64)
    
    item = {
        "product_id": product_id,
        "quantity": random.randint(1, 5),
        "unit_price": products[product_id]["price"],
        "unit_cost": products[product_id]["cost"]
    }

    return item

def generate_orders(pool, products):
    with pool.connection() as conn:
        with conn:
            cur = conn.cursor()
            customer_id = random.choices([0, random.randint(1, 100)], k=1, weights=[0.5, 0.5])[0]
            branch_id = random.randint(1, 63)
            total_amount = 0
            payment_id = random.randint(1, 13)
            status = "PENDING"
            try:
                cur.execute("""
                            INSERT INTO orders (customer_id, branch_id, total_amount, payment_id, status)
                            VALUES (%s, %s, %s, %s, %s)
                            RETURNING order_id, created_at
                            """, 
                            (customer_id, branch_id, total_amount, payment_id, status))
                
                order_id, create_time = cur.fetchone()
                print(order_id, create_time)
                num_items = random.randint(1, 5)
                for _ in range(num_items):
                    item = generate_order_item(products)
                    cur.execute("""
                                INSERT INTO order_items (order_id, product_id, quantity, unit_price, unit_cost, created_at)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                """,
                                (order_id, item["product_id"], item["quantity"], item["unit_price"], item["unit_cost"], create_time)
                                )
                    total_amount += item["unit_price"] * item["quantity"]
                

                status = random.choices(["CANCELED", "COMPLETED"], k=1, weights=[0.1, 0.9])[0]
                completed_at = create_time + timedelta(minutes=random.randint(30, 90))
                canceled_at = create_time + timedelta(minutes=random.randint(1, 2))
                cur.execute("""
                            UPDATE orders 
                            SET total_amount = %s,
                                status = %s,
                                canceled_at = CASE WHEN %s = 'CANCELED' THEN %s ELSE canceled_at END,
                                completed_at = CASE WHEN %s = 'COMPLETED' THEN %s ELSE completed_at END
                            WHERE order_id = %s""",
                            (total_amount, 
                             status,
                             status, canceled_at,
                             status, completed_at,
                             order_id))
                conn.commit()
                print(f"Created order {order_id}.")
            except Exception as e:
                print(f"Failed to create order {order_id}: {e}")

def main():
    pool = ConnectionPool(conninfo=f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
                      min_size=1,
                      max_size=2)
    with pool.connection() as conn:
        products = get_product_data(conn)
    while True:
        time.sleep(5)
        generate_orders(pool, products)

if __name__ == "__main__":
    main()