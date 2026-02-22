import os
import psycopg
import random

from datetime import datetime
from faker import Faker


DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "root")
DB_NAME = os.getenv("DB_NAME", "restaurant_db")


def load_data_from_csv(path: str, table: str, conn: psycopg.Connection) -> None:
    with open(path, encoding="utf-8") as f:
        csv_schema = f.readline().strip()
        f.seek(0)
        with conn.cursor() as cur:    
                with cur.copy(f"""
                    COPY {table} ({csv_schema})
                    FROM STDIN
                    WITH (FORMAT csv, HEADER true);
                """) as copy:
                    copy.write(f.read())

def load_data_from_list_dict(list_dict_data, table, conn):
    cols = list(list_dict_data[0].keys())
    col_list = ", ".join(cols)
    ph = ", ".join([f"%({col})s" for col in cols])

    with conn.cursor() as cur:
        cur.executemany(f"""
                        INSERT INTO {table} ({col_list})
                        VALUES ({ph});
                        """,
                        list_dict_data)

def fake_customers(fake: Faker, numbers: int) -> list[dict]:
    guest = {"customer_id": 0, 
             "name": "Khách vãng lai", 
             "birthday": None, 
             "city": None, 
             "phone": None, 
             "customer_segment": None}
    list_customer = [guest]
    for i in range(numbers):
        customer = {
            "customer_id": i+1,
            "name": fake.name(),
            "birthday": fake.date_of_birth(minimum_age=18),
            "city": fake.administrative_unit(),
            "phone": fake.phone_number(),
            "customer_segment": random.choice(["bronze", "silver", "gold"]),
            "registration_date": fake.date_this_decade(),
            "updated_at": datetime.now()
        }
        list_customer.append(customer)

    return list_customer

def fake_branch(fake: Faker) -> list[dict]:
    list_branch = []
    cities = [fake.unique.administrative_unit() for _ in range(63)]
    for i, city in enumerate(cities, start=1):
        branch = {
            "branch_id": i,
            "name": f"Chi nhánh {city}",
            "city": city,
            "address": fake.street_address(),
            "phone": fake.phone_number(),
            "status": "active",
            "created_at": fake.date_this_decade(),
            "updated_at": datetime.now()
        }
        list_branch.append(branch)
    
    return list_branch

def main():
    fake = Faker(locale="vi_VN")
    conn = psycopg.connect(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    list_customers = fake_customers(fake, 100)
    load_data_from_list_dict(list_customers, "customers", conn)

    list_branches = fake_branch(fake)
    load_data_from_list_dict(list_branches, "branches", conn)

    load_data_from_csv("./data/payments.csv", "payments", conn)
    load_data_from_csv("./data/categories.csv", "categories", conn)
    load_data_from_csv("./data/products.csv", "products", conn)
    
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()