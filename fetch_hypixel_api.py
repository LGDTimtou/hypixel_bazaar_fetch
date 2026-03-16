# Load libraries
import os
import time
import psycopg2
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone
from psycopg2.extras import execute_values



def fetch_data():
    url = "https://api.hypixel.net/skyblock/bazaar"
    response = requests.get(url)
    data = response.json()
    return data["lastUpdated"], data["products"]


def connect_database():
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")

    while True:
        try:
            conn = psycopg2.connect(database_url, connect_timeout=10)
            print("Successfully connected to the database.")
            return conn
        except psycopg2.OperationalError as error:
            print(
                f"DB connection attempt failed. "
                f"error: {error}. Retrying..."
            )


def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id TEXT PRIMARY KEY
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS snapshots (
        id BIGSERIAL PRIMARY KEY,
        collected_at TIMESTAMPTZ NOT NULL
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bazaar_prices (
        snapshot_id BIGINT NOT NULL,
        product_id TEXT NOT NULL,
        buy_price DOUBLE PRECISION NOT NULL,
        sell_price DOUBLE PRECISION NOT NULL,
        buy_volume BIGINT NOT NULL,
        sell_volume BIGINT NOT NULL,
        buy_moving_week BIGINT NOT NULL,
        sell_moving_week BIGINT NOT NULL,
        PRIMARY KEY (snapshot_id, product_id),
        FOREIGN KEY (snapshot_id) REFERENCES snapshots(id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    )
    """)

    conn.commit()
    cursor.close()


def store_snapshot(conn, products, timestamp):
    cursor = conn.cursor()

    # Create snapshot
    cursor.execute("""
    INSERT INTO snapshots (collected_at)
    VALUES (%s)
    RETURNING id
    """, (timestamp,))
    snapshot_id = cursor.fetchone()[0]

    # Bulk ensure products exist
    product_rows = [(product_id,) for product_id in products.keys()]
    execute_values(
        cursor,
        "INSERT INTO products(product_id) VALUES %s ON CONFLICT (product_id) DO NOTHING",
        product_rows,
    )

    # Bulk insert price data linked to snapshot
    price_rows = []
    for product_id, product_data in products.items():
        quick = product_data["quick_status"]
        price_rows.append((
            snapshot_id,
            product_id,
            quick["buyPrice"],
            quick["sellPrice"],
            quick["buyVolume"],
            quick["sellVolume"],
            quick["buyMovingWeek"],
            quick["sellMovingWeek"],
        ))

    execute_values(
        cursor,
        """
        INSERT INTO bazaar_prices
        (snapshot_id, product_id, buy_price, sell_price,
         buy_volume, sell_volume, buy_moving_week, sell_moving_week)
        VALUES %s
        """,
        price_rows,
    )

    conn.commit()
    cursor.close()


def main():
    timestamp_ms, products = fetch_data()
    timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)

    conn = connect_database()
    create_tables(conn)
    store_snapshot(conn, products, timestamp)
    conn.close()
    print(f"Snapshot stored at {timestamp} (UTC)")


if __name__ == "__main__":
    main()