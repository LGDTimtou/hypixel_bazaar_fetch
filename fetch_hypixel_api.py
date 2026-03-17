# Load libraries
import os
import psycopg2
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone
from psycopg2.extras import execute_values


EXPECTED_CANDIDATES = 5



def fetch_data():
    url = "https://api.hypixel.net/skyblock/bazaar"
    response = requests.get(url)
    data = response.json()
    return data["lastUpdated"], data["products"]


def fetch_mayor_data():
    url = "https://api.hypixel.net/v2/resources/skyblock/election"
    response = requests.get(url)
    data = response.json()

    if not data.get("success"):
        raise RuntimeError("Failed to fetch mayor election data from Hypixel API")

    return data.get("mayor", {})


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
        collected_at TIMESTAMPTZ NOT NULL,
        skyblock_year INTEGER
    )
    """)

    cursor.execute("""
    ALTER TABLE snapshots
    ADD COLUMN IF NOT EXISTS skyblock_year INTEGER
    """)

    cursor.execute("""
    UPDATE snapshots
    SET skyblock_year = 478
    WHERE skyblock_year IS NULL
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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS mayors (
        name TEXT PRIMARY KEY
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS election (
        year INTEGER PRIMARY KEY,
        candidate_1 TEXT,
        candidate_1_votes BIGINT,
        candidate_2 TEXT,
        candidate_2_votes BIGINT,
        candidate_3 TEXT,
        candidate_3_votes BIGINT,
        candidate_4 TEXT,
        candidate_4_votes BIGINT,
        candidate_5 TEXT,
        candidate_5_votes BIGINT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS perks_election (
        election_year INTEGER NOT NULL,
        perk_name TEXT NOT NULL,
        mayor TEXT NOT NULL,
        description TEXT NOT NULL,
        minister BOOLEAN NOT NULL,
        PRIMARY KEY (election_year, perk_name, mayor),
        FOREIGN KEY (election_year) REFERENCES election(year),
        FOREIGN KEY (mayor) REFERENCES mayors(name)
    )
    """)

    conn.commit()
    cursor.close()


def store_snapshot(conn, products, timestamp, skyblock_year):
    cursor = conn.cursor()

    # Create snapshot
    cursor.execute("""
    INSERT INTO snapshots (collected_at, skyblock_year)
    VALUES (%s, %s)
    RETURNING id
    """, (timestamp, skyblock_year))
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


def store_mayor_snapshot(conn, mayor_data):
    election_data = mayor_data.get("election") or {}
    candidates = election_data.get("candidates") or []
    year = election_data.get("year")

    if year is None or not candidates:
        print("No election candidate data found; skipping mayor snapshot storage.")
        return

    cursor = conn.cursor()

    if len(candidates) != EXPECTED_CANDIDATES:
        print(
            f"Expected {EXPECTED_CANDIDATES} election candidates, got {len(candidates)}; "
            "skipping mayor snapshot storage."
        )
        cursor.close()
        return

    normalized_candidates = []
    mayor_names = set()
    perks_election_rows = set()

    for candidate in candidates:
        mayor_name = candidate.get("name")
        votes = candidate.get("votes")

        if mayor_name is None or votes is None:
            continue

        normalized_candidates.append((mayor_name, votes))
        mayor_names.add(mayor_name)

        for perk in candidate.get("perks") or []:
            perk_name = perk.get("name")
            perk_description = perk.get("description", "")
            minister = bool(perk.get("minister", False))

            if not perk_name:
                continue

            perks_election_rows.add((year, perk_name, mayor_name, perk_description, minister))

    if not normalized_candidates:
        print("No valid election candidate rows found; skipping mayor snapshot storage.")
        cursor.close()
        return

    mayor_rows = [(name,) for name in sorted(mayor_names)]
    if mayor_rows:
        execute_values(
            cursor,
            """
            INSERT INTO mayors (name)
            VALUES %s
            ON CONFLICT (name) DO NOTHING
            """,
            mayor_rows,
        )

    candidate_payload = []
    for mayor_name, votes in normalized_candidates:
        candidate_payload.extend([
            mayor_name,
            votes,
        ])

    cursor.execute(
        """
        INSERT INTO election (
            year,
            candidate_1, candidate_1_votes,
            candidate_2, candidate_2_votes,
            candidate_3, candidate_3_votes,
            candidate_4, candidate_4_votes,
            candidate_5, candidate_5_votes
        )
        VALUES (
            %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s
        )
        ON CONFLICT (year) DO NOTHING
        """,
        [year, *candidate_payload],
    )

    inserted_new_year = cursor.rowcount == 1

    if inserted_new_year and perks_election_rows:
        execute_values(
            cursor,
            """
            INSERT INTO perks_election (election_year, perk_name, mayor, description, minister)
            VALUES %s
            ON CONFLICT (election_year, perk_name, mayor) DO UPDATE SET
                description = EXCLUDED.description,
                minister = EXCLUDED.minister
            """,
            list(perks_election_rows),
        )

    conn.commit()
    cursor.close()


def main():
    timestamp_ms, products = fetch_data()
    timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)

    mayor_data = fetch_mayor_data()
    skyblock_year = (mayor_data.get("election") or {}).get("year")

    conn = connect_database()
    create_tables(conn)
    store_snapshot(conn, products, timestamp, skyblock_year)
    store_mayor_snapshot(conn, mayor_data)
    conn.close()
    print(f"Snapshot stored at {timestamp} (UTC)")
    print(f"Mayor election snapshot stored for SkyBlock year {skyblock_year}")


if __name__ == "__main__":
    main()