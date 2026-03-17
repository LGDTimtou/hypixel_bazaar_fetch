import json
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values


SLOTS = [
	"A1", "A2", "A3",
	"B1", "B2", "B3",
	"C1", "C2", "C3",
]


def connect_database():
	script_dir = Path(__file__).resolve().parent
	load_dotenv(script_dir / ".env")
	database_url = os.getenv("DATABASE_URL")

	if not database_url:
		raise RuntimeError("DATABASE_URL is not set in .env")

	conn = psycopg2.connect(database_url, connect_timeout=10)
	print("Successfully connected to the database.")
	return conn


def create_tables(conn):
	cursor = conn.cursor()

	cursor.execute(
		"""
		CREATE TABLE IF NOT EXISTS products (
			product_id TEXT PRIMARY KEY
		)
		"""
	)

	cursor.execute(
		"""
		CREATE TABLE IF NOT EXISTS item_recipes (
			product_id TEXT PRIMARY KEY REFERENCES products(product_id),
			output_amount DOUBLE PRECISION,
			a1 TEXT,
			a2 TEXT,
			a3 TEXT,
			b1 TEXT,
			b2 TEXT,
			b3 TEXT,
			c1 TEXT,
			c2 TEXT,
			c3 TEXT,
			a1_quantity INTEGER,
			a2_quantity INTEGER,
			a3_quantity INTEGER,
			b1_quantity INTEGER,
			b2_quantity INTEGER,
			b3_quantity INTEGER,
			c1_quantity INTEGER,
			c2_quantity INTEGER,
			c3_quantity INTEGER
		)
		"""
	)

	cursor.execute(
		"""
		ALTER TABLE item_recipes
		ADD COLUMN IF NOT EXISTS output_amount DOUBLE PRECISION
		"""
	)

	conn.commit()
	cursor.close()


def parse_ingredient(raw_value):
	if not raw_value:
		return None, None

	value = str(raw_value).strip()
	if not value:
		return None, None

	if ":" not in value:
		return value, 1

	ingredient, maybe_quantity = value.rsplit(":", 1)
	try:
		quantity = int(maybe_quantity)
		return ingredient, quantity
	except ValueError:
		return value, 1


def has_grid_slots(recipe_obj):
	if not isinstance(recipe_obj, dict):
		return False
	return any(slot in recipe_obj for slot in SLOTS)


def extract_grid_recipe(item_data):
	top_level_recipe = item_data.get("recipe")
	if has_grid_slots(top_level_recipe):
		output_amount = top_level_recipe.get("count")
		if output_amount is None:
			output_amount = item_data.get("count", 1)
		return top_level_recipe, output_amount

	recipes = item_data.get("recipes", [])
	if isinstance(recipes, list):
		for recipe_entry in recipes:
			if has_grid_slots(recipe_entry):
				output_amount = recipe_entry.get("count", 1)
				return recipe_entry, output_amount

	return None, None


def normalize_output_amount(raw_value):
	if raw_value is None:
		return 1.0
	try:
		return float(raw_value)
	except (TypeError, ValueError):
		return 1.0


def build_recipe_row(product_id, recipe_obj, output_amount):
	values = {
		"product_id": product_id,
		"output_amount": normalize_output_amount(output_amount),
	}

	for slot in SLOTS:
		ingredient, quantity = parse_ingredient(recipe_obj.get(slot, ""))
		values[slot.lower()] = ingredient
		values[f"{slot.lower()}_quantity"] = quantity

	return (
		values["product_id"],
		values["output_amount"],
		values["a1"],
		values["a2"],
		values["a3"],
		values["b1"],
		values["b2"],
		values["b3"],
		values["c1"],
		values["c2"],
		values["c3"],
		values["a1_quantity"],
		values["a2_quantity"],
		values["a3_quantity"],
		values["b1_quantity"],
		values["b2_quantity"],
		values["b3_quantity"],
		values["c1_quantity"],
		values["c2_quantity"],
		values["c3_quantity"],
	)


def collect_recipe_rows(items_dir):
	rows = []
	json_files = sorted(items_dir.glob("*.json"))

	for file_path in json_files:
		try:
			with open(file_path, "r", encoding="utf-8") as handle:
				item_data = json.load(handle)
		except (json.JSONDecodeError, OSError):
			continue

		recipe_obj, output_amount = extract_grid_recipe(item_data)
		if not recipe_obj:
			continue

		product_id = file_path.stem
		rows.append(build_recipe_row(product_id, recipe_obj, output_amount))

	return rows


def store_recipes(conn, rows):
	if not rows:
		print("No grid recipes found to store.")
		return

	cursor = conn.cursor()

	product_rows = [(row[0],) for row in rows]
	execute_values(
		cursor,
		"INSERT INTO products(product_id) VALUES %s ON CONFLICT (product_id) DO NOTHING",
		product_rows,
	)

	execute_values(
		cursor,
		"""
		INSERT INTO item_recipes (
			product_id, output_amount,
			a1, a2, a3, b1, b2, b3, c1, c2, c3,
			a1_quantity, a2_quantity, a3_quantity,
			b1_quantity, b2_quantity, b3_quantity,
			c1_quantity, c2_quantity, c3_quantity
		)
		VALUES %s
		ON CONFLICT (product_id) DO UPDATE SET
			output_amount = EXCLUDED.output_amount,
			a1 = EXCLUDED.a1,
			a2 = EXCLUDED.a2,
			a3 = EXCLUDED.a3,
			b1 = EXCLUDED.b1,
			b2 = EXCLUDED.b2,
			b3 = EXCLUDED.b3,
			c1 = EXCLUDED.c1,
			c2 = EXCLUDED.c2,
			c3 = EXCLUDED.c3,
			a1_quantity = EXCLUDED.a1_quantity,
			a2_quantity = EXCLUDED.a2_quantity,
			a3_quantity = EXCLUDED.a3_quantity,
			b1_quantity = EXCLUDED.b1_quantity,
			b2_quantity = EXCLUDED.b2_quantity,
			b3_quantity = EXCLUDED.b3_quantity,
			c1_quantity = EXCLUDED.c1_quantity,
			c2_quantity = EXCLUDED.c2_quantity,
			c3_quantity = EXCLUDED.c3_quantity
		""",
		rows,
	)

	conn.commit()
	cursor.close()
	print(f"Stored {len(rows)} recipe rows.")


def main():
	script_dir = Path(__file__).resolve().parent
	items_dir = script_dir.parent / "NotEnoughUpdates-REPO" / "items"

	if not items_dir.exists():
		raise FileNotFoundError(f"Items folder not found: {items_dir}")

	rows = collect_recipe_rows(items_dir)

	conn = connect_database()
	try:
		create_tables(conn)
		store_recipes(conn, rows)
	finally:
		conn.close()


if __name__ == "__main__":
	main()
