import pandas as pd
import os
from sqlalchemy import create_engine, text
import sys
import warnings
from sqlalchemy.exc import SADeprecationWarning

warnings.filterwarnings("ignore", category=SADeprecationWarning)
# Load env vars
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "olist")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "admin")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATA_DIR = os.path.join(BASE_DIR, 'data')

CSV_PATHS = {
    'products': os.path.join(DATA_DIR, 'olist_products_dataset.csv'),
    'sellers': os.path.join(DATA_DIR, 'olist_sellers_dataset.csv'),
    'orders': os.path.join(DATA_DIR, 'olist_orders_dataset.csv'),
    'customers': os.path.join(DATA_DIR, 'olist_customers_dataset.csv'),
    'order_items': os.path.join(DATA_DIR, 'olist_order_items_dataset.csv'),
    'order_reviews': os.path.join(DATA_DIR, 'olist_order_reviews_dataset.csv'),
    'product_categories': os.path.join(DATA_DIR, 'product_category_name_translation.csv'),
    'order_payments': os.path.join(DATA_DIR, 'olist_order_payments_dataset.csv'),
}

def load_data(file_path):
    try:
        df = pd.read_csv(file_path, low_memory=False)
        df = df.drop_duplicates(keep='first')
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        df = df.replace('', None)
        df = df.loc[:, df.isnull().mean() < 0.8]
        return df
    except Exception as e:
        print(f"[ERROR] Failed to load {file_path}: {e}")
        return None

def create_postgres_connection():
    try:
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        return engine
    except Exception as e:
        print(f"[ERROR] PostgreSQL connection failed: {e}")
        sys.exit(1)

def ensure_db_exists(engine, db_name):
    try:
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :dbname"),
                {"dbname": db_name}
            ).fetchone()
            if not exists:
                conn.execution_options(isolation_level="AUTOCOMMIT").execute(text(f"CREATE DATABASE {db_name}"))
                print(f"[INFO] Created database: {db_name}")
    except Exception as e:
        print(f"[ERROR] Could not check or create DB '{db_name}': {e}")

def copy_data_to_postgres(df, table_name, engine, key_id):
    try:
        with engine.connect() as conn:
            # Load existing keys
            existing_ids = pd.read_sql(f"SELECT DISTINCT {key_id} FROM {table_name}", conn)
            df = df[~df[key_id].isin(existing_ids[key_id])]

        if df.empty:
            print(f"[INFO] No new rows for {table_name}")
        else:
            df.to_sql(table_name, engine, index=False, if_exists="append")
            print(f"[INFO] Inserted {len(df)} new rows into {table_name}")
    except Exception as e:
        print(f"[ERROR] Failed to insert into {table_name}: {e}")

    print("[INFO] Ingestion job completed.")

def main():
    print("[INFO] Starting ingestion job...")

    engine = create_postgres_connection()
    ensure_db_exists(engine, DB_NAME)

    for table, path in CSV_PATHS.items():
        df = load_data(path)
        if table == "products":
            if df is not None:
                copy_data_to_postgres(df=df, table_name=table, engine=engine, key_id='product_id')
            else:
                print(f"[WARN] Skipping table {table} due to load failure.")

        if table == "sellers":
            if df is not None:
                copy_data_to_postgres(df=df, table_name=table, engine=engine, key_id='seller_id')
            else:
                print(f"[WARN] Skipping table {table} due to load failure.")

        if table == "orders":
            if df is not None:
                copy_data_to_postgres(df=df, table_name=table, engine=engine, key_id='order_id')
            else:
                print(f"[WARN] Skipping table {table} due to load failure.")

        if table == "customers":
            if df is not None:
                copy_data_to_postgres(df=df, table_name=table, engine=engine, key_id='customer_id')
            else:
                print(f"[WARN] Skipping table {table} due to load failure.")

        if table == "order_items":
            if df is not None:
                copy_data_to_postgres(df=df, table_name=table, engine=engine, key_id='order_id')
            else:
                print(f"[WARN] Skipping table {table} due to load failure.")

        if table == "order_reviews":
            if df is not None:
                copy_data_to_postgres(df=df, table_name=table, engine=engine, key_id='review_id')
            else:
                print(f"[WARN] Skipping table {table} due to load failure.")

        if table == "order_payments":
            if df is not None:
                copy_data_to_postgres(df=df, table_name=table, engine=engine, key_id='order_id')
            else:
                print(f"[WARN] Skipping table {table} due to load failure.")

if __name__ == "__main__":
    main()
