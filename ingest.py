import pandas as pd
import os
import subprocess
from sqlalchemy import create_engine

# Load env vars
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "olist")
DB_USER = os.environ.get("DB_USER", "admin")
DB_PASS = os.environ.get("DB_PASS", "admin")
DB_ADMIN_USER = os.getenv('DB_ADMIN_USER', 'postgres')

csv_paths = {'products': 'data/olist_products_dataset.csv',
             'sellers': 'data/olist_sellers_dataset.csv',
             'orders': 'data/olist_orders_dataset.csv',
             'customers': 'data/olist_customers_dataset.csv',
             'geolocation': 'data/olist_geolocation_dataset.csv',
             'order_items': 'data/olist_order_items_dataset.csv',
             'order_reviews': 'data/olist_order_reviews_dataset.csv',
             'product_categories': 'data/product_category_name_translation.csv',
             'order_payments': 'data/olist_order_payments_dataset.csv'}

def coerce_column_types(df, target_types=None, errors="raise"):
    """
    Coerce DataFrame columns to specified data types.

    Parameters:
    - df: pandas.DataFrame
    - target_types: dict (e.g. {'col1': 'str', 'col2': 'float'})
                    If None, will use df.dtypes
    - errors: 'raise', 'coerce', or 'ignore' (passed to pd.to_numeric or astype)

    Returns:
    - Converted DataFrame
    """
    df = df.copy()

    if target_types is None:
        print("Inferred types:")
        print(df.dtypes)
        return df  # Return as-is with printed types

    for col, dtype in target_types.items():
        if dtype in ['int', 'float', 'str']:
            try:
                if dtype == 'str':
                    df[col] = df[col].astype(str)
                elif dtype in ['int', 'float']:
                    df[col] = pd.to_numeric(df[col], errors=errors)
                print(f"✔ Converted '{col}' to {dtype}")
            except Exception as e:
                print(f"✘ Failed to convert '{col}' to {dtype}: {e}")
        else:
            try:
                df[col] = df[col].astype(dtype, errors=errors)
                print(f"✔ Converted '{col}' to {dtype}")
            except Exception as e:
                print(f"✘ Failed to convert '{col}' to {dtype}: {e}")

    return df

def create_db_with_psql():
    try:
        # Using psql command-line
        subprocess.run([
            'psql',
            '-U', DB_ADMIN_USER,
            '-h', DB_HOST,
            '-p', DB_PORT,
            '-c', 'CREATE DATABASE olist;'
        ], check=True)
        print("Database created successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error creating database: {e}")
        return False

def create_pg_user_subprocess():
    """Creates user using psql command-line"""
    try:
        cmd = [
            'psql',
            '-U', 'postgres',
            '-h', 'localhost',
            '-c', f"CREATE USER {DB_USER} WITH PASSWORD '{DB_PASS}';"
        ]
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"User {DB_USER} created successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error creating user: {e.stderr}")
        return False

def start_postgres_service():
    # Replace with your actual sudo password (not recommended in production)
    sudo_password = "Dbifrjyn13104*"

    command = "systemctl start postgresql"

    try:
        result = subprocess.run(
            ['sudo', '-S'] + command.split(),
            input=sudo_password + '\n',
            text=True,
            capture_output=True,
            check=True
        )
        print("PostgreSQL started successfully.")
    except subprocess.CalledProcessError as e:
        print("Error:", e.stderr)

def load_data(file_path):
    """Charge les données des vendeurs depuis le fichier CSV"""
    try:
        # Lecture du fichier CSV
        df = pd.read_csv(file_path,  low_memory=False, sep=",", quotechar='"')

        # Nettoyage des données
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        df = df.replace('', None)  # Remplacer les chaînes vides par NULL
        df = df.loc[:, df.isnull().mean() < 0.8]
        df.drop_duplicates(keep='first')

        return df

    except Exception as e:
        print(f"Erreur lors du chargement des données: {e}")
        return None

def create_postgres_connection():
    """Crée une connexion à la base de données PostgreSQL"""
    try:
        engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME))
        return engine
    except Exception as e:
        print(f"Erreur de connexion à PostgreSQL: {e}")
        return None

def copy_data_to_postgres(df, table_name, engine):
    """Insère les données dans PostgreSQL en utilisant copy_from pour une meilleure performance"""
    try:
        df.to_sql(table_name, engine, index=False, if_exists="append")  # or append
        print(f"{len(df)} copiés avec succès.")
    except Exception as e:
        print(f"Erreur lors de copie des données: {e}")


def compare_csv_to_postgresql(engine, df, table_name):

    # 2. Load database data
    df_db = pd.read_sql(table_name, engine)
    # Ensure columns are in the same order
    df_db = df_db[df.columns]

    # Identify rows in df2 that are not in df1 (i.e., newly added to df2)
    removed_from_df_csv = df_db.merge(df, how='left', indicator=True)
    removed_from_df_csv = removed_from_df_csv[removed_from_df_csv['_merge'] == 'left_only'].drop('_merge', axis=1)

    # Similarly, if you want rows in df1 that are not in df2 (deleted from df2)
    # added_to_df_csv = df.merge(df_db, how='left', indicator=True)
    # added_to_df_csv = added_to_df_csv[added_to_df_csv['_merge'] == 'left_only'].drop('_merge', axis=1)

    # print('added_to_df_csv', added_to_df_csv)
    print('removed_from_df_csv', removed_from_df_csv)
    print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
    print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')

    if not removed_from_df_csv.empty:

        df.to_sql(table_name, engine, index=False, if_exists="replace")  # or append


def main():
    start_postgres_service()
    create_pg_user_subprocess()
    create_db_with_psql()

    # Se connecter à PostgreSQL
    conn = create_postgres_connection()
    if conn is None:
        return

    for key,value in csv_paths.items():
    # Charger les données
        df = load_data(value)
        print(key)
        print(df)
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

        if df is None:
            return

        try:
            # Créer la table si elle n'existe pas
            # value[1](conn)
            print('-----------------------------------------------')
            print('-----------------------------------------------')

            # Copier les données
            copy_data_to_postgres(engine=conn, df=df, table_name=key)
            compare_csv_to_postgresql(engine=conn, df=df, table_name=key)

        except Exception as e:
            print(e)

if __name__ == "__main__":
    main()