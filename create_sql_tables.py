from sqlalchemy import create_engine, text

def create_products_table_if_not_exists(conn):
    """Creates the products table if it doesn't exist"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS products (
        product_id VARCHAR(255) PRIMARY KEY,
        product_category_name VARCHAR(255),
        product_name_lenght FLOAT,
        product_description_lenght FLOAT,
        product_photos_qty FLOAT,
        product_weight_g FLOAT,
        product_length_cm FLOAT,
        product_height_cm FLOAT,
        product_width_cm FLOAT
    )
    """

    # Execute the query
    with conn.begin() as conn:  # begin() handles commit automatically
        conn.execute(text(create_table_query))

def create_sellers_table_if_not_exists(conn):
    """Crée la table si elle n'existe pas"""
    create_table_query = """
        CREATE TABLE IF NOT EXISTS sellers (
            seller_id VARCHAR(255) PRIMARY KEY,
            seller_zip_code_prefix INT,
            seller_city VARCHAR(255),
            seller_state VARCHAR(2)
        )
    """

    # Execute the query
    with conn.begin() as conn:  # begin() handles commit automatically
        conn.execute(text(create_table_query))

def create_orders_table_if_not_exists(conn):

    create_table_query = """
        CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50),
            order_status VARCHAR(20),
            order_purchase_timestamp VARCHAR(20),
            order_approved_at VARCHAR(20),
            order_delivered_carrier_date VARCHAR(20),
            order_delivered_customer_date VARCHAR(20),
            order_estimated_delivery_date VARCHAR(20)
        );
    """

    # Execute the query
    with conn.begin() as conn:  # begin() handles commit automatically
        conn.execute(text(create_table_query))

def create_customers_table_if_not_exists(conn):

    create_table_query = """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            customer_unique_id VARCHAR(50),
            customer_zip_code_prefix INT,
            customer_city VARCHAR(255),
            customer_state VARCHAR(2)
        );
    """

    # Execute the query
    with conn.begin() as conn:  # begin() handles commit automatically
        conn.execute(text(create_table_query))

def create_order_payments_table_if_not_exists(conn):

    create_table_query = """
        CREATE TABLE IF NOT EXISTS order_payments (
            order_id VARCHAR(255),
            payment_sequential INT,
            payment_type VARCHAR(50),
            payment_installments INT,
            payment_value DECIMAL(10,2),
            PRIMARY KEY (order_id, payment_sequential)
        );
    """

    # Execute the query
    with conn.begin() as conn:  # begin() handles commit automatically
        conn.execute(text(create_table_query))

def create_geolocation_table_if_not_exists(conn):
    create_table_query = """
        CREATE TABLE IF NOT EXISTS geolocation (
            geolocation_zip_code_prefix INT,
            geolocation_lat FLOAT,
            geolocation_lng FLOAT,
            geolocation_city VARCHAR(255),
            geolocation_state VARCHAR(2)
        );
    """

    # Execute the query
    with conn.begin() as conn:  # begin() handles commit automatically
        conn.execute(text(create_table_query))

def create_order_items_if_not_exists(conn):
    create_table_query = """
        CREATE TABLE IF NOT EXISTS order_items (
            order_id VARCHAR(255),
            order_item_id INT,
            product_id VARCHAR(255),
            seller_id VARCHAR(255),
            shipping_limit_date VARCHAR(255),
            price FLOAT,
            freight_value FLOAT
        );
    """

    # Execute the query
    with conn.begin() as conn:  # begin() handles commit automatically
        conn.execute(text(create_table_query))

def create_order_reviews_if_not_exists(conn):
    create_table_query = """
        CREATE TABLE IF NOT EXISTS order_reviews (
            review_id VARCHAR(255),
            order_id VARCHAR(255),
            review_score INT,
            review_comment_title TEXT,
            review_comment_message TEXT,
            review_creation_date VARCHAR(255),
            review_answer_timestamp VARCHAR(255)
        );
    """

    # Execute the query
    with conn.begin() as conn:  # begin() handles commit automatically
        conn.execute(text(create_table_query))

def create_product_categories_if_not_exists(conn):

    create_table_query = """
        CREATE TABLE IF NOT EXISTS product_categories (
            product_category_name VARCHAR(255),
            product_category_name_english VARCHAR(255)
        );
    """

    # Execute the query
    with conn.begin() as conn:  # begin() handles commit automatically
        conn.execute(text(create_table_query))