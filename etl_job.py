from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, sum as spark_sum, to_date, date_format, lag, countDistinct
from pyspark.sql.types import FloatType
from ingest import *

jdbc_url = "jdbc:postgresql://{}:{}/{}".format(DB_HOST, DB_PORT, DB_NAME)


def extract_from_postgresql(table, conn_properties):
    """Extract data from PostgreSQL"""

    # Read data from a table
    df = spark.read \
        .jdbc(url=jdbc_url,
              table=table,
              properties=conn_properties)

    return df

def total_revenue_aov(df_raw):
    payments_df = df_raw.withColumn(
        "payment_value_clean",
        when(col("payment_value").cast(FloatType()).isNotNull(),
             col("payment_value").cast(FloatType()))
        .otherwise(0.0)
    )
    payments_df.show(10)
    print('****************************************************')
    print('****************************************************')
    print('****************************************************')

    # Calculate total revenue and number of unique orders
    agg_result = payments_df.agg(
        spark_sum("payment_value_clean").alias("total_revenue"),
        countDistinct("order_id").alias("unique_orders")
    ).collect()[0]

    # Compute Average Order Value (AOV)
    total_revenue = agg_result["total_revenue"]
    unique_orders = agg_result["unique_orders"]
    aov = total_revenue / unique_orders if unique_orders > 0 else 0.0
    return total_revenue, aov


def monthly_sales_growth(payments_df, orders_df):
    payments_df = payments_df.withColumn(
        "payment_value_clean",
        when(col("payment_value").cast(FloatType()).isNotNull(),
             col("payment_value").cast(FloatType()))
        .otherwise(0.0)
    )
    # Join on order_id to get timestamp
    payments_with_date = payments_df.join(orders_df, on="order_id", how="inner")
    payments_with_date.show(10)
    # Extract year-month from 'order_purchase_timestamp'
    payments_with_date = payments_with_date.withColumn(
        "order_month",
        date_format(to_date("order_purchase_timestamp"), "yyyy-MM")
    )
    # Aggregate monthly revenue
    monthly_revenue_df = payments_with_date.groupBy("order_month") \
        .agg(spark_sum("payment_value_clean").alias("monthly_revenue")) \
        .orderBy("order_month")

    # Add previous month revenue using a window function
    window_spec = Window.orderBy("order_month")
    monthly_revenue_df = monthly_revenue_df.withColumn(
        "prev_month_revenue",
        lag("monthly_revenue").over(window_spec)
    )

    # Calculate month-over-month growth rate
    monthly_revenue_df = monthly_revenue_df.withColumn(
        "mom_growth_percent",
        when(col("prev_month_revenue").isNotNull() & (col("prev_month_revenue") != 0),
             ((col("monthly_revenue") - col("prev_month_revenue")) / col("prev_month_revenue")) * 100)
        .otherwise(None)
    )

    # Show results
    monthly_revenue_df.select("order_month", "monthly_revenue", "mom_growth_percent").show()

if __name__ == "__main__":
    # Initialize Spark Session with PostgreSQL JDBC driver
    spark = SparkSession.builder. \
        appName("PostgreSQL ETL Job"). \
        config("spark.jars.packages", "org.postgresql:postgresql:42.2.24"). \
        getOrCreate()

    connection_properties = {
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver",
        "fetchsize": "1000"  # Fetch rows in batches
    }

    olist_data = {}
    for key,value in csv_paths.items():
        # Charger les données
        df = extract_from_postgresql(table=key, conn_properties=connection_properties)
        olist_data[key] = df

    total_sum, aov = total_revenue_aov(df_raw=olist_data['order_payments'])
    print(f"Total Revenue: R${total_sum:,.2f}")
    print(f"Average Order Value: R${aov:,.2f}")

    monthly_sales_growth(payments_df=olist_data['order_payments'],
                         orders_df=olist_data['orders'])

