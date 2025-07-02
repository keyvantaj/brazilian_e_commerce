import streamlit as st
import altair as alt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum, to_date, date_format, lag
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
from ingest import *

# Streamlit Title
st.title("📈 Monthly Sales Growth - Olist")

# Spark session
spark = SparkSession.builder.appName("Olist_Monthly_Growth").getOrCreate()

# JDBC config
jdbc_url = "jdbc:postgresql://{}:{}/{}".format(DB_HOST, DB_PORT, DB_NAME)
props = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver"
}

# Load payments and orders
payments_df = spark.read.jdbc(url=jdbc_url, table="olist_order_payments_dataset", properties=props)
orders_df = spark.read.jdbc(url=jdbc_url, table="olist_orders_dataset", properties=props)

# Clean 'payment_value'
payments_df = payments_df.withColumn(
    "payment_value_clean",
    when(col("payment_value").cast(FloatType()).isNotNull(), col("payment_value").cast(FloatType()))
    .otherwise(0.0)
)

# Join with order dates
payments_with_date = payments_df.join(orders_df, on="order_id", how="inner")

# Extract year-month
payments_with_date = payments_with_date.withColumn(
    "order_month",
    date_format(to_date("order_purchase_timestamp"), "yyyy-MM")
)

# Monthly revenue
monthly_df = payments_with_date.groupBy("order_month") \
    .agg(spark_sum("payment_value_clean").alias("monthly_revenue")) \
    .orderBy("order_month")

# MoM growth %
window_spec = Window.orderBy("order_month")
monthly_df = monthly_df.withColumn("prev_month_revenue", lag("monthly_revenue").over(window_spec))
monthly_df = monthly_df.withColumn(
    "mom_growth_percent",
    when(
        col("prev_month_revenue").isNotNull() & (col("prev_month_revenue") != 0),
        ((col("monthly_revenue") - col("prev_month_revenue")) / col("prev_month_revenue")) * 100
    ).otherwise(None)
)

# Convert to Pandas
monthly_pd = monthly_df.select("order_month", "monthly_revenue", "mom_growth_percent") \
    .toPandas().sort_values("order_month")

# Format month column
monthly_pd["order_month"] = pd.to_datetime(monthly_pd["order_month"])

# Display line chart with Altair
line_chart = alt.Chart(monthly_pd).mark_line(point=True).encode(
    x=alt.X('order_month:T', title='Month'),
    y=alt.Y('mom_growth_percent:Q', title='MoM Growth (%)'),
    tooltip=['order_month:T', 'mom_growth_percent:Q']
).properties(
    title="Monthly Sales Growth (%)",
    width=700,
    height=400
).interactive()

st.altair_chart(line_chart, use_container_width=True)