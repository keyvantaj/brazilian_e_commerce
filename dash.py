import streamlit as st
import altair as alt
from ingest import *

# Streamlit Title
st.title("📈 Monthly Sales Growth - Olist")

conn = create_postgres_connection(db_name='dash')
monthly_df = pd.read_sql('monthly_revenue', conn)
# Convert to Pandas
monthly_pd = monthly_df[["order_month", "monthly_revenue", "mom_growth_percent"]].sort_values("order_month")

print(monthly_pd)

# Format month column
monthly_pd["order_month"] = pd.to_datetime(monthly_pd["order_month"])

# Display line chart with Altair
line_chart = alt.Chart(monthly_pd).mark_line(point=True).encode(
    x=alt.X('order_month:T', title='Month'),
    y=alt.Y('monthly_revenue:Q', title='MoM Growth'),
    tooltip=['order_month:T', 'monthly_revenue:Q']
).properties(
    title="Monthly Sales Growth (%)",
    width=700,
    height=400
).interactive()

st.altair_chart(line_chart, use_container_width=True)