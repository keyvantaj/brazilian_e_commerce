import streamlit as st
from ingest import *

st.title("Brazilian E-Commerce Public Dataset by Olist")

@st.cache_data
def load_data(_engine):
    try:
        query = "SELECT * FROM sellers ORDER BY seller_zip_code_prefix LIMIT 100;"
        df_db = pd.read_sql(query, engine)

        return df_db
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return pd.DataFrame()

engine = create_postgres_connection()
df = load_data(engine)

if df.empty:
    st.warning("No data loaded.")
else:
    st.subheader("Daily Trip Summary (first 100 rows)")
    st.dataframe(df)

    if st.checkbox("Show aggregated stats"):
        st.write(df.describe())