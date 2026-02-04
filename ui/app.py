import os
import pandas as pd
import psycopg2
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="NYC 311 Dashboard", layout="wide")

# ---- DB CONFIG (use env vars) ----
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "airflow")
PG_PASSWORD = os.getenv("PG_PASSWORD", "airflow")


@st.cache_resource
def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def read_df(query: str, params=None) -> pd.DataFrame:
    conn = get_conn()
    return pd.read_sql_query(query, conn, params=params)


st.title("NYC 311 Daily Pipeline Dashboard")

# ---- Sidebar filters ----
st.sidebar.header("Filters")

days_df = read_df("""
    SELECT DISTINCT load_day
    FROM nyc311_daily_borough_complaint
    ORDER BY load_day DESC;
""")

if days_df.empty:
    st.error("No data found in Postgres. Run/backfill the Airflow DAG first.")
    st.stop()

available_days = days_df["load_day"].astype(str).tolist()
selected_day = st.sidebar.selectbox("Select day", available_days, index=0)

boroughs_df = read_df("""
    SELECT DISTINCT borough
    FROM nyc311_daily_borough_complaint
    WHERE load_day = %s
    ORDER BY borough;
""", params=(selected_day,))

available_boroughs = boroughs_df["borough"].tolist()
selected_borough = st.sidebar.selectbox("Select borough", ["ALL"] + available_boroughs, index=0)

top_n = st.sidebar.slider("Top N complaint types", min_value=5, max_value=30, value=10)

# ---- KPIs row ----
kpi = read_df("""
    SELECT
      %s::date AS load_day,
      SUM(count) AS total_complaints,
      COUNT(*) AS borough_complaint_combos
    FROM nyc311_daily_borough_complaint
    WHERE load_day = %s;
""", params=(selected_day, selected_day))

k1, k2, k3 = st.columns(3)
k1.metric("Selected day", selected_day)
k2.metric("Total complaints", int(kpi["total_complaints"].iloc[0]))
k3.metric("Unique (borough × type) combos", int(kpi["borough_complaint_combos"].iloc[0]))

st.divider()

# ---- Trend chart: daily totals ----
trend_df = read_df("""
    SELECT load_day, SUM(count) AS total_complaints
    FROM nyc311_daily_borough_complaint
    GROUP BY load_day
    ORDER BY load_day;
""")

fig_trend = px.line(trend_df, x="load_day", y="total_complaints", title="Daily Total Complaints")
st.plotly_chart(fig_trend, use_container_width=True)

# ---- Top complaints for selected day ----
st.subheader("Top Complaint Types")

if selected_borough == "ALL":
    top_df = read_df("""
        SELECT complaint_type, SUM(count) AS total_count
        FROM nyc311_daily_borough_complaint
        WHERE load_day = %s
        GROUP BY complaint_type
        ORDER BY total_count DESC
        LIMIT %s;
    """, params=(selected_day, top_n))
else:
    top_df = read_df("""
        SELECT complaint_type, count AS total_count
        FROM nyc311_daily_borough_complaint
        WHERE load_day = %s AND borough = %s
        ORDER BY total_count DESC
        LIMIT %s;
    """, params=(selected_day, selected_borough, top_n))

fig_top = px.bar(top_df, x="total_count", y="complaint_type", orientation="h", title="Top complaints")
st.plotly_chart(fig_top, use_container_width=True)

# ---- Spikes table ----
st.subheader("Spikes (vs previous day)")

spikes_df = read_df("""
    SELECT borough, complaint_type, count_today, count_yesterday, abs_change,
           (COALESCE(pct_change, 0) * 100) AS pct_change_percent
    FROM nyc311_spikes
    WHERE load_day = %s
    ORDER BY abs_change DESC
    LIMIT 50;
""", params=(selected_day,))

if not spikes_df.empty:
    spikes_df["pct_change_percent"] = spikes_df["pct_change_percent"].round(1)

if spikes_df.empty:
    st.info("No spikes stored for this day (either none met threshold, or you don't have the previous day loaded yet).")
else:
    st.dataframe(spikes_df, use_container_width=True)

# ---- Borough leaderboard ----
st.subheader("Borough totals for selected day")

borough_totals = read_df("""
    SELECT borough, SUM(count) AS total_complaints
    FROM nyc311_daily_borough_complaint
    WHERE load_day = %s
    GROUP BY borough
    ORDER BY total_complaints DESC;
""", params=(selected_day,))

fig_b = px.bar(borough_totals, x="borough", y="total_complaints", title="Total complaints by borough")
st.plotly_chart(fig_b, use_container_width=True)