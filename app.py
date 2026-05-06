"""Streamlit dashboard -- reads Parquet output for deployment on Streamlit Cloud."""
import duckdb, streamlit as st, pandas as pd
from pathlib import Path

st.set_page_config(page_title="Mal Payments Platform", layout="wide", page_icon="💳")
st.title("💳 Mal Unified Payment Data Platform")

PARQUET = Path(__file__).parent / "data" / "output" / "payment_events.parquet"
if not PARQUET.exists():
    st.warning("No data yet — run the pipeline first to generate the Parquet file.")
    st.stop()

@st.cache_resource
def load_data():
    con = duckdb.connect()
    con.execute(f"CREATE VIEW events AS SELECT *, CAST(event_timestamp AS DATE) AS event_date, "
                f"CAST(amount AS DOUBLE) AS amt FROM read_parquet('{PARQUET}')")
    return con

con = load_data()
all_df = con.execute("SELECT * FROM events").fetchdf()

# ── Sidebar Filters ──
st.sidebar.header("🔍 Filters")
sources = st.sidebar.multiselect("Source System", all_df["source_system"].unique(), default=all_df["source_system"].unique())
types = st.sidebar.multiselect("Payment Type", all_df["payment_type"].unique(), default=all_df["payment_type"].unique())
statuses = st.sidebar.multiselect("Status", all_df["status"].unique(), default=all_df["status"].unique())
currencies = st.sidebar.multiselect("Currency", all_df["currency"].unique(), default=all_df["currency"].unique())
min_d, max_d = all_df["event_date"].min(), all_df["event_date"].max()
date_range = st.sidebar.date_input("Date Range", value=(min_d, max_d), min_value=min_d, max_value=max_d)
amt_min, amt_max = float(all_df["amt"].min()), float(all_df["amt"].max())
amt_range = st.sidebar.slider("Amount Range", amt_min, amt_max, (amt_min, amt_max), step=10.0)

d_start, d_end = (date_range[0], date_range[1]) if len(date_range) == 2 else (date_range[0], date_range[0])
where = (f"source_system IN ({','.join(repr(s) for s in sources)}) "
         f"AND payment_type IN ({','.join(repr(t) for t in types)}) "
         f"AND status IN ({','.join(repr(s) for s in statuses)}) "
         f"AND currency IN ({','.join(repr(c) for c in currencies)}) "
         f"AND event_date BETWEEN '{d_start}' AND '{d_end}' "
         f"AND amt BETWEEN {amt_range[0]} AND {amt_range[1]}")

def q(sql): return con.execute(sql).fetchdf()
df = q(f"SELECT * FROM events WHERE {where}")
st.sidebar.metric("Filtered Events", f"{len(df):,} / {len(all_df):,}")

# ── KPI Row ──
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Events", f"{len(df):,}")
c2.metric("Completed", f"{(df['status']=='completed').sum():,}")
c3.metric("Failed", f"{(df['status']=='failed').sum():,}")
c4.metric("Pending", f"{(df['status']=='pending').sum():,}")
c5.metric("Total Volume", f"${df['amt'].sum():,.0f}")

# ── Charts Row 1 ──
left, right = st.columns(2)
with left:
    st.subheader("Payment Volume by Type")
    vol = df.groupby("payment_type")["amt"].sum().sort_values(ascending=True)
    st.bar_chart(vol)
with right:
    st.subheader("Daily Transaction Count")
    daily = df.groupby("event_date").size()
    st.line_chart(daily)

# ── Charts Row 2 ──
left2, right2 = st.columns(2)
with left2:
    st.subheader("Status Distribution")
    st.bar_chart(df["status"].value_counts())
with right2:
    st.subheader("Failure Rate by Source (%)")
    fr = df.groupby("source_system").apply(lambda g: round((g["status"] == "failed").mean() * 100, 1))
    st.bar_chart(fr)

# ── Cross-Product Customers ──
st.subheader("🔗 Cross-Product Customers (active in 2+ products)")
cross = (df[df["status"] == "completed"].groupby("customer_id")
         .agg(products=("payment_type", "nunique"), txns=("event_id", "count"),
              total_spend=("amt", "sum")).query("products > 1")
         .sort_values("total_spend", ascending=False).head(15).reset_index())
st.dataframe(cross, use_container_width=True, hide_index=True)

# ── Data Explorer ──
with st.expander("📊 Data Explorer — Browse Filtered Events"):
    cols = st.multiselect("Columns", df.columns.tolist(),
        default=["event_id", "source_system", "customer_id", "amt", "currency", "status", "payment_type", "event_date"])
    st.dataframe(df[cols].head(100), use_container_width=True, hide_index=True)
    st.download_button("Download filtered CSV", df.to_csv(index=False), "filtered_events.csv", "text/csv")

# ── SQL Runner ──
with st.expander("🔧 SQL Query Runner"):
    sql = st.text_area("SQL (table: `events`):", value="SELECT payment_type, COUNT(*) AS cnt, "
        "ROUND(AVG(amt),2) AS avg_amt FROM events GROUP BY 1 ORDER BY cnt DESC", height=80)
    if st.button("Run Query"):
        try: st.dataframe(q(sql), use_container_width=True, hide_index=True)
        except Exception as e: st.error(str(e))
