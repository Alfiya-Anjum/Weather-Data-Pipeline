import streamlit as st
from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import os
from datetime import datetime

# --- STREAMLIT PAGE CONFIG ---
st.set_page_config(
    page_title="🌍 Global Weather Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- GCP SETUP ---
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_key.json"
PROJECT_ID = "weather-etl-project-475620"

# Initialize BigQuery Client
client = bigquery.Client(project=PROJECT_ID)

# --- APP HEADER ---
st.markdown("""
# 🌍 Global Weather Analytics Dashboard
#### **Data Pipeline:** OpenWeatherMap → Python ETL → Google BigQuery → Streamlit
""")

# Add a refresh button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()

# --- FETCH DATA FUNCTION ---
@st.cache_data(ttl=600)
def load_data():
    query = f"""
        SELECT city, temperature_celsius, humidity, wind_speed, weather_main, timestamp
        FROM `{PROJECT_ID}.weather_data.weather_records`
        ORDER BY timestamp DESC
        LIMIT 500
    """
    df = client.query(query).to_dataframe()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

try:
    df = load_data()
except Exception as e:
    st.error(f"❌ Failed to load data from BigQuery: {e}")
    st.stop()

# --- Add city coordinates manually ---
city_coords = {
    "London": {"lat": 51.5074, "lon": -0.1278},
    "New York": {"lat": 40.7128, "lon": -74.0060},
    "Paris": {"lat": 48.8566, "lon": 2.3522},
    "Sydney": {"lat": -33.8688, "lon": 151.2093},
    "Tokyo": {"lat": 35.6762, "lon": 139.6503}
}

# Add lat/lon columns
df["lat"] = df["city"].map(lambda x: city_coords.get(x, {}).get("lat"))
df["lon"] = df["city"].map(lambda x: city_coords.get(x, {}).get("lon"))

# --- SIDEBAR FILTERS ---
st.sidebar.header("🔍 Filter Options")
cities = st.sidebar.multiselect(
    "Select Cities:", 
    options=df["city"].unique(),
    default=df["city"].unique()
)
filtered_df = df[df["city"].isin(cities)]

# --- KPI METRICS ---
st.markdown("### 🌡️ Key Weather Insights")

col1, col2, col3, col4 = st.columns(4)

col1.metric("🌆 Cities Tracked", len(filtered_df["city"].unique()))
col2.metric("🌡 Avg Temperature (°C)", round(filtered_df["temperature_celsius"].mean(), 2))
col3.metric("💧 Avg Humidity (%)", round(filtered_df["humidity"].mean(), 2))
col4.metric("💨 Avg Wind Speed (m/s)", round(filtered_df["wind_speed"].mean(), 2))

st.markdown("---")

# --- TEMPERATURE TREND ---
st.subheader("📈 Temperature Trends Over Time")
fig1 = px.line(
    filtered_df,
    x="timestamp",
    y="temperature_celsius",
    color="city",
    markers=True,
    title="Temperature Over Time by City"
)
fig1.update_layout(
    xaxis_title="Timestamp",
    yaxis_title="Temperature (°C)",
    template="plotly_white"
)
st.plotly_chart(fig1, use_container_width=True)

# --- ROW 2 VISUALS ---
col5, col6 = st.columns(2)

with col5:
    st.subheader("💧 Average Humidity by City")
    avg_humidity = filtered_df.groupby("city")["humidity"].mean().reset_index()
    fig2 = px.bar(
        avg_humidity,
        x="city",
        y="humidity",
        color="city",
        text_auto=".2f",
        title="Average Humidity (%)"
    )
    fig2.update_layout(template="plotly_white", showlegend=False)
    st.plotly_chart(fig2, use_container_width=True)

with col6:
    st.subheader("🌦 Weather Condition Distribution")
    fig3 = px.pie(
        filtered_df,
        names="weather_main",
        title="Weather Conditions (%)",
        hole=0.4
    )
    fig3.update_traces(textinfo="percent+label")
    st.plotly_chart(fig3, use_container_width=True)

# --- MAP VISUALIZATION ---
st.subheader("🗺️ Global Weather Map")

fig4 = px.scatter_geo(
    filtered_df,
    lat="lat",
    lon="lon",
    color="temperature_celsius",
    size="humidity",
    hover_name="city",
    projection="natural earth",
    title="Temperature & Humidity by City"
)

fig4.update_layout(
    template="plotly_white",
    geo=dict(showland=True, landcolor="lightgrey"),
)
st.plotly_chart(fig4, use_container_width=True)


# --- FOOTER ---
st.markdown("""
---
✅ **Last Updated:** {}
  
🧠 **Tech Stack:** Python | Google BigQuery | Streamlit | Plotly  
💾 **Pipeline:** ETL (Extract → Transform → Load) automated via OpenWeatherMap API  
📊 **Purpose:** Real-time analytics of global weather patterns
""".format(datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")))
