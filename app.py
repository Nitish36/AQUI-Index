import streamlit as st
import pandas as pd
import ssl
import requests
import io
import matplotlib.pyplot as plt

spreadsheet_id = "1myYlsoOTpXPPN9mKfZkEDrX_H5mlAiIPbM0HxA6L0OY"
ssl._create_default_https_context = ssl._create_unverified_context
url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv"

response = requests.get(url, verify=False)
df = pd.read_csv(io.StringIO(response.text))

st.title("AQUI Dashboard")

col1, col2 = st.columns(2)

with col1:
    state = st.selectbox(
        "Select City",
        df["state"].unique()
    )
filtered_data = df[df["state"] == state]

with col2:
    city = st.selectbox(
        "Select City",
        filtered_data["city"].unique()
    )

recordcount,avg_aqi,avg_temp,avg_humidity,avg_uv=st.columns(5)
with recordcount:
    st.metric(
        "Total Records",
        len(filtered_data)
    )

with avg_aqi:
    st.metric(
        "Average AQI",
        round(filtered_data["aqi"].mean(),2)
    )

with avg_temp:
    st.metric(
        "Average Temp",
        round(filtered_data["temp_c"].mean(),2)
    )

with avg_humidity:
    st.metric(
        "Average Humidity",
        round(filtered_data["humidity"].mean(),2)
    )

with avg_uv:
    st.metric(
        "Average UV",
        round(filtered_data["uv"].mean(),2)
    )

st.subheader("AQUI Across Cities")
st.bar_chart(
    df.groupby("city")["aqi"].mean()
)

st.subheader("Temperature Across Cities")
st.bar_chart(
    df.groupby("city")["temp_c"].mean()
)

st.subheader("Humidity Across Cities")
st.bar_chart(
    df.groupby("state")["humidity"].mean()
)

fig, ax = plt.subplots()

Temp_AQUI = ax.scatter(
    filtered_data["temp_c"],
    filtered_data["AQI_IN"],
    c=filtered_data["AQI_IN"],
    cmap="RdYlGn_r"
)

plt.colorbar(Temp_AQUI)
st.pyplot(fig)

fig1, ax1 = plt.subplots()
Wind_AQUI = ax1.scatter(
    filtered_data["humidity"],
    filtered_data["AQI_IN"],
    c=filtered_data["AQI_IN"],
    cmap="viridis"
)
plt.colorbar(Wind_AQUI)
st.pyplot(fig1)

fig2, ax2 = plt.subplots()
UV_AQUI = ax2.scatter(
    filtered_data["uv"],
    filtered_data["AQI_IN"],
    c=filtered_data["AQI_IN"],
    cmap="viridis"
)
plt.colorbar(UV_AQUI)
st.pyplot(fig2)

st.header("Raw Data")
st.dataframe(filtered_data)

