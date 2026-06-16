import streamlit as st
import pandas as pd
import ssl
import requests
import io
import matplotlib.pyplot as plt
import pydeck as pdk

spreadsheet_id = "1myYlsoOTpXPPN9mKfZkEDrX_H5mlAiIPbM0HxA6L0OY"
ssl._create_default_https_context = ssl._create_unverified_context
url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv"

response = requests.get(url, verify=False)
df = pd.read_csv(io.StringIO(response.text))

def get_aqi_status(aqi):
    if aqi<=50:
        return "Good 🟢"
    elif aqi<=100:
        return "Moderate 🟡"
    elif aqi<=150:
        return "Poor 🟠"
    else:
        return "Severe 🔴"

st.title("🌍 India AQI & Weather Dashboard")
st.markdown("Real-time Air Quality and Weather Analytics Across Indian Cities")

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

recordcount,avg_aqi,aqi_status,avg_temp,avg_humidity=st.columns(5)
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

with aqi_status:
    aqi = filtered_data["aqi"].mean()
    st.status(
        f"AQI Status: {get_aqi_status(aqi)}"
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

bar1, bar2 = st.columns(2)
with bar1:
    st.subheader("AQUI Across Cities")
    st.bar_chart(
        df.groupby("city")["aqi"].mean()
    )

with bar2:
    st.subheader("Temperature Across Cities")
    st.bar_chart(
        df.groupby("city")["temp_c"].mean()
    )

st.subheader("Humidity Across Cities")
st.bar_chart(
    df.groupby("state")["humidity"].mean()
)

scat1,scat2,scat3 = st.columns(3)

with scat1:
    fig, ax = plt.subplots()
    st.subheader("AQI Vs Temp")
    Temp_AQUI = ax.scatter(
        filtered_data["temp_c"],
        filtered_data["AQI_IN"],
        c=filtered_data["AQI_IN"],
        cmap="RdYlGn_r"
    )

    plt.colorbar(Temp_AQUI)
    st.pyplot(fig)

with scat2:
    fig1, ax1 = plt.subplots()
    st.subheader("AQI Vs Humidity")
    Wind_AQUI = ax1.scatter(
        filtered_data["humidity"],
        filtered_data["AQI_IN"],
        c=filtered_data["AQI_IN"],
        cmap="viridis"
    )
    plt.colorbar(Wind_AQUI)
    st.pyplot(fig1)

with scat3:
    fig2, ax2 = plt.subplots()
    st.subheader("AQI Vs UV")
    UV_AQUI = ax2.scatter(
        filtered_data["uv"],
        filtered_data["AQI_IN"],
        c=filtered_data["AQI_IN"],
        cmap="viridis"
    )
    plt.colorbar(UV_AQUI)
    st.pyplot(fig2)



top_polluted = (
    df.sort_values(
        by="AQI_IN",
        ascending=False
    )
    .head(10)
)

top_high_temp=(
    df.sort_values(
        by="temp_c",
        ascending=False
    ).head(10)
)

high_wind = (
    df.sort_values(
        by="wind_kph",
        ascending=False
    ).head(10)
)

tab1,tab2 = st.columns(2)

with tab1:
    st.subheader("Top 10 Polluted Cities")

    st.dataframe(
        top_polluted[["city","AQI_IN","pm25","pm10"]]
    )

with tab2:
    st.subheader("Top 10 Hottest Cities")
    st.dataframe(
        top_high_temp[["city", "temp_c", "feels_like_c"]]
    )

st.subheader("Top strong winds blown")
st.dataframe(
    high_wind[["city","wind_kph","wind_degree","wind_dir"]]
)

st.pydeck_chart(
    pdk.Deck(
        initial_view_state=pdk.ViewState(
            latitude=df["latitude"].mean(),
            longitude=df["longitude"].mean(),
            zoom=4,
            pitch=50,
        ),
        layers=[
            pdk.Layer(
                "HeatmapLayer",
                data=df,
                get_position=["longitude", "latitude"],
                get_weight="AQI_IN",
                opacity=0.8,
            )
        ],
    )
)

st.header("Raw Data")
st.dataframe(filtered_data)

