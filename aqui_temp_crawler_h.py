import requests
import pandas as pd
import os
import urllib3
import gspread
from gspread_dataframe import set_with_dataframe
import json

urllib3.disable_warnings()

def get_hotweatherdata():
    url = "https://apiserver.aqi.in/aqi/getCityRankingsOfWorld?type=h&source=web"
    headers = {
        "accept-encoding":"gzip, deflate, br, zstd",
        "authorization": "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOjEsImlhdCI6MTc2OTIzMzc5NSwiZXhwIjoxNzY5ODM4NTk1fQ.PnsZkBQmiGHUQkHbqozm_ky-4Se9L4Wd0zBKjgQ0AC0",
        "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
    }
    resp = requests.get(url,headers=headers,verify=False)
    json_data = resp.json()
    dict_city = {}
    dict_weather = {}
    hot_weather = []
    for data in json_data["data"]:
        if data["rank"]<=100:
            dict_city = {
                "uid": data["uid"],
                "status":json_data["status"],
                "city":data["city"],
                "state":data["state"],
                "country":data["country"],
                "rank":data["rank"]
            }
            for weather in data["weather"]:
                dict_weather={
                    "id":weather["_id"],
                    "cloud":weather["cloud"],
                    "condition":weather["condition"]["text"],
                    "feelslike_c":weather["feelslike_c"],
                    "feelslike_f":weather["feelslike_f"],
                    "gust_kph":weather["gust_kph"],
                    "gust_mph":weather["gust_mph"],
                    "humidity":weather["humidity"],
                    "last_updated":weather["last_updated"],
                    "precip_in":weather["precip_in"],
                    "precip_mm":weather["precip_mm"],
                    "pressure_in":weather["pressure_in"],
                    "pressure_mb":weather["pressure_mb"],
                    "temp_c":weather["temp_c"],
                    "temp_f":weather["temp_f"],
                    "uv":weather["uv"],
                    "vis_km":weather["vis_km"],
                    "vis_miles":weather["vis_miles"],
                    "wind_degree":weather["wind_degree"],
                    "wind_dir":weather["wind_dir"],
                    "wind_kph":weather["wind_kph"],
                    "wind_mph":weather["wind_mph"],
                    "uv_condition":weather["uv_condition"]["text"],
                }
            dict_city.update(dict_weather)
            hot_weather.append(dict_city)
    return hot_weather

def put_hotweatherdata():
    hot_weather_data = get_hotweatherdata()

    # Convert list → DataFrame
    hot_weather_df = pd.DataFrame(hot_weather_data)

    GSHEET_NAME = 'AQUI Index'
    TAB_NAME = 'hot_weather'

    creds_json = os.environ.get("GSHEET_CREDENTIALS")
    if not creds_json:
        raise ValueError("GSHEET_CREDENTIALS not found")

    creds_dict = json.loads(creds_json)
    gc = gspread.service_account_from_dict(creds_dict)

    sh = gc.open(GSHEET_NAME)
    worksheet = sh.worksheet(TAB_NAME)

    # 👇 ADD THIS LOGIC HERE
    existing_rows = len(worksheet.get_all_records())

    if existing_rows == 0:
        start_row = 1
        include_header = True
    else:
        start_row = existing_rows + 2  # +1 for header, +1 for next row
        include_header = False

    set_with_dataframe(
        worksheet,
        hot_weather_df,
        row=start_row,
        include_index=False,
        include_column_header=include_header
    )

    print("✅ Data loaded successfully to Google Sheets!")


put_hotweatherdata()









