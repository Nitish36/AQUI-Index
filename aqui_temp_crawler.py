import requests
import pandas as pd
import os
import urllib3
import gspread
from gspread_dataframe import set_with_dataframe
import json
urllib3.disable_warnings()

def get_aqui():
    url = "https://apiserver.aqi.in/aqi/getAirQualityRanklistCountryAndCity?sensorname=AQI-IN&type=2&limit=100&source=web"
    headers = {
        "accept-encoding":"gzip, deflate, br, zstd",
        "authorization": "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOjEsImlhdCI6MTc2OTg2MjYzMSwiZXhwIjoxNzcwNDY3NDMxfQ.QU2NuioUOxA8dRl-UIKZ674Nr1naumENL7ZOpVsAcyg",
        "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
    }
    resp = requests.get(url,headers=headers,verify=False)
    json_data = resp.json()
    dict_city = {}
    dict_weather = {}
    aqui = []
    for data in json_data["data"]:
        if data["rank"]<=100:
            dict_city = {
                "status":json_data["status"],
                "location":data["location"],
                "city":data["city"],
                "state":data["state"],
                "country":data["country"],
                "rank":data["rank"],
                "latitude": data["latitude"],
                "longitude": data["longitude"],
                "updated_at": data["updated_at"],
                "AQI-IN": data["AQI-IN"],
            }
            aqui.append(dict_city)
    return aqui

def put_aqui():
    aqui_data = get_aqui()

    # Convert list → DataFrame
    aqui_df = pd.DataFrame(aqui_data)

    GSHEET_NAME = 'AQUI Index'
    TAB_NAME = 'aqui'

    creds_json = os.environ.get("GSHEET_CREDENTIALS")
    if not creds_json:
        raise ValueError("GSHEET_CREDENTIALS not found")

    creds_dict = json.loads(creds_json)
    gc = gspread.service_account_from_dict(creds_dict)

    sh = gc.open(GSHEET_NAME)
    worksheet = sh.worksheet(TAB_NAME)

    # 👇 ADD THIS LOGIC HERE
    existing_rows = len(worksheet.get_all_values())

    if existing_rows == 0:
        start_row = 1
        include_header = True
    else:
        start_row = existing_rows + 1
        include_header = False


    set_with_dataframe(
        worksheet,
        aqui_df,
        row=start_row,
        include_index=False,
        include_column_header=include_header
    )

    print("✅ Data loaded successfully to Google Sheets!")
    
put_aqui()



