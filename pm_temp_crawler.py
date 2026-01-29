import requests
import pandas as pd
import urllib3
urllib3.disable_warnings()

def get_pm():
    url = "https://apiserver.aqi.in/aqi/getAirQualityRanklistCountryAndCity?sensorname=pm25&type=2&limit=100&source=web"
    headers = {
        "accept-encoding":"gzip, deflate, br, zstd",
        "authorization": "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOjEsImlhdCI6MTc2OTIzMzc5NSwiZXhwIjoxNzY5ODM4NTk1fQ.PnsZkBQmiGHUQkHbqozm_ky-4Se9L4Wd0zBKjgQ0AC0",
        "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
    }
    resp = requests.get(url,headers=headers,verify=False)
    json_data = resp.json()
    dict_city = {}
    dict_weather = {}
    pm = []
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
                "pm25": data["pm25"],
            }
            pm.append(dict_city)
    return pm

pm_weather_data = get_pm()
print(pm_weather_data)