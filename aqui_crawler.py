import requests
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

slug = ["india/gujarat/ahmedabad", "india/karnataka/bangalore", "india/tamil-nadu/chennai",
        "india/telangana/hyderabad", "india/west-bengal/kolkata", "india/maharashtra/mumbai",
        "india/delhi/new-delhi", "india/maharashtra/pune", "india/uttar-pradesh/lucknow",
        "india/punjab/ludhiana", "india/madhya-pradesh/bhopal", "india/haryana/gurgaon",
        "india/kerala/kochi", "india/odisha/bhubaneswar", "india/jharkhand/ranchi",
        "india/goa/madgaon", "india/rajasthan/jaipur", "india/chhattisgarh/raipur",
        "india/assam/guwahati", "india/bihar/patna", "india/uttarakhand/dehradun",
        "india/himachal-pradesh/shimla", "india/sikkim/gangtok"]
dict_weather = {}
aqui_data = []
for slug in slug:
    url = f"https://apiserver.aqi.in/aqi/v2/getLocationDetailsBySlug?slug={slug}&type=3&source=web"
    headers = {
            "accept-encoding": "gzip, deflate, br, zstd",
            "authorization": "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOjEsImlhdCI6MTc2OTg2MjYzMSwiZXhwIjoxNzcwNDY3NDMxfQ.QU2NuioUOxA8dRl-UIKZ674Nr1naumENL7ZOpVsAcyg",
            "connection": "keep-alive",
            "host": "apiserver.aqi.in",
            "origin": "https://aqi.in",
            "referer": "https://aqi.in/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
    }
    print(f"Fetching data for {slug}...")
    response = requests.get(url, headers=headers, verify=False)
    data = response.json()
    for df in data['data']:
        dict_weather = {
            "status": data["status"],
            "station": df["station"],
            "city": df["city"],
            "state": df["state"],
            "country": df["country"],
            "location": df["location"],
            "timezone": df["time_zone"],
            "latitude": df["latitude"],
            "longitude": df["longitude"],
            "uid": df["uid"],
            "city_lat": df["city_lat"],
            "city_lon": df["city_lon"],
            "state_lat": df["state_lat"],
            "state_lon": df["state_lon"],
            "updated_at": df["updated_at"],
            "isOnline": df["isOnline"],
            "AQI_IN": df["iaqi"]["AQI-IN"],
            "aqi": df["iaqi"]["aqi"],
            "co": df["iaqi"]["co"],
            "no2": df["iaqi"]["no2"],
            "o3": df["iaqi"]["o3"],
            "pm10": df["iaqi"]["pm10"],
            "pm25": df["iaqi"]["pm25"],
            "so2": df["iaqi"]["so2"],
            "t": df["iaqi"]["t"],
            "clouds": df["weather"]["cloud"],
            "condition": df["weather"]["condition"]["text"],
            "feels_like_c": df["weather"]["feelslike_c"],
            "feels_like_f": df["weather"]["feelslike_f"],
            "gust_kph": df["weather"]["gust_kph"],
            "gust_mph": df["weather"]["gust_mph"],
            "humidity": df["weather"]["humidity"],
            "precip_in": df["weather"]["precip_in"],
            "precip_mm": df["weather"]["precip_mm"],
            "pressure_in": df["weather"]["pressure_in"],
            "pressure_mb": df["weather"]["pressure_mb"],
            "temp_c": df["weather"]["temp_c"],
            "temp_f": df["weather"]["temp_f"],
            "uv": df["weather"]["uv"],
            "vis_km": df["weather"]["vis_km"],
            "vis_miles": df["weather"]["vis_miles"],
            "wind_degree": df["weather"]["wind_degree"],
            "wind_dir": df["weather"]["wind_dir"],
            "wind_kph": df["weather"]["wind_kph"],
            "wind_mph": df["weather"]["wind_mph"],
            "uv_condition": df["weather"]["uv_condition"]["text"],
        }
        aqui_data.append(dict_weather)

df = pd.DataFrame(aqui_data)
df.to_csv("aqi_data_india.csv", index=False)