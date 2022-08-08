import requests
import boto3
from datetime import datetime
import os

WEATHER_API_URL = 'http://api.openweathermap.org/data/2.5/weather?q=Hamburg,de&APPID=8116534ad5cccd3a23217710dddcc089'
weather_table_name = os.environ["WEATHER_DATA"]

dynamodb_resource = boto3.resource("dynamodb")
weather_table = dynamodb_resource.Table(weather_table_name)


def load_weather_data():
    print("start loading weather data")
    response = requests.get(WEATHER_API_URL)
    return response.json()["data"]

def map_weather_data(weather_api_data):
    result = []
    today = datetime.now()
    for weather_item in weather_api_data:
        result.append({
            "id": weather_item["source_id"],
            "weather": weather_item["main"],
            "wind": weather_item["speed"],
            "date": today.strftime("%Y-%m-%d %H:%M:%S")
        })
    return result

def save_weather_data(weatherdata):
    for weather in weatherdata:
        weather_table.put_item(Item = weather)

def handle(event, context):
    weather_api_data = load_weather_data()
    mapped_data = map_weather_data(weather_api_data)
    save_weather_data(mapped_data)

if __name__ == "__main__":
    handle({},{})