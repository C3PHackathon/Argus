from fastapi import FastAPI
import pandas as pd
from geopy import distance
from transit import client
from datetime import datetime, timedelta


app = FastAPI()
traffic_cam_data = pd.read_csv("Traffic_cameras.csv")

@app.get("/")
async def root():
    return "Hello, Open Data Hackathon!"


@app.get("/traffic-cams")
async def traffic_cams(lat: float, lon: float, dist: int):
    ret = list()
    for _, row in traffic_cam_data.iterrows():
        cam_location = row.Latitude, row.Longitude
        source = (lat, lon)
        if abs(distance.distance(cam_location, source).m) <= dist:
            ret.append(row.to_dict())
    return ret


@app.get("/bus-cams")
async def bus_cams(lat: float, lon: float, event_time: datetime = datetime.now(), radius:int = 50, time_delta: int = 30):
    transit = client.Transit(api_key="", endpoint="https://api.winnipegtransit.com/v3/")
    r = transit.get_bus_records(
        latitude=lat, longitude=lon, time_stamp=event_time, radius=radius, time_window_delta=timedelta(minutes=time_delta)
        )
    return r