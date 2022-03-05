import requests
import typing as tp
from datetime import datetime, timedelta, timezone
from .models import BusRecord


# import numpy as np

def format_date(date_string: str) -> str:
    if not date_string:
        return
    return date_string.strftime("%Y-%m-%dT%H:%M:%S")


class Transit:
    def __init__(self, api_key: str, endpoint: str):
        self.api_key = api_key
        self.endpoint = endpoint
    
    def _request(
            self, 
            method: str = "GET",
            body: tp.Optional[tp.Any] = None,
            path: str = "/",
            query: tp.Optional[tp.Any] = None,
            headers: tp.Optional[tp.Any] = None,
        ) -> requests.Response:

        if query is None:
            query = { 'api-key': self.api_key }
        else:
            query['api-key'] = self.api_key

        if headers is None:
            headers = { 'Content-Type': 'application/json' }

        endpoint = self.endpoint + path + '.json'
        
        request_config = {
            "headers": headers,
            "params": query,
        }

        if method.lower() in ('put', 'post'):
            request_config['data'] = body
        
        fn = getattr(requests, method.lower())
        return fn(endpoint, **request_config)


    def __call__(self, *args, **kwargs):
        resp = self._request(method="GET", *args, **kwargs)
        resp.raise_for_status()
        return resp.json()
    
    def get_nearby_stops(
        self, 
        latitude: float, 
        longitude: float, 
        walking: bool, 
        radius: int
    ) -> tp.Dict[int, tp.Any]:
        """Get all the stops that are within some distance of the given lat/lon coordinate.

        """
        stop_data = self(
            path="/stops", 
            query={
                "lat": latitude,
                "lon": longitude,
                "walking": walking,
                "distance": radius
            }
        )

        stops_by_key = dict()
        for stop_record in stop_data['stops']:
            stops_by_key[stop_record['key']] = stop_record
        return stops_by_key

    def get_schedules_for(
        self,
        stop_ids: tp.Iterable[int],
        start_time: datetime,
        end_time: datetime,
    ) -> tp.Dict[int, tp.Any]:
        schedules = {
            stop_id: self(
                    path=f"/stops/{stop_id}/schedule",
                    query={
                        "start": format_date(start_time),
                        "end": format_date(end_time)
                    }
                )
            for stop_id in stop_ids
        }
        return schedules
    
    def get_bus_records(
        self,
        latitude: float,
        longitude: float,
        time_stamp: datetime,
        radius: int,
        time_window_delta: timedelta
    ) -> tp.List[BusRecord]:

        nearby_stops = self.get_nearby_stops(latitude, longitude, walking=True, radius=radius)
    
        stop_schedules = self.get_schedules_for(
            nearby_stops,
            start_time=time_stamp - time_window_delta,
            end_time=time_stamp + time_window_delta
        )
        
        all_bus_records = []

        # Since the structure of the data returned
        # isn't exactly ideal or consistent, we'll just pick the
        # key/vals of interest.

        for stop_id, stop_schedule in stop_schedules.items():

            record = dict()
            record['stop_key'] = stop_id

            query_time = stop_schedule['query-time']
            record['query_time'] = stop_schedule['query-time']
            
            schedule = stop_schedule['stop-schedule']
            record['stop_name'] = schedule['stop']['name']
            
            if 'route-schedules' not in schedule:
                continue
            
            for route_schedule in schedule['route-schedules']:
                record['route_key'] = route_schedule['route']['key']
                record['route_name'] = route_schedule['route']['name']
                
                
                if 'scheduled-stops' not in route_schedule:
                    continue
        
                for scheduled_stop in route_schedule['scheduled-stops']:
                    record['schedule_key'] = scheduled_stop['key']
                    record['route_variant_key'] = scheduled_stop['variant']['key']
                    record['route_variant_name'] = scheduled_stop['variant']['name']
                    if 'bus' in scheduled_stop:
                        record['bus_key'] = scheduled_stop['bus']['key']
                    else:
                        record['bus_key'] = None
                    
                    times = scheduled_stop['times']

                    if 'arrival' in times:
                        arrival = times['arrival']
                        if 'scheduled' in arrival:
                            record['scheduled_arrival_time'] = arrival['scheduled']
                        if 'estimated' in arrival:
                            record['estimated_arrival_time'] = arrival['estimated']
                    else:
                        record['scheduled_arrival_time'] = None
                        record['estimated_arrival_time'] = None
                        
                    if 'departure' in times:
                        departure = times['departure']
                        if 'scheduled' in departure:
                            record['scheduled_departure_time'] = departure['scheduled']
                        if 'estimated' in departure:
                            record['estimated_departure_time'] = departure['estimated']
                    else:
                        record['scheduled_departure_time'] = None
                        record['estimated_departure_time'] = None
                    all_bus_records.append(BusRecord(**record))
        
        return all_bus_records

