from dataclasses import dataclass
from datetime import datetime

@dataclass
class BusRecord:
    """A representation of a bus movement to/from a bus stop."""
    bus_key: int
    route_key: str
    route_name: str
    route_variant_key: str
    route_variant_name: str
    schedule_key: str
    scheduled_arrival_time: datetime
    scheduled_departure_time: datetime
    estimated_arrival_time: datetime
    estimated_departure_time: datetime
    stop_key: int
    stop_name: str
    query_time: datetime
