import typing as tp
import itertools as it
from collections import defaultdict
from datetime import timedelta

import pandas as pd
import numpy as np

from .models import BusRecord


def connect_bus_trips(bus_records_df: pd.DataFrame) -> tp.List[tp.List[BusRecord]]:
    """Given raw dataframe of all bus records, connect those that belong to the same bus.

    This is the "making line segments out of consecutive bus stops" aspect of the solution.

    """
    
    all_bus_trips = []

    bus_records_grouped_by_bus = bus_records_df.groupby(['route_key', 'route_variant_key', 'bus_key'])

    for (route_key, route_variant_key, bus_key), bus_record_ids in bus_records_grouped_by_bus.groups.items():
        
        bus_record_ids = [int(record_id) for record_id in bus_record_ids]
        
        # Don't bother checking buses that appear only once in the region of interest.
        # We don't even know where they're heading. Bad for time window estimation.
        if len(bus_record_ids) < 2:
            continue
        
        records_of_interest = (
            bus_records_df
            .iloc[bus_record_ids, :]
            .sort_values(
                ['scheduled_arrival_time', 'stop_key']
            )
        )


        # Convert records of interest from rows into BusRecord objects
        # for ease of use.
        bus_records = records_of_interest.to_dict(orient='records')
        bus_records = [BusRecord(**record) for record in bus_records] 
        
        
        # If there are not at least two unique stops, we know its
        # only one bus that appeared multiple times in the given window.
        # This is an edge case, although not helpful.
        # Just ignore these.
        unique_stops_along_the_way = {record.stop_key for record in bus_records}

        if len(unique_stops_along_the_way) < 2:
            continue

            
        bus_records_by_stop = defaultdict(list)
        
        # Bin the bus records by the stops they pass through.
        # This helps distinguish multiple trips of the same bus
        # through this stop within the time window.
        
        for bus_record in bus_records:
            bus_records_by_stop[bus_record.stop_key].append(bus_record)
        
        # Sort each of the bins on the estimated arrival time.
        # This provides a consistent ordering of 
        # the buses across different stops.
        # So the n-th records of every bin correspond to a trip of the same bus.
        
        for stop_key in bus_records_by_stop.keys():
            bus_records_by_stop[stop_key].sort(key=lambda record: record.estimated_arrival_time)
        
        
        # Take one item from every bin and call that tuple the records for a single bus trip.
        for single_bus_trip in zip(*bus_records_by_stop.values()):
            
            # This should be a correct trip of a bus within the target radius.
            # Further sort the bus trip according to the time, so we get the correct
            # order of stops travelled by this bus.
            single_bus_trip = list(sorted(single_bus_trip, key=lambda record: record.estimated_arrival_time))
            all_bus_trips.append(single_bus_trip)

    return all_bus_trips


def connect_based_on_bus_key_and_departure_time(df: pd.DataFrame):
    bus_trips = defaultdict(list)

    indices_with_bus_key = np.array(df[~(df['bus_key'].isna())].index.tolist())
    df = df.iloc[indices_with_bus_key, :]

    df = df.astype({
        'bus_key': 'int32', 
        'scheduled_arrival_time': 'datetime64[ns]',
        'scheduled_departure_time': 'datetime64[ns]',
        'estimated_arrival_time': 'datetime64[ns]',
        'estimated_departure_time': 'datetime64[ns]',
        'query_time': 'datetime64[ns]'
    })

    # df['bus_key'] = np.array(df['bus_key'], dtype=np.uint32)
    df = df.reset_index()

    grouped_by_bus_and_route = df.groupby(['bus_key', 'route_variant_key'], sort=False)

    for (bus_key, route_variant_key), bus_route_indices in (
        grouped_by_bus_and_route.groups.items()
    ):
        indices = np.array(list(bus_route_indices))
        
        records_by_bus = (
            df
            .iloc[indices, :]
            .sort_values(['scheduled_departure_time'])
        )
        del records_by_bus['index']
        records_by_bus = [
            BusRecord(**record) for record in records_by_bus.to_dict(orient='records')
        ]
        
        if len(records_by_bus) < 2:
            continue

        current_bus_record = records_by_bus[0]
        bus_trip = [current_bus_record]

        for bus_record in records_by_bus[1:]:
            if bus_record.estimated_departure_time - current_bus_record.estimated_departure_time <= timedelta(minutes=10):
                bus_trip.append(bus_record)
            else:
                bus_trips[bus_key, route_variant_key].append(bus_trip)
                bus_trip = [bus_record]

            current_bus_record = bus_record
        bus_trips[bus_key, route_variant_key].append(bus_trip)
    return bus_trips

