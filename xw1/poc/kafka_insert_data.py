# Insert into Table raw_data
# Expects a list in the format to insert into table raw_data

from timeit import default_timer as timer
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.query import ordered_dict_factory
from cassandra.concurrent import execute_concurrent_with_args

from datetime import datetime
import mzgeohash

start_time = timer()
cluster = Cluster()
session = cluster.connect('xweather')


def insert_raw_data(data):
    # Inserts data into the table raw_data
    # Assuming that the data is in the format (station_id,cal_bucket,event_time,metric_name,metric_value)
    ctr = 0
    query = "insert into raw_data(station_id,cal_bucket,event_time,metric_name,metric_value,insert_time) \
              values (?,?,?,?,?,?)"
    prepared = session.prepare(query)
    parameters =[]
    for row in data:
        try:
            row = list(row.split(','))
            row[1] = row[2][:10]
            row[2] = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S")

            # bound = prepared.bind((row[0], row[1], row[2], row[3], row[4], datetime.utcnow()))
            # session.execute_async(bound)
            parameters.append((row[0], row[1], row[2], row[3], row[4], datetime.utcnow()))
            ctr += 1
        except:
            print('Error processing row ', row)

    execute_concurrent_with_args(session, prepared, parameters, concurrency=500)

    print('Executed dataset in ', timer() - start_time, 'rows:', ctr)


def insert_weather_stations(data):
    # Assuming that the data is in the correct format for inserting to weather_stations

    query = "insert into weather_stations(geohash_id,station_id,elev,geohash_sub,lat,lon,source,timezone,tzoffset,neighbors) \
                                                 values(?,?,?,?,?,?,?,?,?,?)"
    prepared = session.prepare(query)
    query2 = "insert into neighbor_map_stations(geohash_sub,station_id,geohash_id,lat_long,neighbors) \
                values (?,?,?,?,?)"
    prepared2 = session.prepare(query2)

    ctr = 0
    for row in data:
        try:
            row = list(row.split(','))
            maplist = list(mzgeohash.neighbors(row[0][:3]).values())
            # print (row,maplist)
            bound = prepared.bind((row[0], row[1], float(row[2]), (row[3]), float(row[4]), float(row[5]), (row[6]),
                                   (row[7]), row[8], maplist))
            session.execute_async(bound)

            # Insert into neighbor_map_stations
            bound2 = prepared2.bind((row[3], row[1], row[0], list((row[4], row[5])), maplist))
            session.execute_async(bound2)
            ctr += 1
        except:
            print('Error processing row ', row)
        print('Executed dataset in ', timer() - start_time, 'rows:', ctr)


def query_neighbors(geohashid):
    # Function returns nearest neighbors for the geohash passed in from the neighbor_map_stations

    # print('Geohashid sent is ', geohashid)
    query = "select geohash_sub,station_id,geohash_id,lat_long from neighbor_map_stations where geohash_sub=?"
    session.row_factory = dict_factory
    prepared = session.prepare(query)
    m = session.execute(prepared, (geohashid,))
    return m


def  query_raw_data(station_id,cal_bucket):
    # Function returns the metrics for the station_id and cal_date passed in
    query = "select station_id,event_time,metric_name,metric_value from raw_data where station_id=? and cal_bucket=?"
    session.row_factory=dict_factory
    prepared=session.prepare(query)
    m=session.execute(prepared,(station_id,cal_bucket))
    return m
