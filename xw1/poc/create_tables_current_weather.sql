CREATE TABLE IF NOT EXISTS xweather.raw_data (
    station_id text,
    cal_bucket text,
    event_time timestamp,
    metric_name text,
    metric_value text,
    insert_time  timestamp,
    PRIMARY KEY ((station_id, cal_bucket), event_time,metric_name)
) WITH CLUSTERING ORDER BY (event_time DESC,metric_name ASC)
;

CREATE TABLE IF NOT EXISTS  xweather.weather_stations (
     station_id text,
     geohash_id text,
     geohash_sub text,
     neighbors list<text>,
     lat	float,
     lon	float,
     elev       float,
     timezone   text,
     tzoffset   text,
     source	text,
     PRIMARY KEY ((geohash_id),station_id)
     )
;

CREATE TABLE IF NOT EXISTS xweather.neighbor_map_stations (
geohash_sub text,
station_id text,
geohash_id text,
lat_long   list<text>,
neighbors  list<text>,
PRIMARY KEY ((geohash_sub),station_id,geohash_id)
);

