CREATE TABLE IF NOT EXISTS xweather.raw_data (
    station_id text,
    cal_bucket text,
    event_time timestamp,
    metric text,
    metric_value float,
    insert_time  timestamp,
    PRIMARY KEY ((station_id, cal_bucket), event_time,metric)
) WITH CLUSTERING ORDER BY (event_time DESC,metric ASC)
;

CREATE TABLE IF NOT EXISTS  xweather.weather_stations (
     station_id text,
     geohash_id text,
     geohash_4 text,
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
geohash_4 text,
station_id text,
geohash_id text,
PRIMARY KEY ((geohash_4),station_id,geohash_id)
);

