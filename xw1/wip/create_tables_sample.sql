CREATE TABLE xweather.raw_temperature (
    geohash_id text,
    cal_bucket text,
    event_time timestamp,
    bar double,
    temp double,
    PRIMARY KEY ((geohash_id, cal_bucket), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
;
