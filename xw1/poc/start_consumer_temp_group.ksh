#!/bin/bash
nohup python -u kafka_raw_data_consumer.py > wd1.log 2>&1 &
nohup python -u kafka_raw_data_consumer.py > wd2.log 2>&1 &
nohup python -u kafka_raw_data_consumer.py > wd3.log 2>&1 &
nohup python -u kafka_raw_data_consumer.py > wd4.log 2>&1 &
#nohup python -u kafka_weather_stations_consumer.py >ws1.log 2>&1 &
#nohup python -u kafka_weather_stations_consumer.py >ws2.log 2>&1 &

