#!/bin/bash
nohup python -u kafka_raw_data_consumer.py 4  > wd1.log 2>&1 &
nohup python -u kafka_weather_stations_consumer.py 2  > s1.log 2>&1 &

