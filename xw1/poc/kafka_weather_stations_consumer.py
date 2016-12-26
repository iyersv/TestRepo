import time
from timeit import default_timer as timer
from kafka import KafkaConsumer
import kafka_insert_data
import multiprocessing
import logging
import sys

def insert_weather_stations(data,name):
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
        except Exception,e:
            print('Error processing row due to  '+str(e)+ 'in '+str(name), row)
    print('Executed dataset for  '+name, timer() - start_time, 'rows:', ctr)




def Consumer():
   data = []
   start_time=timer()
   name=multiprocessing.current_process().name
  # print(name,'Starting')
   while True:
        print(name,'Starting')
	consumer = KafkaConsumer('topic-weather-stations',group_id='consumer-weather-data',bootstrap_servers=['vm1:9092'],consumer_timeout_ms=15000,heartbeat_interval_ms=1000)
	consumer.zookeeper_connect='vm1:2181'
	try:
	    for message in consumer:
		data.append(message.value)
		if len(data) >15000:
			insert_weather_stations(data,name)
			data=[]
		else:
			continue
	finally:
	      print(name,'Exiting now')
              if len(data) >0:
                        insert_weather_stations(data,name)
                        data=[]

	      sys.stdout.flush()
	      consumer.close()

def Threadstart(ithreads = 1):
        jobs = []
        for i in range(ithreads):
           p = multiprocessing.Process(name='Consumer '+str(i+1),target = Consumer)
           jobs.append(p)
           p.start()

        for i in jobs:
          i.join()
          print('Job',i)


def main():
	if len(sys.argv) >1:
	  threads = sys.argv[1]
	else:
	  threads =1
	print('Threads to process :',threads)
	multiprocessing.log_to_stderr()
	logger =multiprocessing.get_logger()
	logger.setLevel(logging.INFO)

#	while True:
 	Threadstart(int(threads))
	#	print('Sleeping for 5')
	#	time.sleep(5)

if __name__ =="__main__":
    main()
