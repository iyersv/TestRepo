import time
from timeit import default_timer as timer
from kafka import KafkaConsumer
import kafka_insert_data
import multiprocessing
import logging
import sys
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.query import ordered_dict_factory
from cassandra.concurrent import execute_concurrent_with_args


def insert_raw_data(data,consumer='Consumer 1'):
    # Inserts data into the table raw_data
    # Assuming that the data is in the format (station_id,cal_bucket,event_time,metric_name,metric_value)
    cluster = Cluster()
    session = cluster.connect('xweather')

    start_time = timer()
    ctr = 0
    query = "insert into raw_data(station_id,cal_bucket,event_time,metric_name,metric_value,insert_time) \
              values (?,?,?,?,?,?)"
    prepared = session.prepare(query)
    parameters =[]
    for row in data:
        try:
            row = list(row.split(','))
            row[1] = row[2][:7]
            row[2] = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S")

            # bound = prepared.bind((row[0], row[1], row[2], row[3], row[4], datetime.utcnow()))
            # session.execute_async(bound)
            parameters.append((row[0], row[1], row[2], row[3], row[4], datetime.utcnow()))
            ctr += 1
        except Exception,e:
            print('Error processing row '+str(e), row)

    
    execute_concurrent_with_args(session, prepared, parameters, concurrency=500)

    print('Executed dataset by '+str(consumer)+' in ', timer() - start_time, 'rows:', ctr)



def Consumer():
   data = []
   start_time=timer()
   name=multiprocessing.current_process().name
   while True:
        print (name,'Starting')
	consumer = KafkaConsumer('topic-weather-data',group_id='consumer-weather-data',bootstrap_servers=['vm1:9092'],consumer_timeout_ms=16000,heartbeat_interval_ms=1000)
	consumer.zookeeper_connect='vm1:2181'
	try:
	    for message in consumer:
		data.append(message.value)
		if len(data) >9500:
			insert_raw_data(data,name)
			data=[]
		else:
			continue
	finally:
	      print(name,'Exiting now',len(data))
              if len(data) >0:
			try:
                        	insert_raw_data(data,name)
                        	#kafka_insert_data.insert_raw_data(data)
                        	data=[]
			except Exception,e :
				print('Error due to ',e)
	      sys.stdout.flush()
	      print (name,'Closing out',timer() - start_time)
	      consumer.close()

def Threadstart(ithreads = 1):
	jobs = []
	for i in range(int(ithreads)):
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
	  threads = 1
	print('Threads to run :',threads)
	
        multiprocessing.log_to_stderr()
	logger = multiprocessing.get_logger()
	logger.setLevel(logging.INFO)
	
	Threadstart(threads)

if __name__ =="__main__":
    main()
