import time
from timeit import default_timer as timer
from kafka import KafkaConsumer
import kafka_insert_data
data=[]
def Consumer():
	global data
	start_time=timer()
	consumer = KafkaConsumer('topic-weather-data',group_id='consumer-weather-data',bootstrap_servers=['vm1:9092'],consumer_timeout_ms=15000,heartbeat_interval_ms=1000)
	consumer.zookeeper_connect='vm1:2181'
	try:
	    for message in consumer:
		data.append(message.value)
		if len(data) >5000:
			kafka_insert_data.insert_raw_data(data)
			data=[]
		else:
			continue
	finally:
	      print('Exiting now')
              if len(data) >0:
                        kafka_insert_data.insert_raw_data(data)
                        data=[]

	      consumer.close()

def main():
	global data
	while True:
    		Consumer()
		print('Sleeping for 5')
		time.sleep(5)

if __name__ =="__main__":
    main()
