import time
from timeit import default_timer as timer
from kafka import KafkaConsumer
import kafka_insert_data
data=[]
def Consumer():
	global data
	start_time=timer()
	consumer = KafkaConsumer('temp',group_id='consumer-temp',bootstrap_servers=['vm1:9092'],consumer_timeout_ms=20000,heartbeat_interval_ms=1000)
#	consumer.subscribe('temp')
	consumer.zookeeper_connect='localhost:2181'
	try:
	    for message in consumer:
		data.append(message.value)
		#time.sleep(3)
		if len(data) >5000:
			kafka_insert_data.insert_vals(data)
			data=[]
		else:
			continue
   		#print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,message.offset, message.key, message.value))
	finally:
	      print('Exiting now')
              if len(data) >0:
                        kafka_insert_data.insert_vals(data)
                        data=[]

	      #consumer.commit_async()
	      consumer.close()

def main():
	global data
	while True:
    		Consumer()
		print('Sleeping for 5')
		time.sleep(5)

if __name__ =="__main__":
    main()
