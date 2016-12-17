
import time
from kafka import KafkaConsumer


def Consumer():
	consumer = KafkaConsumer('temp',bootstrap_servers=['vm1:9092'])

	for message in consumer:
   		print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,message.offset, message.key, message.value))

def main():
    	Consumer()

if __name__ =="__main__":
    main()
