from kafka import KafkaProducer

def Producer():
        producer = KafkaProducer(bootstrap_servers=['vm1:9092'])
	for i in range(100):
	  future   = producer.send('temp','testing a very y very very long message...message '+str(i))
        print('Sent messages '+str(i))
        producer.flush()
        producer.close()

Producer()

