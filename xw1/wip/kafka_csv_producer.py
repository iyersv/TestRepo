import csv
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['vm1:9092'])
f = open('c.csv','rb')
rows=csv.reader(f)
for row in rows:
   #print ','.join(row)
   future = producer.send('temp',','.join(row))

producer.flush()
producer.close()

